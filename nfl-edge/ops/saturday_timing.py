#!/usr/bin/env python3
"""Saturday opportunity-lifetime experiment: high-frequency totals collection.

ONE-TIME experiment authorized for Saturday 2026-09-19 only.
Measures whether a favorable displayed totals quote survives long enough
to act on after a Candidate #1 pressure signal fires.

Measurement only. No betting, no execution, no changes to the frozen
v1.0 rule (nfl-edge/docs/ncaaf-totals-pressure-v1-frozen.md).

Design (2026-09-16 revision: one bulk odds request per tick):
  1. Hard date gate: the script exits before ANY API call unless the
     current America/New_York date is 2026-09-19 and the time is inside
     the 10:00-22:00 ET window. The GitHub cron may fire on other
     Saturdays; those runs are zero-cost no-ops.
  2. Exactly 48 observation slots: 10:00-21:45 ET every 15 minutes
     (cron '*/15 14-23 * * 6' + '*/15 0-1 * * 0' = 40 + 8 runs).
  3. Cost is per tick, not per event: ONE bulk call per run to
     /v4/sports/americanfootball_ncaaf/odds with regions=us,
     markets=totals costs exactly 1 provider credit (measured
     2026-09-16: 1 region x 1 market = 1). Expected Saturday total:
     48 ticks x 1 = 48 credits. Hard experiment ceiling: 55/day.
  4. Quota accounting measures the complete run: provider credit balance
     before the first paid call and after the last one.
  5. Hard credit caps enforced BEFORE any paid call: per-run cap,
     per-day cap (55), and the standing NCAAF quota-reserve floor that
     protects the NFL collector.
  6. Retry discipline: at most ONE retry per tick, only on request
     failure, explicitly logged with tick timestamp + reason. The retry
     writes under the SAME tick timestamp: no extra observation, no
     cadence shift. A tick that fails twice is recorded as
     request_failed, never silently dropped.
  7. Explicit per-book snapshot states (available / absent /
     request_failed / unmapped) derived client-side from the bulk
     payload and the mechanical 9-book universe:
       available      book returned a totals quote for the event.
       absent         book is in the universe but the bulk payload
                      carried no totals quote for the event (includes
                      expected events missing from the payload).
       unmapped       event was in the bulk payload but NOT in the
                      expected Saturday slate; quotes are still stored,
                      but flagged, never silently treated as clean.
       request_failed the bulk request failed (after the one retry);
                      the whole tick is marked, never individual-book
                      absence inferred from a failed tick.
  8. Writes to the dedicated ncaaf_timing_* tables, never the live
     odds-quotes table.
  9. Every observation carries the mechanical 15-minute slot plus
     provider observed_at and collector collected_at timestamps, so the
     analyzer can reconstruct signal time, quote availability,
     disappearance/deterioration, and consensus repricing with
     interval-censored bounds (never inferred minute precision). A
     failed tick contributes no quotes, so the analyzer's per-event
     slot series skips it and lifetime_bounds widens the interval via
     actual timestamps: a censored gap, never a fabricated
     disappearance.
"""
import argparse
import json
import os
import pathlib
import statistics
import sys
from dataclasses import asdict
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

# Same import convention as ops/collect_checkpoints.py: nfl-edge/ root and
# model/ both on sys.path so `provider_oddsapi`, `research.devig` and
# `ops.sports` resolve however the script is invoked (repo root with
# sparse checkout, or nfl-edge/ as cwd).
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "model"))
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from provider_oddsapi import fetch_events, fetch_odds, normalize, quota  # noqa: E402
from research.devig import devig_multiplicative  # noqa: E402
from ops.sports import odds_api_sport  # noqa: E402
from quota_log import log_quota  # noqa: E402

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

EXPERIMENT = "saturday-timing"
EXPERIMENT_DATE_ET = "2026-09-19"  # the single authorized Saturday (ET)
WINDOW_START = time(10, 0)  # 10:00 ET inclusive
WINDOW_END = time(22, 0)    # 22:00 ET exclusive -> last slot 21:45 ET
SLOT_MINUTES = 15
N_SLOTS = 48

SPORT = "ncaaf"
MARKETS = ["totals"]  # totals-only
REGIONS = "us"

# Mechanical contract universe: book keys observed on the live NCAAF us
# feed (2026-09-16 probe: draftkings, fanduel, betmgm, betrivers,
# betonlineag, betus, lowvig, mybookieag, bovada). Override with
# TIMING_BOOK_UNIVERSE="draftkings,fanduel,...".
DEFAULT_BOOK_UNIVERSE = (
    "draftkings,fanduel,betmgm,betrivers,"
    "betonlineag,betus,lowvig,mybookieag,bovada"
)

# Measured 2026-09-16: one bulk /odds call with markets=totals,
# regions=us costs exactly 1 provider credit (1 region x 1 market).
COST_PER_TICK = 1
# Initial attempt + at most one retry per tick (retry discipline).
MAX_ATTEMPTS_PER_TICK = 2
# Expected Saturday consumption: 48 ticks x 1 credit.
EXPECTED_DAY_CREDITS = N_SLOTS * COST_PER_TICK  # 48
# Hard experiment ceiling: 55/day. The 7-credit headroom above 48 is
# reserved EXCLUSIVELY for explicitly logged retries; retries never
# increase the experimental cadence (same tick timestamp, no extra
# observation slots).
DEFAULT_MAX_CREDITS_PER_RUN = 4
DEFAULT_MAX_CREDITS_DAY = 55

STATES = ("available", "absent", "request_failed", "unmapped")

# Run statuses that are persisted to ncaaf_timing_runs. A failed tick
# (request_failed) IS persisted: the tick-level failure must be recorded
# so the analyzer treats it as a censored gap, not as "no signal".
# Gate skips and standdowns record nothing (no tick occurred).
PERSISTABLE_STATUSES = ("ok", "capped", "request_failed")
NON_TICK_STATUSES = ("date_gate_skip", "window_skip", "quota_standdown",
                     "day_cap_skip", "run_cap_skip")


def _env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def book_universe():
    raw = os.environ.get("TIMING_BOOK_UNIVERSE", DEFAULT_BOOK_UNIVERSE)
    return sorted({b.strip() for b in raw.split(",") if b.strip()})


def slot_floor(dt_utc):
    """Floor an aware UTC datetime to the 15-minute mechanical slot."""
    minute = (dt_utc.minute // SLOT_MINUTES) * SLOT_MINUTES
    return dt_utc.replace(minute=minute, second=0, microsecond=0)


def gate(now_utc, experiment_date=EXPERIMENT_DATE_ET):
    """Date/window gate. Returns (ok, status, slot_utc, et_now).

    ok=False means: exit before ANY provider API call. The GitHub cron is
    deliberately left on a weekly cadence; the gate is what restricts the
    experiment to the single authorized Saturday.
    """
    et_now = now_utc.astimezone(ET)
    slot = slot_floor(now_utc)
    if et_now.date().isoformat() != experiment_date:
        return False, "date_gate_skip", slot, et_now
    et_time = et_now.timetz().replace(tzinfo=None)
    if not (WINDOW_START <= et_time < WINDOW_END):
        return False, "window_skip", slot, et_now
    return True, "ok", slot, et_now


def saturday_events(events_payload, et_date):
    """Provider events whose kickoff falls on the experiment Saturday (ET)."""
    out = []
    for e in events_payload or []:
        try:
            kickoff = datetime.fromisoformat(
                str(e.get("commence_time", "")).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=UTC)
        if kickoff.astimezone(ET).date().isoformat() == et_date:
            out.append(e)
    out.sort(key=lambda e: str(e.get("id")))
    return out


def implied(odds):
    odds = int(odds)
    return 100 / (100 + odds) if odds > 0 else abs(odds) / (100 + abs(odds))


def devig_event_quotes(quotes):
    """Attach paired same-book de-vig fair probabilities to normalized
    totals quotes. Only Over/Under pairs at the same book+line are kept
    (the frozen rule requires non-null fair probabilities)."""
    by_key = {}
    for q in quotes:
        if q.market != "FULL_GAME_TOTAL":
            continue
        by_key.setdefault((q.sportsbook_key, q.line), {})[q.selection] = q
    out = []
    n_unpaired = 0
    for (book, line), sides in by_key.items():
        if set(sides) != {"Over", "Under"}:
            n_unpaired += len(sides)
            continue
        try:
            fair = devig_multiplicative(
                implied(sides["Over"].american_odds),
                implied(sides["Under"].american_odds))
        except (TypeError, ValueError):
            fair = None
        if fair is None:
            n_unpaired += 2
            continue
        for sel, fq in zip(("Over", "Under"), fair):
            d = asdict(sides[sel])
            d["book_key"] = sides[sel].sportsbook_key
            d["fair_probability"] = fq
            out.append(d)
    return out, n_unpaired


def classify_event_books(event_id, universe, seen_books,
                         books_with_quotes, fetch_ok, mapped):
    """Explicit per-book snapshot states for one event.

    available:      book returned a totals quote for this event.
    absent:         book is in the universe but returned no totals quote
                    (includes expected events missing from the bulk
                    payload: fetch succeeded, provider sent no odds).
    request_failed: the bulk odds fetch raised (whole tick failed).
    unmapped:       the event was in the bulk payload but not in the
                    expected Saturday slate; quotes are still stored,
                    but flagged, never silently treated as clean.
    """
    rows = []
    for book in sorted(set(universe) | set(seen_books)):
        if not fetch_ok:
            state = "request_failed"
        elif not mapped:
            state = "unmapped"
        elif book in books_with_quotes:
            state = "available"
        else:
            state = "absent"
        rows.append({
            "provider_event_id": event_id,
            "book_key": book,
            "universe_member": book in universe,
            "state": state,
        })
    return rows


def process_bulk_tick(bulk_events, expected_events, universe,
                      slot_iso, now_iso):
    """Pure function: bulk payload + expected slate -> (quotes, states).

    bulk_events:     list of event dicts from the bulk /odds endpoint.
    expected_events: Saturday slate from the free events endpoint
                     (the mechanical expected event set).
    Returns (quotes, book_states, stats). No I/O, no API calls.
    """
    by_id = {}
    for e in bulk_events or []:
        eid = e.get("id")
        if isinstance(eid, str) and eid:
            by_id[eid] = e
    expected_ids = {e.get("id") for e in expected_events
                    if isinstance(e.get("id"), str)}

    all_quotes = []
    book_states = []
    n_unpaired = 0
    n_expected_missing = 0
    n_unexpected = 0

    def stamp_quote(q):
        q["experiment"] = EXPERIMENT
        q["slot"] = slot_iso
        q["collected_at"] = now_iso
        return q

    def book_observed(quotes, book):
        obs = [q["observed_at"] for q in quotes
               if q["book_key"] == book and q.get("observed_at")]
        return max(obs) if obs else None

    # 1. Expected events: normalize quotes where the payload has them;
    #    mark books absent where it does not.
    for e in expected_events:
        event_id = e.get("id")
        be = by_id.get(event_id)
        if be is None:
            n_expected_missing += 1
            for row in classify_event_books(
                    event_id, universe, [], set(),
                    fetch_ok=True, mapped=True):
                row["observed_at"] = None
                row["collected_at"] = now_iso
                book_states.append(row)
            continue
        seen_books = [b.get("key") for b in (be.get("bookmakers") or [])
                      if b.get("key")]
        normed = normalize(be,
                           allowed_books=set(universe) | set(seen_books),
                           sport=SPORT)
        devigged, unp = devig_event_quotes(normed)
        n_unpaired += unp
        for q in devigged:
            stamp_quote(q)
        all_quotes.extend(devigged)
        books_with_quotes = {q["book_key"] for q in devigged}
        for row in classify_event_books(
                event_id, universe, seen_books, books_with_quotes,
                fetch_ok=True, mapped=True):
            row["observed_at"] = book_observed(devigged, row["book_key"])
            row["collected_at"] = now_iso
            book_states.append(row)

    # 2. Unexpected events (in bulk payload, not in the slate): unmapped.
    for event_id, be in sorted(by_id.items()):
        if event_id in expected_ids:
            continue
        n_unexpected += 1
        seen_books = [b.get("key") for b in (be.get("bookmakers") or [])
                      if b.get("key")]
        normed = normalize(be,
                           allowed_books=set(universe) | set(seen_books),
                           sport=SPORT)
        devigged, unp = devig_event_quotes(normed)
        n_unpaired += unp
        for q in devigged:
            stamp_quote(q)
        all_quotes.extend(devigged)
        books_with_quotes = {q["book_key"] for q in devigged}
        for row in classify_event_books(
                event_id, universe, seen_books, books_with_quotes,
                fetch_ok=True, mapped=False):
            row["observed_at"] = book_observed(devigged, row["book_key"])
            row["collected_at"] = now_iso
            book_states.append(row)

    stats = {
        "n_bulk_events": len(by_id),
        "n_expected_missing_from_bulk": n_expected_missing,
        "n_unexpected_events": n_unexpected,
        "n_unpaired_sides_skipped": n_unpaired,
    }
    return all_quotes, book_states, stats


def failed_tick_states(expected_events, universe, now_iso):
    """Book states for a tick whose bulk request failed (after the one
    retry): every expected event x universe book is request_failed.
    Never infer individual-book absence from a failed tick."""
    rows = []
    for e in expected_events:
        for row in classify_event_books(
                e.get("id"), universe, [], set(),
                fetch_ok=False, mapped=True):
            row["observed_at"] = None
            row["collected_at"] = now_iso
            rows.append(row)
    return rows


def run_collection(fetch_events_fn=fetch_events,
                   fetch_odds_fn=fetch_odds,
                   now_utc=None,
                   dry_run=False,
                   day_spend_fn=None,
                   bulk_fixture=None):
    """Execute one observation run. Pure orchestration: no DB writes here.

    One bulk odds call per tick (1 credit), at most one retry on request
    failure. Returns a result dict with full accounting (slots, states,
    quotes, credit balances, retry log). In dry_run mode no paid calls
    are made; when bulk_fixture is provided the bulk path is exercised
    in-memory with zero API cost.

    day_spend_fn: optional zero-arg callable returning today's measured
    timing-experiment credit consumption (from ncaaf_timing_runs). When
    provided, the per-day hard cap is enforced BEFORE any paid call.
    """
    now_utc = now_utc or datetime.now(UTC)
    ok, status, slot, et_now = gate(now_utc, EXPERIMENT_DATE_ET)
    universe = book_universe()
    max_credits = _env_int("TIMING_MAX_CREDITS_PER_RUN",
                           DEFAULT_MAX_CREDITS_PER_RUN)
    day_cap = _env_int("TIMING_MAX_CREDITS_DAY", DEFAULT_MAX_CREDITS_DAY)
    reserve = _env_int("NCAAF_EDGE_MIN_QUOTA_REMAINING", 2000)
    slot_iso = slot.isoformat()
    now_iso = now_utc.isoformat()

    result = {
        "experiment": EXPERIMENT,
        "slot": slot_iso,
        "et_now": et_now.isoformat(),
        "status": status,
        "n_slots_planned": N_SLOTS,
        "cost_model": "bulk_per_tick",
        "cost_per_tick_credits": COST_PER_TICK,
        "max_attempts_per_tick": MAX_ATTEMPTS_PER_TICK,
    }
    if not ok:
        # Gate fired before any provider call: zero API cost by construction.
        return result

    api_sport = odds_api_sport(SPORT)
    events_payload, ev_headers = fetch_events_fn(sport=SPORT)
    qb = quota(ev_headers)
    result["credits_remaining_before"] = qb.get("remaining")
    result["credits_used_before"] = qb.get("used")

    # Standing quota isolation: the NFL collector has priority. Stand down
    # entirely (no paid calls) when the shared key is below the reserve.
    remaining = qb.get("remaining")
    if remaining is not None and remaining < reserve:
        result["status"] = "quota_standdown"
        result["reserve_floor"] = reserve
        return result

    events = saturday_events(events_payload, EXPERIMENT_DATE_ET)
    result["n_events_universe"] = len(events)
    result["n_books_universe"] = len(universe)
    result["n_events_capped"] = 0  # bulk covers the whole slate: no truncation
    result["max_credits_per_run"] = max_credits

    # Hard per-run cap enforced BEFORE any paid call: one tick needs at
    # most MAX_ATTEMPTS_PER_TICK credits (initial attempt + one retry).
    if max_credits < MAX_ATTEMPTS_PER_TICK:
        result["status"] = "run_cap_skip"
        result["run_cap_credits"] = max_credits
        result["run_cap_needed"] = MAX_ATTEMPTS_PER_TICK
        return result

    # Hard per-day cap, also BEFORE any paid call: refuse the run when
    # today's already-measured consumption plus this tick's worst case
    # (attempt + one retry) would exceed the 55-credit experiment ceiling.
    worst_case = MAX_ATTEMPTS_PER_TICK * COST_PER_TICK
    if day_spend_fn is not None and not dry_run:
        try:
            day_spent = day_spend_fn() or 0
        except Exception:  # noqa: BLE001 - fail closed on accounting error
            day_spent = None
        if day_spent is None:
            result["status"] = "day_cap_skip"
            result["day_cap_error"] = "could not measure day spend; failing closed"
            return result
        if day_spent + worst_case > day_cap:
            result["status"] = "day_cap_skip"
            result["day_cap"] = day_cap
            result["day_spent_before"] = day_spent
            result["day_worst_case"] = worst_case
            return result
        result["day_spent_before"] = day_spent
        result["day_cap"] = day_cap
    result["day_cap_configured"] = day_cap

    def finalize(payload_quotes, payload_states, stats, spent, headers,
                 n_attempts, retries):
        qa = quota(headers)
        result["credits_remaining_after"] = qa.get("remaining")
        result["credits_used_after"] = qa.get("used")
        ub, ua = qb.get("used"), qa.get("used")
        result["credits_consumed"] = (ua - ub) if (ub is not None
                                                   and ua is not None) else spent
        result["credits_spent_tracked"] = spent
        result["n_bulk_attempts"] = n_attempts
        result["n_events_attempted"] = 1 if n_attempts else 0
        result["retries"] = retries
        result["n_quotes"] = len(payload_quotes)
        result["n_book_states"] = len(payload_states)
        for k, v in stats.items():
            result[k] = v
        result["dry_run"] = dry_run
        # Keep the heavy payloads out of the small --out JSON; they are
        # persisted to the timing tables (or returned for tests).
        result["_quotes"] = payload_quotes
        result["_book_states"] = payload_states
        result["_quota_before"] = qb
        result["_quota_after"] = qa
        return result

    # Dry run: zero paid calls. With a bulk fixture, exercise the full
    # in-memory normalization path at zero API cost.
    if dry_run:
        if bulk_fixture is not None:
            quotes, states, stats = process_bulk_tick(
                bulk_fixture, events, universe, slot_iso, now_iso)
            result["status"] = "ok"
            return finalize(quotes, states, stats, 0, ev_headers,
                            0, [])
        result["status"] = "ok"
        return finalize([], [], {"n_bulk_events": 0,
                                 "n_expected_missing_from_bulk": 0,
                                 "n_unexpected_events": 0,
                                 "n_unpaired_sides_skipped": 0},
                        0, ev_headers, 0, [])

    # Live tick: one bulk call, at most one retry on request failure.
    # The retry is logged with tick timestamp + reason and writes under
    # the SAME tick timestamp: no extra observation, no cadence shift.
    retries = []
    payload = None
    last_headers = ev_headers
    last_exc = None
    n_attempts = 0
    for attempt in range(1, MAX_ATTEMPTS_PER_TICK + 1):
        n_attempts = attempt
        try:
            raw, h = fetch_odds_fn(MARKETS, regions=REGIONS, sport=SPORT)
            last_headers = h
            payload = raw if isinstance(raw, list) else (raw or {}).get(
                "data", [])
            last_exc = None
            if retries:
                retries[-1]["outcome"] = "succeeded"
            break
        except Exception as exc:  # noqa: BLE001 - one retry, then fail loud
            last_exc = exc
            reason = f"{type(exc).__name__}: {exc}"[:200]
            if attempt < MAX_ATTEMPTS_PER_TICK:
                # At most one retry entry ever exists: the append runs
                # only for attempt < MAX_ATTEMPTS_PER_TICK.
                retries.append({"slot": slot_iso, "attempt": attempt + 1,
                                "reason": reason, "outcome": "pending"})
                print(f"tick {slot_iso}: bulk attempt {attempt} failed "
                      f"({reason}); retrying once under the same tick",
                      file=sys.stderr)

    if payload is None:
        # Tick failed after the single retry: record it explicitly.
        # Zero quotes; every expected event x universe book is
        # request_failed. The analyzer's per-event slot series skips
        # this tick and lifetime_bounds widens the interval via actual
        # timestamps: a censored gap, never a fabricated disappearance.
        reason = f"{type(last_exc).__name__}: {last_exc}"[:200] \
            if last_exc else "unknown"
        print(f"tick {slot_iso}: bulk request failed after "
              f"{MAX_ATTEMPTS_PER_TICK} attempts ({reason}); "
              f"recording request_failed tick", file=sys.stderr)
        if retries:
            retries[-1]["outcome"] = "failed"
            retries[-1]["final_reason"] = reason
        states = failed_tick_states(events, universe, now_iso)
        result["status"] = "request_failed"
        result["error"] = reason
        spent = n_attempts * COST_PER_TICK
        res = finalize([], states,
                       {"n_bulk_events": 0,
                        "n_expected_missing_from_bulk": 0,
                        "n_unexpected_events": 0,
                        "n_unpaired_sides_skipped": 0},
                       spent, last_headers, n_attempts, retries)
        # Fail closed on failed ticks: the provider may still have
        # counted the attempts against quota, so the ceiling accounts
        # for them even when the header delta reads 0.
        res["credits_consumed"] = max(res["credits_consumed"] or 0, spent)
        return res

    qh = quota(last_headers)
    spent = qh["last"] if qh.get("last") is not None else COST_PER_TICK
    quotes, states, stats = process_bulk_tick(
        payload, events, universe, slot_iso, now_iso)
    result["status"] = "ok"
    return finalize(quotes, states, stats, spent, last_headers,
                    n_attempts, retries)


def persist(result, dsn):
    """Write one run's rows to the ncaaf_timing_* tables. Returns run_id.

    Idempotent per (experiment, slot): a retry never double-writes.
    Failed ticks (request_failed) ARE persisted: the tick-level failure
    is recorded so the analyzer treats it as a censored gap. The
    per-day credit cap is enforced in run_collection BEFORE any paid
    call; persist is storage only.
    """
    import psycopg
    with psycopg.connect(dsn) as conn:
        row = conn.execute("""
            INSERT INTO public.ncaaf_timing_runs
            (experiment, slot, run_finished_at, n_events_universe,
             n_events_attempted, n_events_capped, n_books_universe,
             credits_remaining_before, credits_used_before,
             credits_remaining_after, credits_used_after, credits_consumed,
             status, error)
            VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (experiment, slot) DO NOTHING
            RETURNING id
        """, (EXPERIMENT, result["slot"], result.get("n_events_universe", 0),
              result.get("n_events_attempted", 0),
              result.get("n_events_capped", 0),
              result.get("n_books_universe", 0),
              result.get("credits_remaining_before"),
              result.get("credits_used_before"),
              result.get("credits_remaining_after"),
              result.get("credits_used_after"),
              result.get("credits_consumed"),
              result["status"], result.get("error"))).fetchone()
        if row is None:
            # Slot already recorded (e.g. retry): do not double-write.
            run_id = conn.execute("""
                SELECT id FROM public.ncaaf_timing_runs
                WHERE experiment = %s AND slot = %s
            """, (EXPERIMENT, result["slot"])).fetchone()[0]
            conn.commit()
            return run_id
        run_id = row[0]
        for q in result.get("_quotes", []):
            conn.execute("""
                INSERT INTO public.ncaaf_timing_quotes
                (run_id, experiment, slot, provider_event_id, book_key,
                 market, selection, line, american_odds, fair_probability,
                 observed_at, collected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, EXPERIMENT, result["slot"],
                  q.get("provider_event_id"), q.get("book_key"),
                  q.get("market"), q.get("selection"), q.get("line"),
                  q.get("american_odds"), q.get("fair_probability"),
                  q.get("observed_at"), q.get("collected_at")))
        for s in result.get("_book_states", []):
            conn.execute("""
                INSERT INTO public.ncaaf_timing_book_states
                (run_id, provider_event_id, book_key, universe_member,
                 state, observed_at, collected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id, provider_event_id, book_key)
                DO NOTHING
            """, (run_id, s["provider_event_id"], s["book_key"],
                  s["universe_member"], s["state"], s.get("observed_at"),
                  s["collected_at"]))
        conn.commit()
        return run_id


def _day_spend(dsn):
    """Today's measured timing-experiment credit consumption (ET day).

    Includes request_failed ticks: a failed tick still costs its
    attempt(s), and the 55-credit ceiling must account for them."""
    import psycopg
    with psycopg.connect(dsn) as conn:
        row = conn.execute("""
            SELECT COALESCE(SUM(credits_consumed), 0)
            FROM public.ncaaf_timing_runs
            WHERE experiment = %s
              AND run_started_at >= (NOW() AT TIME ZONE 'America/New_York')::date
                  AT TIME ZONE 'America/New_York'
              AND status IN ('ok', 'capped', 'request_failed')
        """, (EXPERIMENT,)).fetchone()
    return row[0] if row else 0


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Saturday totals opportunity-lifetime collector "
                    "(one-time experiment, 2026-09-19).")
    ap.add_argument("--out", default="/tmp/saturday_timing.json")
    ap.add_argument("--dry-run", action="store_true",
                    help="Zero paid API calls and zero DB writes; the free "
                         "events endpoint is still consulted to enumerate "
                         "Saturday's games and estimate cost.")
    ap.add_argument("--date-override", default=None,
                    help="ONLY honored with --dry-run: simulate the run as if "
                         "the current ET date were YYYY-MM-DD (validation, "
                         "never production).")
    ap.add_argument("--events-fixture", default=None,
                    help="Dry-run only: JSON file with the events endpoint "
                         "payload ({\"quota\": {...}, \"data\": [...], "
                         "\"bulk\": [...] (optional)}), so the dry run "
                         "costs zero API calls even for the free events "
                         "endpoint. The optional \"bulk\" key exercises the "
                         "full in-memory bulk normalization path.")
    args = ap.parse_args(argv)

    if args.date_override and not args.dry_run:
        print("--date-override requires --dry-run", file=sys.stderr)
        return 2

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn and not args.dry_run:
        print("NFL_EDGE_DATABASE_URL required (or use --dry-run)",
              file=sys.stderr)
        return 2

    # Dry-run date simulation: shift "now" so the ET date matches the
    # override, keeping the time of day. This exercises the gate,
    # Saturday-event filtering, and cost estimation as they will behave
    # on 2026-09-19.
    now_utc = None
    if args.date_override:
        from datetime import date as _date, timedelta as _td
        try:
            want = _date.fromisoformat(args.date_override)
        except ValueError:
            print("--date-override must be YYYY-MM-DD", file=sys.stderr)
            return 2
        real_now = datetime.now(timezone.utc)
        shift = want - real_now.astimezone(ET).date()
        now_utc = real_now + _td(days=shift.days)

    if args.events_fixture and not args.dry_run:
        print("--events-fixture requires --dry-run", file=sys.stderr)
        return 2

    fetch_events_fn = fetch_events
    bulk_fixture = None
    if args.events_fixture:
        fixture = json.loads(pathlib.Path(args.events_fixture).read_text())
        _payload, _headers = fixture["data"], fixture.get("quota", {})
        bulk_fixture = fixture.get("bulk")

        def fetch_events_fn(sport="ncaaf", _p=_payload, _h=_headers):
            return _p, {"x-requests-remaining": _h.get("requests_remaining"),
                        "x-requests-used": _h.get("requests_used"),
                        "x-requests-last": _h.get("requests_last")}

    result = run_collection(fetch_events_fn=fetch_events_fn,
                            dry_run=args.dry_run,
                            now_utc=now_utc,
                            bulk_fixture=bulk_fixture,
                            day_spend_fn=(None if args.dry_run or not dsn
                                          else lambda: _day_spend(dsn)))

    run_id = None
    if (not args.dry_run and dsn
            and result["status"] in PERSISTABLE_STATUSES):
        try:
            run_id = persist(result, dsn)
        except Exception as exc:  # noqa: BLE001 - report, don't mask
            result["status"] = "persist_failed"
            result["error"] = str(exc)[:500]
    if run_id is not None:
        result["run_id"] = run_id

    # Complete-run quota accounting: balance before the first paid call vs
    # after the last one. Logged even for gate skips (delta 0) so the
    # ledger shows every scheduled invocation.
    qb, qa = result.get("_quota_before"), result.get("_quota_after")
    if (not args.dry_run and dsn and qb is not None and qa is not None
            and result["status"] not in ("persist_failed",)):
        try:
            credits = log_quota(
                SPORT, f"timing-{EXPERIMENT}", qb, qa,
                n_events=result.get("n_events_attempted", 0))
            result["quota_log_credits"] = credits
        except Exception as exc:  # noqa: BLE001
            result["quota_log_error"] = str(exc)[:200]

    public = {k: v for k, v in result.items() if not k.startswith("_")}
    with open(args.out, "w") as f:
        json.dump(public, f, indent=2)
    print(json.dumps(public, indent=2))
    return 0 if result["status"] not in ("persist_failed",) else 1


if __name__ == "__main__":
    sys.exit(main())
