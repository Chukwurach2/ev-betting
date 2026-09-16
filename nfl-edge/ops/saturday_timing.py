#!/usr/bin/env python3
"""Saturday opportunity-lifetime experiment: high-frequency totals collection.

ONE-TIME experiment authorized for Saturday 2026-09-19 only.
Measures whether a favorable displayed totals quote survives long enough
to act on after a Candidate #1 pressure signal fires.

Measurement only. No betting, no execution, no changes to the frozen
v1.0 rule (nfl-edge/docs/ncaaf-totals-pressure-v1-frozen.md).

Design (repairs the 2026-09-15 audit defects):
  1. Hard date gate: the script exits before ANY API call unless the
     current America/New_York date is 2026-09-19 and the time is inside
     the 10:00-22:00 ET window. The GitHub cron may fire on other
     Saturdays; those runs are zero-cost no-ops.
  2. Exactly 48 observation slots: 10:00-21:45 ET every 15 minutes
     (cron '*/15 14-23 * * 6' + '*/15 0-1 * * 0' = 40 + 8 runs).
  3. Cost is per event, not per snapshot: the provider's event-odds
     endpoint costs 1 credit per event for totals-only/us (measured
     2026-09-16 via x-requests-last=1 on the live feed).
  4. Quota accounting measures the complete run: provider credit balance
     before the first paid call and after the last one.
  5. Hard credit caps enforced BEFORE any paid call and inside the loop:
     per-run cap, per-day cap, and the standing NCAAF quota-reserve
     floor that protects the NFL collector.
  6. Explicit per-book snapshot states (available / absent /
     request_failed / unmapped) against a mechanical contract universe:
     Saturday's provider events x the configured book universe.
  7. Writes to the dedicated ncaaf_timing_* tables, never the live
     odds-quotes table.
  8. Every observation carries the mechanical 15-minute slot plus
     provider observed_at and collector collected_at timestamps, so the
     analyzer can reconstruct signal time, quote availability,
     disappearance/deterioration, and consensus repricing with
     interval-censored bounds (never inferred minute precision).
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

from provider_oddsapi import fetch_events, fetch_event_odds, normalize, quota  # noqa: E402
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

# Measured 2026-09-16: one event-odds call with markets=totals, regions=us
# costs exactly 1 provider credit (x-requests-last=1, used delta +1).
COST_PER_EVENT = 1

STATES = ("available", "absent", "request_failed", "unmapped")


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


def team_mapped(name, aliases):
    """A provider team name is mapped when the canonical identity layer can
    resolve it: either the raw provider name (with mascot) is a known alias
    key, or it already equals a canonical name."""
    if not name:
        return False
    if name in aliases:
        return True
    return name in set(aliases.values())


def load_aliases():
    path = pathlib.Path(__file__).parent / "team_aliases.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def cap_event_list(events, max_events):
    """Deterministic truncation to the credit cap. Returns (kept, n_capped)."""
    if len(events) <= max_events:
        return events, 0
    return events[:max_events], len(events) - max_events


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
    absent:         book is in the universe but returned no totals quote.
    request_failed: the event-odds fetch raised.
    unmapped:       the event's teams could not be canonically resolved;
                    quotes are still stored, but flagged, never silently
                    treated as clean.
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


def run_collection(fetch_events_fn=fetch_events,
                   fetch_odds_fn=fetch_event_odds,
                   now_utc=None,
                   dry_run=False,
                   day_spend_fn=None):
    """Execute one observation run. Pure orchestration: no DB writes here.

    Returns a result dict with full accounting (slots, states, quotes,
    credit balances). In dry_run mode the free events endpoint is still
    consulted (0 credits) but no paid odds calls are made.

    day_spend_fn: optional zero-arg callable returning today's measured
    timing-experiment credit consumption (from ncaaf_timing_runs). When
    provided, the per-day hard cap is enforced BEFORE any paid call.
    """
    now_utc = now_utc or datetime.now(UTC)
    ok, status, slot, et_now = gate(now_utc, EXPERIMENT_DATE_ET)
    universe = book_universe()
    max_credits = _env_int("TIMING_MAX_CREDITS_PER_RUN", 80)
    day_cap = _env_int("TIMING_MAX_CREDITS_DAY", 4000)
    reserve = _env_int("NCAAF_EDGE_MIN_QUOTA_REMAINING", 2000)
    aliases = load_aliases()

    result = {
        "experiment": EXPERIMENT,
        "slot": slot.isoformat(),
        "et_now": et_now.isoformat(),
        "status": status,
        "n_slots_planned": N_SLOTS,
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

    # Hard per-run cap enforced BEFORE any paid call: 1 credit per event
    # (measured), so at most max_credits - margin events are attempted.
    margin = 2
    max_events = max(0, max_credits - margin)
    kept, n_capped = cap_event_list(events, max_events)
    result["n_events_capped"] = n_capped
    result["max_credits_per_run"] = max_credits

    # Hard per-day cap, also BEFORE any paid call: refuse the run when
    # today's already-measured consumption plus this run's estimate would
    # exceed the day budget.
    if day_spend_fn is not None and not dry_run:
        try:
            day_spent = day_spend_fn() or 0
        except Exception:  # noqa: BLE001 - fail closed on accounting error
            day_spent = None
        if day_spent is None:
            result["status"] = "day_cap_skip"
            result["day_cap_error"] = "could not measure day spend; failing closed"
            return result
        est = len(kept) * COST_PER_EVENT
        if day_spent + est > day_cap:
            result["status"] = "day_cap_skip"
            result["day_cap"] = day_cap
            result["day_spent_before"] = day_spent
            result["day_estimate"] = est
            return result
        result["day_spent_before"] = day_spent
        result["day_cap"] = day_cap

    spent = 0
    all_quotes = []
    book_states = []
    n_unpaired = 0
    n_failed = 0
    last_headers = ev_headers
    attempted = 0

    for e in kept:
        if dry_run:
            break  # dry-run: enumerate only, zero paid calls
        if spent >= max_credits:
            result["capped_mid_run"] = True
            break
        event_id = e.get("id")
        attempted += 1
        try:
            payload, h = fetch_odds_fn(
                event_id, MARKETS, regions=REGIONS, sport=SPORT)
            last_headers = h
            qh = quota(h)
            # Prefer the header delta; fall back to the measured unit cost.
            if qh.get("last") is not None:
                spent += qh["last"]
            else:
                spent += COST_PER_EVENT
            fetch_ok = True
        except Exception as exc:  # noqa: BLE001 - per-event isolation
            print(f"request_failed {event_id}: {exc}", file=sys.stderr)
            payload, fetch_ok = {}, False
            n_failed += 1

        seen_books = [b.get("key") for b in (payload.get("bookmakers") or [])
                      if b.get("key")]
        mapped = (team_mapped(e.get("home_team"), aliases)
                  and team_mapped(e.get("away_team"), aliases))
        if fetch_ok:
            normed = normalize(payload,
                               allowed_books=set(universe) | set(seen_books),
                               sport=SPORT)
            devigged, unp = devig_event_quotes(normed)
            n_unpaired += unp
        else:
            devigged = []
        for q in devigged:
            q["experiment"] = EXPERIMENT
            q["slot"] = slot.isoformat()
            q["collected_at"] = now_utc.isoformat()
        all_quotes.extend(devigged)
        books_with_quotes = {q["book_key"] for q in devigged}
        for row in classify_event_books(
                event_id, universe, seen_books, books_with_quotes,
                fetch_ok, mapped):
            row["observed_at"] = None
            row["collected_at"] = now_utc.isoformat()
            book_states.append(row)

        # Mid-run reserve check: stop paid calls if the shared key is now
        # below the NFL-protection floor.
        rem = quota(last_headers).get("remaining")
        if rem is not None and rem < reserve:
            result["capped_mid_run"] = True
            result["mid_run_reserve_stop"] = True
            break

    qa = quota(last_headers)
    result["credits_remaining_after"] = qa.get("remaining")
    result["credits_used_after"] = qa.get("used")
    ub, ua = qb.get("used"), qa.get("used")
    result["credits_consumed"] = (ua - ub) if (ub is not None
                                               and ua is not None) else spent
    result["credits_spent_tracked"] = spent
    result["n_events_attempted"] = attempted
    result["n_events_failed"] = n_failed
    result["n_quotes"] = len(all_quotes)
    result["n_book_states"] = len(book_states)
    result["n_unpaired_sides_skipped"] = n_unpaired
    result["status"] = "capped" if result.get("capped_mid_run") else "ok"
    result["dry_run"] = dry_run

    # Keep the heavy payloads out of the small --out JSON; they are
    # persisted to the timing tables (or returned for tests).
    result["_quotes"] = all_quotes
    result["_book_states"] = book_states
    result["_quota_before"] = qb
    result["_quota_after"] = qa
    return result


def persist(result, dsn):
    """Write one run's rows to the ncaaf_timing_* tables. Returns run_id.

    Idempotent per (experiment, slot): a retry never double-writes.
    The per-day credit cap is enforced in run_collection BEFORE any paid
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
    """Today's measured timing-experiment credit consumption (ET day)."""
    import psycopg
    with psycopg.connect(dsn) as conn:
        row = conn.execute("""
            SELECT COALESCE(SUM(credits_consumed), 0)
            FROM public.ncaaf_timing_runs
            WHERE experiment = %s
              AND run_started_at >= (NOW() AT TIME ZONE 'America/New_York')::date
                  AT TIME ZONE 'America/New_York'
              AND status IN ('ok', 'capped')
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
                         "payload ({\"quota\": {...}, \"data\": [...]}), so "
                         "the dry run costs zero API calls even for the "
                         "free events endpoint.")
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
    if args.events_fixture:
        fixture = json.loads(pathlib.Path(args.events_fixture).read_text())
        _payload, _headers = fixture["data"], fixture.get("quota", {})

        def fetch_events_fn(sport="ncaaf", _p=_payload, _h=_headers):
            return _p, {"x-requests-remaining": _h.get("requests_remaining"),
                        "x-requests-used": _h.get("requests_used"),
                        "x-requests-last": _h.get("requests_last")}

    result = run_collection(fetch_events_fn=fetch_events_fn,
                            dry_run=args.dry_run,
                            now_utc=now_utc,
                            day_spend_fn=(None if args.dry_run or not dsn
                                          else lambda: _day_spend(dsn)))

    run_id = None
    if not args.dry_run and dsn and result["status"] not in (
            "date_gate_skip", "window_skip", "quota_standdown",
            "day_cap_skip"):
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
