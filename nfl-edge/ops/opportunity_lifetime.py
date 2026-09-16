#!/usr/bin/env python3
"""Opportunity-lifetime analysis for the Saturday timing experiment.

Reads ONLY the dedicated timing tables (ncaaf_timing_quotes /
ncaaf_timing_runs / ncaaf_timing_book_states, experiment='saturday-timing')
-- never the ordinary live odds-quotes table.

For each 15-minute observation slot it applies the FROZEN Candidate #1
rule (constants imported read-only from ops.pressure_shadow; this file
must never modify them) to detect pressure signals, identifies the
favorable displayed quote at signal time, then measures how long that
quote survives.

INTERVAL-CENSORED methodology: with 15-minute polling, a quote seen at
slot s and s+1 but gone at s+2 has lifetime in [15, 30) minutes -- never
a point estimate, never an inferred 1/5/10-minute lifetime. Bounds are
computed from actual slot timestamps, so a missed observation widens
the interval instead of silently biasing it.

Read-only. Zero API credits. Does not modify v1.0.
"""
import argparse
import json
import os
import pathlib
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "model"))
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

# Frozen v1.0 rule parameters -- READ-ONLY reuse. The rule lives in
# ops/pressure_shadow.py and nfl-edge/docs/ncaaf-totals-pressure-v1-frozen.md;
# nothing in this file may change threshold, eligibility, consensus
# calculation, or the endpoint.
import pressure_shadow as frozen  # noqa: E402

EXPERIMENT = "saturday-timing"
SLOT_MINUTES = 15


# ---------------------------------------------------------------------------
# Pure, unit-testable analysis functions (no DB, no I/O).
# ---------------------------------------------------------------------------

def snapshot_signal(quotes):
    """Apply the frozen v1.0 rule to one event's snapshot quotes.

    quotes: list of dicts with book_key, selection, line, fair_probability.
    Returns a signal dict or None. Mirrors ops/pressure_shadow.py exactly:
    consensus = median(line) across all books at the snapshot; eligible =
    Over quotes within 0.01 of consensus with non-null fair_probability,
    from >= 3 distinct books; STRONG iff |pressure| >= 0.00126.
    """
    if len({q["book_key"] for q in quotes}) < frozen.MIN_BOOKS:
        return None
    lines = [float(q["line"]) for q in quotes]
    consensus = statistics.median(lines)
    over_qs = [q for q in quotes
               if q["selection"] == frozen.SELECTION
               and abs(float(q["line"]) - consensus) < frozen.LINE_TOL
               and q.get("fair_probability") is not None]
    if len({q["book_key"] for q in over_qs}) < frozen.MIN_BOOKS:
        return None
    fps = [float(q["fair_probability"]) for q in over_qs]
    pressure = statistics.fmean(fps) - 0.5
    if abs(pressure) < frozen.THRESHOLD:
        return None
    direction = 1 if pressure > 0 else -1
    return {
        "rule_version": frozen.RULE_VERSION,
        "market": frozen.MARKET,
        "consensus_line": consensus,
        "n_books": len({q["book_key"] for q in over_qs}),
        "pressure": pressure,
        "direction": direction,
        "direction_label": "Over" if direction > 0 else "Under",
    }


def favorable_contract(signal, quotes):
    """The favorable displayed quote at signal time.

    If pressure predicts the total will move UP (direction +1), the
    favorable action is Over at the current consensus line at the best
    displayed price; symmetrically Under for direction -1. Best price =
    highest american_odds among eligible books at the consensus line.
    Returns (book_key, line, american_odds) or None.
    """
    want = "Over" if signal["direction"] > 0 else "Under"
    cands = [q for q in quotes
             if q["selection"] == want
             and abs(float(q["line"]) - signal["consensus_line"]) < frozen.LINE_TOL]
    if not cands:
        return None
    best = max(cands, key=lambda q: (int(q["american_odds"]),
                                     str(q["book_key"])))
    return {"book_key": best["book_key"], "selection": want,
            "line": float(best["line"]),
            "american_odds": int(best["american_odds"])}


def contract_intact_at(contract, slot_quotes, slot_states):
    """Is the favorable contract still obtainable at this slot?

    Intact = the book's snapshot state is 'available' AND a quote for the
    same selection+line exists at a price no worse than signal time.
    Deterioration (worse price), absence, request failure, or an unmapped
    identity all end the run -- disappearance and deterioration are both
    "the favorable number is no longer there".
    """
    book = contract["book_key"]
    sel = contract["selection"]
    line = contract["line"]
    price = contract["american_odds"]
    st = None
    for s in slot_states:
        if s["book_key"] == book:
            st = s["state"]
            break
    if st != "available":
        return False
    # Favorable side at the signal line, at a price no worse than signal
    # time. A worse price or a vanished quote both end the run.
    for q in slot_quotes:
        if (q["book_key"] == book
                and q["selection"] == sel
                and abs(float(q["line"]) - line) < frozen.LINE_TOL
                and int(q["american_odds"]) >= price):
            return True
    return False


def lifetime_bounds(signal_idx, slot_times, intact):
    """Interval-censored lifetime of a contract.

    intact[i] = whether the contract was obtainable at slot i
    (i >= signal_idx; intact[signal_idx] is True by construction).
    Returns (lower_min, upper_min_or_None): lower = minutes from signal
    slot to the last intact slot; upper = minutes to the first
    non-intact slot, or None when right-censored (still intact at the
    final observation). Bounds use actual slot timestamps.
    """
    t0 = slot_times[signal_idx]
    last_intact = signal_idx
    first_gone = None
    for i in range(signal_idx + 1, len(slot_times)):
        if intact[i]:
            last_intact = i
        elif first_gone is None:
            first_gone = i
            break
    lower = (slot_times[last_intact] - t0).total_seconds() / 60
    upper = ((slot_times[first_gone] - t0).total_seconds() / 60
             if first_gone is not None else None)
    return lower, upper


def consensus_trajectory(event_slots):
    """Per-event consensus totals line across slots.

    event_slots: list of (slot_time, quotes) in time order.
    Returns list of (slot_time, consensus_or_None).
    """
    out = []
    for t, quotes in event_slots:
        lines = [float(q["line"]) for q in quotes]
        out.append((t, statistics.median(lines) if lines else None))
    return out


def first_repricing_after(traj, signal_idx):
    """First slot index after the signal where the consensus line moved.

    Returns (idx, old, new) or None when no repricing was observed.
    """
    _, base = traj[signal_idx]
    if base is None:
        return None
    for i in range(signal_idx + 1, len(traj)):
        _, c = traj[i]
        if c is not None and c != base:
            return i, base, c
    return None


def summarize_bounds(bounds):
    """Summarize interval-censored [lower, upper) bounds without implying
    point precision: separate distributions of lower and upper bounds."""
    interval = [(lo, hi) for lo, hi in bounds if hi is not None]
    right = [lo for lo, hi in bounds if hi is None]

    def pct(xs, p):
        xs = sorted(xs)
        if not xs:
            return None
        k = (len(xs) - 1) * p / 100
        lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
        return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 1)

    def desc(xs):
        return {"n": len(xs), "median": pct(xs, 50),
                "p25": pct(xs, 25), "p75": pct(xs, 75)} if xs else {"n": 0}
    return {
        "n_interval_censored": len(interval),
        "n_right_censored": len(right),
        "lower_bounds_min": desc([lo for lo, _ in interval]),
        "upper_bounds_min": desc([hi for _, hi in interval]),
        "right_censored_lower_bounds_min": desc(right),
    }


# ---------------------------------------------------------------------------
# DB readers (thin) and main analysis.
# ---------------------------------------------------------------------------

def load_timing_data(conn, experiment=EXPERIMENT):
    """Load runs, quotes, and book states for the experiment."""
    runs = conn.execute("""
        SELECT id, slot, status, credits_consumed, n_events_universe
        FROM public.ncaaf_timing_runs
        WHERE experiment = %s AND status IN ('ok', 'capped')
        ORDER BY slot
    """, (experiment,)).fetchall()
    run_ids = [r[0] for r in runs]
    quotes, states = [], []
    if run_ids:
        quotes = conn.execute("""
            SELECT run_id, slot, provider_event_id, book_key, market,
                   selection, line, american_odds, fair_probability,
                   observed_at, collected_at
            FROM public.ncaaf_timing_quotes
            WHERE experiment = %s AND run_id = ANY(%s)
        """, (experiment, run_ids)).fetchall()
        states = conn.execute("""
            SELECT run_id, provider_event_id, book_key, universe_member,
                   state, observed_at, collected_at
            FROM public.ncaaf_timing_book_states
            WHERE run_id = ANY(%s)
        """, (run_ids,)).fetchall()
    return runs, quotes, states


def analyze(runs, quotes, states):
    run_by_id = {r[0]: r[1] for r in runs}
    slots = sorted({r[1] for r in runs})
    slot_idx = {s: i for i, s in enumerate(slots)}

    q_by_slot_event = defaultdict(list)
    for r in quotes:
        _, slot, eid, book, market, sel, line, odds, fp, obs, coll = r
        if market != frozen.MARKET:
            continue
        q_by_slot_event[(slot, eid)].append({
            "book_key": book, "selection": sel, "line": line,
            "american_odds": odds, "fair_probability": fp,
            "observed_at": obs})

    s_by_slot_event = defaultdict(list)
    for r in states:
        run_id, eid, book, umember, state, obs, coll = r
        s_by_slot_event[(run_by_id[run_id], eid)].append({
            "book_key": book, "universe_member": umember, "state": state})

    # Per (slot, event): signal detection.
    signals = []  # (slot, event_id, signal, contract)
    for (slot, eid), qs in sorted(q_by_slot_event.items()):
        sig = snapshot_signal(qs)
        if sig is None:
            continue
        contract = favorable_contract(sig, qs)
        if contract is None:
            continue
        signals.append({"slot": slot, "slot_idx": slot_idx[slot],
                        "event_id": eid, "signal": sig, "contract": contract})

    # Per event: slot-ordered quote series for lifetime + repricing.
    events = defaultdict(list)
    for (slot, eid), qs in q_by_slot_event.items():
        events[eid].append((slot, qs))
    for eid in events:
        events[eid].sort(key=lambda x: x[0])
    event_states = defaultdict(list)
    for (slot, eid), ss in s_by_slot_event.items():
        event_states[eid].append((slot, ss))
    for eid in event_states:
        event_states[eid].sort(key=lambda x: x[0])

    lifetimes = []
    repricing = {"n_signals": len(signals), "n_repricing_observed": 0,
                 "n_direction_match": 0, "n_quote_intact_at_repricing": 0}
    survival_num = defaultdict(int)
    survival_den = defaultdict(int)

    for s in signals:
        eid = s["event_id"]
        eslots = events[eid]
        # Align: slot times for this event (subset of global slots).
        etimes = [t for t, _ in eslots]
        eidx = {t: i for i, t in enumerate(etimes)}
        si = eidx[s["slot"]]
        intact = []
        for t, qs in eslots:
            slot_states = []
            for st_, sss in event_states.get(eid, []):
                if st_ == t:
                    slot_states = sss
                    break
            intact.append(contract_intact_at(s["contract"], qs, slot_states))
        intact[si] = True  # by construction at signal time
        lo, hi = lifetime_bounds(si, etimes, intact)
        lifetimes.append((round(lo, 1), round(hi, 1) if hi is not None else None))

        # Survival: for each lag k slots ahead, was it still intact?
        for k in range(1, 9):
            if si + k < len(etimes):
                survival_den[k] += 1
                if intact[si + k]:
                    survival_num[k] += 1

        # Repricing after the signal.
        traj = consensus_trajectory(eslots)
        rp = first_repricing_after(traj, si)
        if rp is not None:
            ri, old, new = rp
            repricing["n_repricing_observed"] += 1
            pred_up = s["signal"]["direction"] > 0
            if (pred_up and new > old) or (not pred_up and new < old):
                repricing["n_direction_match"] += 1
            if intact[ri]:
                repricing["n_quote_intact_at_repricing"] += 1

    survival = {f"plus_{k*SLOT_MINUTES}min":
                {"intact": survival_num[k], "at_risk": survival_den[k],
                 "fraction": round(survival_num[k] / survival_den[k], 3)
                 if survival_den[k] else None}
                for k in sorted(survival_den)}

    credits = sum((r[3] or 0) for r in runs)
    return {
        "experiment": EXPERIMENT,
        "rule_version": frozen.RULE_VERSION,
        "rule_frozen": frozen.RULE_FROZEN_DATE,
        "n_runs": len(runs),
        "n_slots": len(slots),
        "n_events_universe": sum((r[4] or 0) for r in runs),
        "credits_consumed_measured": credits,
        "n_signals": len(signals),
        "n_events_with_signals": len({s["event_id"] for s in signals}),
        "direction_split": {
            "Over": sum(1 for s in signals if s["signal"]["direction"] > 0),
            "Under": sum(1 for s in signals if s["signal"]["direction"] < 0),
        },
        "lifetime_bounds_min": summarize_bounds(lifetimes),
        "lifetimes_raw": [[lo, hi] for lo, hi in lifetimes],
        "survival_by_lag": survival,
        "repricing": repricing,
        "methodology_note": (
            "INTERVAL-CENSORED: observations are 15 minutes apart, so each "
            "contract lifetime is a [lower, upper) bound in minutes, never "
            "a point estimate. A contract seen at the signal slot and the "
            "next slot but gone at the following one has lifetime in "
            "[15, 30). Right-censored contracts (still obtainable at the "
            "final observation) contribute a lower bound only. Bounds use "
            "actual slot timestamps: a missed observation widens the "
            "interval. Do not interpret bound medians as 'the quote lasts "
            "X minutes'."
        ),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Analyze Saturday timing experiment (read-only).")
    ap.add_argument("--experiment", default=EXPERIMENT)
    ap.add_argument("--out", default="/tmp/saturday_timing_analysis.json")
    args = ap.parse_args(argv)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL required", file=sys.stderr)
        return 2

    with psycopg.connect(dsn) as conn:
        try:
            runs, quotes, states = load_timing_data(conn, args.experiment)
        except Exception as exc:  # noqa: BLE001 - no timing data yet
            result = {"experiment": args.experiment,
                      "status": "no_timing_data",
                      "detail": str(exc)[:200]}
            with open(args.out, "w") as f:
                json.dump(result, f, indent=2)
            print(json.dumps(result, indent=2))
            return 0

    if not runs:
        result = {"experiment": args.experiment, "status": "no_runs_yet",
                  "methodology_note": "No timing runs recorded."}
    else:
        result = analyze(runs, quotes, states)
        result["status"] = "ok"
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
