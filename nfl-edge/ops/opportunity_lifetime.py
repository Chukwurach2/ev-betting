#!/usr/bin/env python3
"""Opportunity lifetime characterization (pre-firewall, shadow).

Measures how long qualifying displayed totals prices survive in the
15-minute collector cadence. This is OPERATIONAL timing, not prediction.

Question: when a pressure signal fires, how long does the favorable
displayed price typically remain available?

Read-only. Zero API credits. Does not modify v1.0.
"""
import os, sys, json, argparse, statistics
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", default="/tmp/opportunity_lifetime.json")
    args = ap.parse_args(argv)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    live = sports.odds_quotes_table(args.sport)

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"""
            SELECT provider_event_id, book_key, selection, line,
                   american_odds, fair_probability, collected_at
            FROM public.{live}
            WHERE market = 'FULL_GAME_TOTAL'
              AND collected_at > NOW() - (%s || ' days')::INTERVAL
            ORDER BY provider_event_id, book_key, selection, line, collected_at
        """, (args.days,)).fetchall()

    # Track each (event, book, selection, line) price trajectory
    # Key insight: when does a specific price appear, change, or disappear?
    trajectories = defaultdict(list)  # (eid,book,sel,line) -> [(ts, odds)]
    for r in rows:
        eid, book, sel, line, odds, fp, ts = r
        key = (eid, book, sel, float(line))
        trajectories[key].append((ts, odds))

    # For each trajectory, measure continuous runs at same price
    lifetimes_min = []
    n_trajectories = len(trajectories)
    n_price_changes = 0

    for key, obs in trajectories.items():
        obs.sort()  # by timestamp
        if len(obs) < 2:
            continue
        run_start = obs[0][0]
        run_price = obs[0][1]
        for i in range(1, len(obs)):
            ts, price = obs[i]
            prev_ts, prev_price = obs[i-1]
            gap_min = (ts - prev_ts).total_seconds() / 60
            if price != run_price or gap_min > 30:
                # Run ended: price changed or gap too large
                lifetime = (prev_ts - run_start).total_seconds() / 60
                if lifetime > 0:
                    lifetimes_min.append(lifetime)
                if price != run_price:
                    n_price_changes += 1
                run_start = ts
                run_price = price
        # Final run (censored — still alive at end of window)
        final_lifetime = (obs[-1][0] - run_start).total_seconds() / 60
        if final_lifetime > 0:
            lifetimes_min.append(final_lifetime)

    def pct(xs, p):
        xs = sorted(xs)
        k = (len(xs) - 1) * p / 100
        lo, hi = int(k), int(k) + 1
        if hi >= len(xs):
            return xs[-1]
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    result = {
        "window_days": args.days,
        "n_trajectories": n_trajectories,
        "n_price_runs": len(lifetimes_min),
        "n_price_changes": n_price_changes,
        "lifetime_minutes": {
            "mean": round(statistics.fmean(lifetimes_min), 1) if lifetimes_min else 0,
            "median": round(pct(lifetimes_min, 50), 1) if lifetimes_min else 0,
            "p25": round(pct(lifetimes_min, 25), 1) if lifetimes_min else 0,
            "p75": round(pct(lifetimes_min, 75), 1) if lifetimes_min else 0,
            "p90": round(pct(lifetimes_min, 90), 1) if lifetimes_min else 0,
        },
        "interpretation": (
            "Median lifetime = typical minutes a displayed totals price "
            "survives unchanged. If median >> 15min, the 15-min cadence "
            "captures most opportunities. If median ~15min or less, "
            "higher-frequency collection may be needed for execution timing."
        ),
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
