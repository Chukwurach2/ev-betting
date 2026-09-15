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
    # INTERVAL-CENSORED: with 15-min polling, if price seen at t=0 and t=15
    # but gone at t=30, lifetime is in [15, 30), not "22.5 min".
    # We report lower and upper bounds, never point estimates.
    lifetimes = []  # (lower_min, upper_min)
    n_trajectories = len(trajectories)
    n_price_changes = 0

    for key, obs in trajectories.items():
        obs.sort()  # by timestamp
        if len(obs) < 2:
            continue
        run_start = obs[0][0]
        run_price = obs[0][1]
        last_seen = obs[0][0]
        for i in range(1, len(obs)):
            ts, price = obs[i]
            prev_ts, prev_price = obs[i-1]
            gap_min = (ts - prev_ts).total_seconds() / 60
            if price != run_price or gap_min > 30:
                # Run ended. Lower bound = last_seen - run_start.
                # Upper bound = ts - run_start (disappeared sometime before ts).
                lower = (last_seen - run_start).total_seconds() / 60
                upper = (ts - run_start).total_seconds() / 60
                if lower > 0:
                    lifetimes.append((lower, upper))
                if price != run_price:
                    n_price_changes += 1
                run_start = ts
                run_price = price
            last_seen = ts
        # Final run is right-censored: still alive at end of window
        # Lower bound = last_seen - run_start, upper = infinity
        final_lower = (last_seen - run_start).total_seconds() / 60
        if final_lower > 0:
            lifetimes.append((final_lower, float('inf')))

    def pct(xs, p):
        xs = sorted(xs)
        k = (len(xs) - 1) * p / 100
        lo, hi = int(k), int(k) + 1
        if hi >= len(xs):
            return xs[-1]
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    # Separate interval-censored from right-censored
    interval = [(lo, hi) for lo, hi in lifetimes if hi != float('inf')]
    right_cens = [lo for lo, hi in lifetimes if hi == float('inf')]

    # For interval-censored: report distribution of lower bounds and upper bounds
    # separately. Never imply point precision.
    lower_bounds = [lo for lo, hi in interval]
    upper_bounds = [hi for lo, hi in interval]

    result = {
        "window_days": args.days,
        "n_trajectories": n_trajectories,
        "n_price_runs": len(lifetimes),
        "n_price_changes": n_price_changes,
        "n_interval_censored": len(interval),
        "n_right_censored": len(right_cens),
        "lifetime_lower_bounds_min": {
            "mean": round(statistics.fmean(lower_bounds), 1) if lower_bounds else 0,
            "median": round(pct(lower_bounds, 50), 1) if lower_bounds else 0,
            "p25": round(pct(lower_bounds, 25), 1) if lower_bounds else 0,
            "p75": round(pct(lower_bounds, 75), 1) if lower_bounds else 0,
        } if lower_bounds else {},
        "lifetime_upper_bounds_min": {
            "mean": round(statistics.fmean(upper_bounds), 1) if upper_bounds else 0,
            "median": round(pct(upper_bounds, 50), 1) if upper_bounds else 0,
            "p25": round(pct(upper_bounds, 25), 1) if upper_bounds else 0,
            "p75": round(pct(upper_bounds, 75), 1) if upper_bounds else 0,
        } if upper_bounds else {},
        "methodology_note": (
            "INTERVAL-CENSORED: With 15-min polling, lifetimes are reported as "
            "[lower, upper) bounds, not point estimates. If a price is seen at "
            "t=0 and t=15 but gone at t=30, lifetime is in [15, 30). "
            "Right-censored runs (still alive at window end) contribute only "
            "lower bounds. Do not interpret medians as 'the price lasts X minutes'."
        ),
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
