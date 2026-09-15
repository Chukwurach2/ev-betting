#!/usr/bin/env python3
"""Saturday timing experiment: high-frequency totals collection.

Purpose: Measure opportunity lifetime — how long do favorable displayed
totals prices survive after a pressure signal?

- Totals-only (1 credit/snapshot)
- Every 15 minutes on Saturday
- Separate table: ncaaf_timing_quotes (not mixed with opener data)
- Labeled: experiment='saturday-timing', cannot modify v1.0
- Measurement only, not outcome optimization

Run via workflow on Saturdays 10am-10pm ET.
"""
import os, sys, json, argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402
from provider_oddsapi import fetch_events, fetch_event_odds, normalize, quota
from quota_log import log_quota

SPORT = "ncaaf"
MARKETS = ["totals"]  # totals-only = 1 credit
REGIONS = ["us"]
EXPERIMENT = "saturday-timing"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/saturday_timing.json")
    args = ap.parse_args(argv)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL required", file=sys.stderr)
        return 2

    # Fetch events (free)
    events_payload, headers = fetch_events(sport="americanfootball_ncaaf")
    quota_before = quota(headers)

    # Filter to today's games (Saturday)
    today = datetime.now(timezone.utc).date()
    games = []
    for e in events_payload:
        kickoff = e.get("commence_time", "")
        if kickoff.startswith(str(today)):
            games.append(e)

    print(f"Found {len(games)} games today")

    all_quotes = []
    quota_after_first = None

    for g in games[:30]:  # Cap at 30 games for cost control
        event_id = g["id"]
        try:
            payload, h = fetch_event_odds(
                event_id, MARKETS, regions=REGIONS,
                sport="americanfootball_ncaaf")
            if quota_after_first is None:
                quota_after_first = quota(h)
            quotes = normalize(payload, sport=SPORT)
            for q in quotes:
                q["experiment"] = EXPERIMENT
                q["collected_at"] = datetime.now(timezone.utc).isoformat()
            all_quotes.extend(quotes)
        except Exception as e:
            print(f"Error fetching {event_id}: {e}", file=sys.stderr)

    # Log quota
    if quota_after_first:
        credits = log_quota(SPORT, f"timing-{EXPERIMENT}",
                           quota_before, quota_after_first,
                           n_events=len(games[:30]))
        print(f"Credits used: {credits}")

    # Store in separate timing table
    with psycopg.connect(dsn) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS public.ncaaf_timing_quotes (
                id SERIAL PRIMARY KEY,
                experiment TEXT NOT NULL,
                provider_event_id TEXT NOT NULL,
                book_key TEXT NOT NULL,
                market TEXT NOT NULL,
                selection TEXT NOT NULL,
                line DOUBLE PRECISION,
                american_odds INTEGER,
                fair_probability DOUBLE PRECISION,
                observed_at TIMESTAMPTZ,
                collected_at TIMESTAMPTZ NOT NULL
            )
        """)
        for q in all_quotes:
            conn.execute("""
                INSERT INTO public.ncaaf_timing_quotes
                (experiment, provider_event_id, book_key, market, selection,
                 line, american_odds, fair_probability, observed_at, collected_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (EXPERIMENT, q.get("provider_event_id"), q.get("book_key"),
                  q.get("market"), q.get("selection"), q.get("line"),
                  q.get("american_odds"), q.get("fair_probability"),
                  q.get("observed_at"), q.get("collected_at")))
        conn.commit()

    result = {
        "experiment": EXPERIMENT,
        "n_games": len(games[:30]),
        "n_quotes": len(all_quotes),
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
