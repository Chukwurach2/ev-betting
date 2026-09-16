#!/usr/bin/env python3
"""Persistent quota logging for Odds API usage.

Every collector run logs: sport, request type, quota before/after, delta.
This enables actual (not estimated) credit budgeting for pilot decisions.

Table: ncaaf_quota_log (also used for NFL via sport column).
"""
import os, sys, json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def log_quota(sport, request_type, quota_before, quota_after, n_events=0,
              endpoint=None, n_markets=None, billed_credits=None, run_id=None):
    """Log one API request's quota impact. Returns delta (credits used).

    Optional per-call credit fields (migration 021, for the NFL prop layer):
    endpoint (e.g. nfl_prop_odds_call), n_markets, billed_credits (measured
    from x-requests-* headers), run_id. Free calls are logged at 0 so a
    future audit can see the full request pattern.
    """
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL required", file=sys.stderr)
        return None

    before_used = quota_before.get("used")
    after_used = quota_after.get("used")
    delta = None
    if before_used is not None and after_used is not None:
        delta = after_used - before_used
    if billed_credits is not None:
        delta = billed_credits

    with psycopg.connect(dsn) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS public.ncaaf_quota_log (
                id SERIAL PRIMARY KEY,
                logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sport TEXT NOT NULL,
                request_type TEXT NOT NULL,
                quota_remaining_before INTEGER,
                quota_used_before INTEGER,
                quota_remaining_after INTEGER,
                quota_used_after INTEGER,
                credits_used INTEGER,
                n_events INTEGER DEFAULT 0
            )
        """)
        # Additive columns (migration 021); IF NOT EXISTS so a fresh table
        # and an already-migrated table both work.
        for coldef in (
            "endpoint TEXT",
            "n_markets INTEGER",
            "billed_credits INTEGER",
            "run_id TEXT",
        ):
            conn.execute("ALTER TABLE public.ncaaf_quota_log "
                         f"ADD COLUMN IF NOT EXISTS {coldef}")
        conn.execute("""
            INSERT INTO public.ncaaf_quota_log
            (sport, request_type, quota_remaining_before, quota_used_before,
             quota_remaining_after, quota_used_after, credits_used, n_events,
             endpoint, n_markets, billed_credits, run_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sport, request_type,
              quota_before.get("remaining"), before_used,
              quota_after.get("remaining"), after_used,
              delta, n_events, endpoint, n_markets,
              billed_credits, run_id))
        conn.commit()
    return delta


def weekly_summary(sport=None, weeks=4):
    """Return per-sport weekly credit usage for budgeting."""
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute("""
            SELECT sport,
                   DATE_TRUNC('week', logged_at) AS week,
                   SUM(credits_used) AS total_credits,
                   COUNT(*) AS n_requests
            FROM public.ncaaf_quota_log
            WHERE logged_at > NOW() - (%s || ' weeks')::INTERVAL
              AND (%s IS NULL OR sport = %s)
            GROUP BY sport, week
            ORDER BY week DESC, sport
        """, (weeks, sport, sport)).fetchall()
    return [{"sport": r[0], "week": r[1].isoformat() if r[1] else None,
             "credits": r[2], "requests": r[3]} for r in rows]


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--sport", default=None)
    ap.add_argument("--weeks", type=int, default=4)
    args = ap.parse_args()
    if args.summary:
        print(json.dumps(weekly_summary(args.sport, args.weeks), indent=2))
