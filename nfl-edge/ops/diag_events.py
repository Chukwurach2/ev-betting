#!/usr/bin/env python3
"""Minimal test: can we query ncaaf_edge_market_history?"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports
import psycopg

dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
mh = sports.market_history_table("ncaaf")
print(f"Table: public.{mh}", flush=True)

with psycopg.connect(dsn) as conn:
    # Count rows
    cnt = conn.execute(f"SELECT COUNT(*) FROM public.{mh}").fetchone()[0]
    print(f"Row count: {cnt}", flush=True)

    # Try the event extraction query
    rows = conn.execute(f"""
        SELECT DISTINCT
            event->>'id' AS odds_event_id,
            event->>'home_team' AS home_team,
            event->>'away_team' AS away_team,
            (event->>'commence_time')::timestamptz AS commence_time
        FROM public.{mh} mh
        CROSS JOIN LATERAL jsonb_array_elements(mh.payload->'data') AS event
        WHERE event->>'id' IS NOT NULL
          AND event->>'home_team' IS NOT NULL
          AND event->>'away_team' IS NOT NULL
          AND event->>'commence_time' IS NOT NULL
        LIMIT 5
    """).fetchall()
    print(f"Sample events: {len(rows)}", flush=True)
    for r in rows:
        print(f"  {r[1]} vs {r[2]} ({r[0][:8]}...)", flush=True)

print("SUCCESS", flush=True)
