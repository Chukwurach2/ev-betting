#!/usr/bin/env python3
"""Diagnostic: inspect market_history schema and payload structure."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports

import psycopg
dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
mh = sports.market_history_table("ncaaf")
hq = sports.historical_quotes_table("ncaaf")

with psycopg.connect(dsn) as conn:
    print("=== market_history columns ===")
    cols = conn.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{mh}'
        ORDER BY ordinal_position
    """).fetchall()
    for c in cols:
        print(f"  {c[0]}: {c[1]}")

    print("\n=== historical_quotes columns ===")
    cols = conn.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{hq}'
        ORDER BY ordinal_position
    """).fetchall()
    for c in cols:
        print(f"  {c[0]}: {c[1]}")

    print("\n=== sample payload structure ===")
    row = conn.execute(f"SELECT payload FROM public.{mh} LIMIT 1").fetchone()
    if row:
        p = row[0]
        print(f"Python type: {type(p).__name__}")
        if isinstance(p, str):
            p = json.loads(p)
            print("Parsed from string")
        print(f"Top-level keys: {list(p.keys()) if isinstance(p, dict) else 'not a dict'}")
        if isinstance(p, dict) and "data" in p:
            data = p["data"]
            print(f"data type: {type(data).__name__}, length: {len(data) if isinstance(data, list) else 'N/A'}")
            if isinstance(data, list) and data:
                g = data[0]
                print(f"First game keys: {list(g.keys()) if isinstance(g, dict) else 'not a dict'}")
                if isinstance(g, dict):
                    print(f"  id: {g.get('id')}")
                    print(f"  home_team: {g.get('home_team')}")
                    print(f"  away_team: {g.get('away_team')}")
                    print(f"  commence_time: {g.get('commence_time')}")
