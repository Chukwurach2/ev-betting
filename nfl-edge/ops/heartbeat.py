"""Record a pipeline heartbeat: the calling component finished successfully.

Usage (from a workflow step, after the component's work completed):
    HEARTBEAT_DETAIL='{"odds_credits_remaining": 492}' \
      python ops/heartbeat.py collector

Components: schedule_sync, collector, picks, settlement.
Exits 0 on success, nonzero on failure (so a missed heartbeat is loud).
"""
from __future__ import annotations

import json
import os
import sys


def record_heartbeat(conn, component: str, detail: dict | None = None) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO public.nfl_edge_heartbeats (component, last_ok_at, detail)
        VALUES (%s, now(), %s)
        ON CONFLICT (component)
        DO UPDATE SET last_ok_at = now(), detail = EXCLUDED.detail
        """,
        (component, json.dumps(detail) if detail is not None else None),
    )
    conn.commit()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python ops/heartbeat.py <component>", file=sys.stderr)
        return 2
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        raise ValueError("NFL_EDGE_DATABASE_URL is required")
    detail = None
    raw = os.environ.get("HEARTBEAT_DETAIL")
    if raw:
        detail = json.loads(raw)
    import psycopg
    conn = psycopg.connect(database)
    try:
        record_heartbeat(conn, argv[1], detail)
    finally:
        conn.close()
    print(json.dumps({"component": argv[1], "heartbeat": "recorded"}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
