#!/usr/bin/env python3
"""Deterministic fingerprint of a frozen historical odds dataset.

Hashes every normalized quote row in canonical order, so any later
mutation (insert/update/delete) changes the fingerprint. `collected_at`
is excluded: it is insert-time metadata, not data.

Usage:
    NFL_EDGE_DATABASE_URL=... python -m nfl_edge.ops.fingerprint_dataset --sport ncaaf
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402

# Columns that constitute the data (collected_at excluded: insert metadata).
QUOTE_COLS = [
    "quote_id", "provider", "provider_event_id", "home_team", "away_team",
    "kickoff", "sportsbook", "book_key", "market", "selection", "line",
    "american_odds", "fair_probability", "observed_at", "source", "ny_licensed",
]


def fingerprint(conn, sport: str) -> dict:
    hq_table = sports.historical_quotes_table(sport)
    mh_table = sports.market_history_table(sport)
    order = ", ".join(QUOTE_COLS)
    rows = conn.execute(
        f"SELECT {', '.join(QUOTE_COLS)} FROM public.{hq_table} ORDER BY {order}"
    ).fetchall()
    h = hashlib.sha256()
    for r in rows:
        h.update(("|".join("" if v is None else str(v) for v in r) + "\n").encode())
    quotes_fp = h.hexdigest()[:32]

    snaps = conn.execute(
        f"""SELECT requested_at, regions, markets, provider_snapshot_id,
                   books_present, credits_used
            FROM public.{mh_table}
            ORDER BY requested_at, regions, markets"""
    ).fetchall()
    h2 = hashlib.sha256()
    for r in snaps:
        h2.update(("|".join("" if v is None else str(v) for v in r) + "\n").encode())
    snaps_fp = h2.hexdigest()[:32]

    n_quotes = conn.execute(
        f"SELECT COUNT(*) FROM public.{hq_table}").fetchone()[0]
    n_snaps = conn.execute(
        f"SELECT COUNT(*) FROM public.{mh_table}").fetchone()[0]
    return {
        "sport": sport,
        "quotes_table": hq_table,
        "market_history_table": mh_table,
        "n_snapshots": n_snaps,
        "n_quotes": n_quotes,
        "quotes_fingerprint": quotes_fp,
        "snapshots_fingerprint": snaps_fp,
        # The dataset fingerprint pins the research data (quotes).
        "dataset_fingerprint": quotes_fp,
        "method": ("sha256 over '|' joined QUOTE_COLS ordered by all QUOTE_COLS, "
                   "collected_at excluded; first 32 hex chars"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="nfl")
    args = ap.parse_args()
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    import psycopg  # lazy: only needed for the live run, not unit tests

    with psycopg.connect(dsn) as conn:
        result = fingerprint(conn, args.sport)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
