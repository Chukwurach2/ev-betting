#!/usr/bin/env python3
"""Deterministic fingerprint of a frozen historical odds dataset.

Method "canonical-1" (frozen 2026-09-16; supersedes the original
str()-based method, whose 2026-09-12 construction was undocumented and
not reproducible -- the lesson of the September 2026 NFL freeze
forensics, see nfl-edge/docs/nfl-freeze-addendum-2026-09-16.md):

  * Scope: all quote rows in the sport's historical-quotes table, or --
    market-scope spread_total to restrict to FULL_GAME_SPREAD +
    FULL_GAME_TOTAL (the NFL 2022-2024 frozen research scope; the
    post-freeze FULL_GAME_MONEYLINE backfill is out of scope).
  * `collected_at` is excluded: it is insert-time metadata, not data.
  * Every value is rendered EXPLICITLY by `render_value()` -- never
    `str()` on a driver object -- so two drivers / two runs agree:

    - NULL            -> "\\x00" (a NUL byte; PostgreSQL text can never
                         contain one, so this cannot collide with data;
                         it is also distinct from the empty string)
    - bool            -> "true" / "false"  (checked before int: Python
                         bool subclasses int)
    - datetime        -> UTC-normalized Zulu:
                         "2024-09-04T11:55:38Z", fractional seconds
                         appended only when nonzero with trailing zeros
                         stripped: "2024-09-04T11:55:38.123456Z".
                         Naive datetimes are assumed UTC.
    - date            -> ISO "2024-09-04"
    - Decimal         -> normalized plain notation: trailing zeros
                         stripped ("45.0" -> "45", "3.50" -> "3.5").
                         Never scientific notation.
    - float           -> via Decimal(str(v)), then as Decimal. (Values
                         come from the database, never from arithmetic,
                         so binary float noise cannot appear.)
    - int             -> str(v)
    - list / tuple    -> ",".join(sorted(str(x) for x in v))
                         (e.g. books_present arrays)
    - str             -> as-is (UTF-8)

  * Row ordering: rows are sorted in PYTHON by the tuple of rendered
    column strings (QUOTE_COLS order). No ORDER BY in SQL -- database
    collation and driver row order cannot affect the result.
  * Preimage: for each row in sorted order, "|".join(rendered columns)
    + "\\n", encoded UTF-8, fed into one SHA-256. The dataset fingerprint
    is the FULL 64-character hex digest (the original method truncated
    to 32 chars; canonical-1 does not truncate).
  * The snapshots component hashes (requested_at, regions, markets,
    provider_snapshot_id, books_present, credits_used) with the same
    rendering; `dataset_fingerprint` pins the research data (quotes).

What the fingerprint covers: quote rows only -- provider, event, book,
market, selection, line, price, fair_probability, observed_at. It never
reads realized scores or outcomes.

Reproducibility note: rendering happens in Python from raw column
values, but every branch above is pinned -- a different DB driver that
returns datetime/Decimal/bool/str for these columns produces the
identical preimage. The unit tests pin the serialization byte-for-byte.

Usage:
    NFL_EDGE_DATABASE_URL=... python nfl-edge/ops/fingerprint_dataset.py \\
        --sport nfl --market-scope spread_total
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402

# Columns that constitute the data (collected_at excluded: insert metadata).
QUOTE_COLS = [
    "quote_id", "provider", "provider_event_id", "home_team", "away_team",
    "kickoff", "sportsbook", "book_key", "market", "selection", "line",
    "american_odds", "fair_probability", "observed_at", "source", "ny_licensed",
]

SNAP_COLS = [
    "requested_at", "regions", "markets", "provider_snapshot_id",
    "books_present", "credits_used",
]

# Frozen market-scope literals (constants, not user input -- safe to inline).
SPREAD_TOTAL_MARKETS = ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL")

#: Fingerprint method identifier, recorded in research_dataset_manifests.
METHOD = "canonical-1"

#: NULL sentinel: NUL cannot appear in PostgreSQL text.
NULL_SENTINEL = "\x00"


def render_value(value) -> str:
    """Render one column value canonically (see module docstring)."""
    if value is None:
        return NULL_SENTINEL
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        # Normalize to UTC; naive values are assumed UTC.
        v = value if value.tzinfo is not None else value.replace(
            tzinfo=timezone.utc)
        v = v.astimezone(timezone.utc)
        base = v.strftime("%Y-%m-%dT%H:%M:%S")
        if v.microsecond:
            frac = ("%06d" % v.microsecond).rstrip("0")
            return f"{base}.{frac}Z"
        return base + "Z"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        # Via str() -> Decimal: "45.0" -> Decimal("45.0") -> "45".
        # Values originate in the DB, never from float arithmetic.
        value = Decimal(str(value))
    if isinstance(value, Decimal):
        # Normalized plain notation, never scientific.
        normalized = value.normalize()
        return format(normalized, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ",".join(sorted(str(x) for x in value))
    if isinstance(value, str):
        return value
    # Last resort: explicit, documented fallback (no bare str() on
    # unknown driver types in the hot path above).
    return str(value)


def render_row(row) -> tuple:
    """Render a QUOTE_COLS-ordered row tuple to canonical strings."""
    return tuple(render_value(v) for v in row)


def hash_rows(rendered_rows) -> str:
    """SHA-256 over the canonical preimage; full 64-char hex digest."""
    h = hashlib.sha256()
    for cols in sorted(rendered_rows):
        h.update(("|".join(cols) + "\n").encode("utf-8"))
    return h.hexdigest()


def _scope_clause(market_scope: str) -> str:
    if market_scope == "spread_total":
        markets = ", ".join("'%s'" % m for m in SPREAD_TOTAL_MARKETS)
        return f"WHERE market IN ({markets})"
    if market_scope == "all":
        return ""
    raise ValueError(f"unknown market_scope: {market_scope!r}")


def fingerprint(conn, sport: str, market_scope: str = "all") -> dict:
    """Compute the canonical-1 fingerprint (read-only SELECTs)."""
    hq_table = sports.historical_quotes_table(sport)
    mh_table = sports.market_history_table(sport)
    scope_clause = _scope_clause(market_scope)

    quote_rows = conn.execute(
        f"SELECT {', '.join(QUOTE_COLS)} FROM public.{hq_table} "
        f"{scope_clause}".rstrip()
    ).fetchall()
    quotes_fp = hash_rows([render_row(r) for r in quote_rows])

    snap_rows = conn.execute(
        f"SELECT {', '.join(SNAP_COLS)} FROM public.{mh_table}"
    ).fetchall()
    snaps_fp = hash_rows([render_row(r) for r in snap_rows])

    n_quotes = conn.execute(
        f"SELECT COUNT(*) FROM public.{hq_table} {scope_clause}".rstrip()
    ).fetchone()[0]
    n_snaps = conn.execute(
        f"SELECT COUNT(*) FROM public.{mh_table}").fetchone()[0]

    return {
        "sport": sport,
        "market_scope": market_scope,
        "method": METHOD,
        "quotes_table": hq_table,
        "market_history_table": mh_table,
        "n_snapshots": n_snaps,
        "n_quotes": n_quotes,
        "quotes_fingerprint": quotes_fp,
        "snapshots_fingerprint": snaps_fp,
        # The dataset fingerprint pins the research data (quotes).
        "dataset_fingerprint": quotes_fp,
        "method_description": (
            "canonical-1: sha256 over '|' joined explicitly-rendered "
            "QUOTE_COLS (UTF-8, '\\n' row terminator), rows sorted by "
            "rendered tuple; NULL->\\x00; datetimes UTC Zulu; numerics "
            "normalized plain notation; collected_at excluded; "
            "full 64-char hex digest"
        ),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="nfl")
    ap.add_argument("--market-scope", default="all",
                    choices=("all", "spread_total"),
                    help=("all: every row (NCAAF back-compat); "
                          "spread_total: FULL_GAME_SPREAD + FULL_GAME_TOTAL"))
    args = ap.parse_args(argv)
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    import psycopg  # lazy: only needed for the live run, not unit tests

    with psycopg.connect(dsn) as conn:
        result = fingerprint(conn, args.sport, args.market_scope)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
