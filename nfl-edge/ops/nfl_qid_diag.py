#!/usr/bin/env python3
"""Diagnose the quote_id reconstruction: check observed_at microseconds and
distinct snapshot timestamps, to test the raw-ISO-Z-string hypothesis."""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg  # noqa: E402

TABLE = "nfl_edge_historical_quotes"


def main() -> None:
    url = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not url:
        sys.exit(10)
    out: dict = {}
    with psycopg.connect(url) as conn:
        out["distinct_observed_at"] = conn.execute(
            f"SELECT COUNT(DISTINCT observed_at) FROM {TABLE}"
        ).fetchone()[0]
        out["with_microseconds"] = conn.execute(
            f"""SELECT COUNT(*) FROM {TABLE}
                WHERE EXTRACT(MICROSECONDS FROM observed_at) <> 0"""
        ).fetchone()[0]
        out["sample_timestamps"] = [
            str(r[0]) for r in conn.execute(
                f"SELECT DISTINCT observed_at FROM {TABLE} ORDER BY 1 LIMIT 8"
            ).fetchall()
        ]
        # line formatting check: any lines where str(Decimal) != str(float)?
        out["line_format_edge_cases"] = conn.execute(
            f"""SELECT COUNT(*) FROM {TABLE}
                WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')
                  AND line::text <> (line::float8)::text"""
        ).fetchone()[0]
    dest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/nfl_qid_diag.json"
    with open(dest, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
