#!/usr/bin/env python3
"""Read-only forensic: characterize nfl_edge_historical_quotes vs the frozen
NFL dataset (fingerprint 43f853a44bf93937d85149ca5fd7241b, 291,586 rows,
frozen 2026-09-12 ~12:45 UTC).

SELECTs only. Zero API credits. No outcomes inspected (no realized results;
only quote/row metadata: counts, books, markets, insert timestamps).

Usage:
    NFL_EDGE_DATABASE_URL=<redacted> python3 nfl_freeze_forensic.py /tmp/nfl_freeze_forensic.json
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg  # noqa: E402

TABLE = "nfl_edge_historical_quotes"
FROZEN_N = 291586
FROZEN_SNAPS = 162
FREEZE_TS = "2026-09-12T12:45:00+00:00"


def main() -> None:
    url = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not url:
        print("NFL_EDGE_DATABASE_URL is missing", file=sys.stderr)
        sys.exit(10)
    out: dict = {
        "table": TABLE,
        "frozen_n": FROZEN_N,
        "frozen_snapshots": FROZEN_SNAPS,
        "freeze_ts": FREEZE_TS,
    }
    with psycopg.connect(url) as conn:
        one = lambda sql, *a: conn.execute(sql, *a).fetchone()  # noqa: E731
        allq = lambda sql, *a: conn.execute(sql, *a).fetchall()  # noqa: E731

        n, nsnaps, mn_id, mx_id, dcnt_id, mn_c, mx_c = one(
            f"""SELECT COUNT(*), COUNT(DISTINCT observed_at),
                       MIN(quote_id), MAX(quote_id), COUNT(DISTINCT quote_id),
                       MIN(collected_at), MAX(collected_at)
                FROM {TABLE}"""
        )
        out.update(
            n_quotes=n,
            n_snapshots=nsnaps,
            min_quote_id=mn_id,
            max_quote_id=mx_id,
            distinct_quote_ids=dcnt_id,
            min_collected_at=str(mn_c),
            max_collected_at=str(mx_c),
            extra_vs_frozen=n - FROZEN_N,
        )
        out["by_collected_day"] = [
            (str(d), c)
            for d, c in allq(
                f"""SELECT date_trunc('day', collected_at) d, COUNT(*)
                    FROM {TABLE} GROUP BY 1 ORDER BY 1"""
            )
        ]
        out["by_market"] = [
            (m, c)
            for m, c in allq(
                f"SELECT market, COUNT(*) FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC"
            )
        ]
        out["by_book"] = [
            (b, c)
            for b, c in allq(
                f"SELECT sportsbook, COUNT(*) FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC"
            )
        ]
        out["by_kickoff_year"] = [
            (str(y), c)
            for y, c in allq(
                f"""SELECT EXTRACT(YEAR FROM kickoff) y, COUNT(*)
                    FROM {TABLE} GROUP BY 1 ORDER BY 1"""
            )
        ]
        out["post_freeze_inserts_by_collected_at"] = one(
            f"SELECT COUNT(*) FROM {TABLE} WHERE collected_at > %s::timestamptz",
            (FREEZE_TS,),
        )[0]
        out["duplicate_key_groups"] = one(
            f"""SELECT COUNT(*) FROM (
                    SELECT provider_event_id, observed_at, book_key, market,
                           selection, line, COUNT(*) c
                    FROM {TABLE}
                    GROUP BY 1, 2, 3, 4, 5, 6 HAVING COUNT(*) > 1) t"""
        )[0]
        out["post_freeze_sample"] = [
            {
                "quote_id": r[0],
                "provider_event_id": r[1],
                "home_team": r[2],
                "away_team": r[3],
                "kickoff": str(r[4]),
                "sportsbook": r[5],
                "market": r[6],
                "selection": r[7],
                "line": r[8],
                "american_odds": r[9],
                "observed_at": str(r[10]),
                "collected_at": str(r[11]),
            }
            for r in allq(
                f"""SELECT quote_id, provider_event_id, home_team, away_team,
                           kickoff, sportsbook, market, selection, line,
                           american_odds, observed_at, collected_at
                    FROM {TABLE}
                    WHERE collected_at > %s::timestamptz
                    ORDER BY quote_id LIMIT 25""",
                (FREEZE_TS,),
            )
        ]
    dest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/nfl_freeze_forensic.json"
    with open(dest, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(
        json.dumps(
            {
                k: out[k]
                for k in (
                    "n_quotes",
                    "n_snapshots",
                    "extra_vs_frozen",
                    "post_freeze_inserts_by_collected_at",
                    "duplicate_key_groups",
                    "min_quote_id",
                    "max_quote_id",
                    "distinct_quote_ids",
                )
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
