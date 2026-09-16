#!/usr/bin/env python3
"""Targeted follow-up: cross-tab market x post-freeze, and hunt for value
modifications to pre-freeze spread/total rows. Read-only."""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg  # noqa: E402

TABLE = "nfl_edge_historical_quotes"
FREEZE_TS = "2026-09-12T12:45:00+00:00"


def main() -> None:
    url = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not url:
        sys.exit(10)
    out: dict = {}
    with psycopg.connect(url) as conn:
        q = lambda sql, *a: conn.execute(sql, *a).fetchall()  # noqa: E731
        out["market_x_postfreeze"] = [
            (m, str(post), c)
            for m, post, c in q(
                f"""SELECT market, collected_at > %s::timestamptz AS post, COUNT(*)
                    FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2""",
                (FREEZE_TS,),
            )
        ]
        # Any spread/total row touched after freeze (collected_at is insert
        # metadata; an UPDATE would not move it -- so also check xmax? not
        # available. Instead: look for spread/total rows whose collected_at
        # is AFTER the freeze.)
        # Also: value-level sanity -- compare a deterministic aggregate of the
        # frozen subset that is sensitive to value changes but cheap.
        out["spread_total_postfreeze_n"] = q(
            f"""SELECT COUNT(*) FROM {TABLE}
                WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')
                  AND collected_at > %s::timestamptz""",
            (FREEZE_TS,),
        )[0][0]
        out["moneyline_prefreeze_n"] = q(
            f"""SELECT COUNT(*) FROM {TABLE}
                WHERE market = 'FULL_GAME_MONEYLINE'
                  AND collected_at <= %s::timestamptz""",
            (FREEZE_TS,),
        )[0][0]
    dest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/nfl_freeze_followup.json"
    with open(dest, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
