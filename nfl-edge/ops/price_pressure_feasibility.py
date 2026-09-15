#!/usr/bin/env python3
"""Price-pressure feasibility: count exact-contract observations with >=3 books.

Read-only SELECTs against ncaaf_edge_historical_quotes. Zero API credits.
Seasons 2022-2024 only (2025 sealed).

An "exact contract" is (provider_event_id, market, selection, line, observed_at).
We count how many such contracts have >=3 distinct books with paired
de-vig fair probabilities.
"""
import os, sys, json, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/price_pressure_feasibility.json")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",")]
    if 2025 in seasons:
        print("2025 must stay sealed", file=sys.stderr)
        return 2

    import psycopg
    from ops import sports

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2

    hq = sports.historical_quotes_table(args.sport)

    # Count exact contracts with >=3 books, by season and market
    # Season derived from kickoff year (Aug-Dec = season year)
    query = f"""
    WITH contracts AS (
        SELECT
            provider_event_id, market, selection, line, observed_at,
            EXTRACT(YEAR FROM kickoff) AS season,
            COUNT(DISTINCT book_key) AS n_books,
            COUNT(*) AS n_quotes
        FROM public.{hq}
        WHERE market IN ('FULL_GAME_SPREAD', 'FULL_GAME_TOTAL')
          AND fair_probability IS NOT NULL
        GROUP BY provider_event_id, market, selection, line, observed_at,
                 EXTRACT(YEAR FROM kickoff)
    )
    SELECT season, market,
           COUNT(*) AS n_contracts,
           SUM(CASE WHEN n_books >= 3 THEN 1 ELSE 0 END) AS n_contracts_3plus,
           AVG(n_books) AS avg_books
    FROM contracts
    WHERE season = ANY(%s)
    GROUP BY season, market
    ORDER BY season, market
    """

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(query, (seasons,)).fetchall()

    result = {
        "scope": {"sport": args.sport, "seasons": seasons,
                  "markets": ["FULL_GAME_SPREAD", "FULL_GAME_TOTAL"]},
        "definition": ("exact contract = (provider_event_id, market, selection, "
                       "line, observed_at); >=3 distinct books with "
                       "non-null fair_probability"),
        "by_season_market": [
            {"season": int(r[0]), "market": r[1],
             "n_contracts": r[2], "n_contracts_3plus": r[3],
             "avg_books": round(float(r[4]), 2)}
            for r in rows
        ],
        "total_3plus": sum(r[3] for r in rows),
    }

    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
