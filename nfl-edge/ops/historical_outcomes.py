"""Immutable historical NFL outcome ingestion for research calibration.

This module imports settled games from nflverse's versioned games.csv into a
separate RLS-protected table.  It does not calculate edges or publish picks.
Existing rows are never overwritten: a conflicting result fails closed.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
import urllib.request

SOURCE_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
DEFAULT_SEASONS = (2022, 2023, 2024)

TEAM_NAMES = {
    "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens", "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers", "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys", "DEN": "Denver Broncos",
    "DET": "Detroit Lions", "GB": "Green Bay Packers",
    "HOU": "Houston Texans", "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars", "KC": "Kansas City Chiefs",
    "LA": "Los Angeles Rams", "LAC": "Los Angeles Chargers",
    "LV": "Las Vegas Raiders", "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings", "NE": "New England Patriots",
    "NO": "New Orleans Saints", "NYG": "New York Giants",
    "NYJ": "New York Jets", "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers", "SEA": "Seattle Seahawks",
    "SF": "San Francisco 49ers", "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans", "WAS": "Washington Commanders",
}

REQUIRED = {"game_id", "season", "week", "gameday", "home_team",
            "away_team", "home_score", "away_score"}


def _integer(value: str, field: str) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        raise ValueError(f"invalid {field}: {value!r}")
    if parsed < 0:
        raise ValueError(f"negative {field}: {parsed}")
    return parsed


def parse_outcomes(data: bytes, seasons=DEFAULT_SEASONS) -> list[dict]:
    """Parse settled rows only; reject unknown teams and malformed results."""
    rows = csv.DictReader(io.StringIO(data.decode("utf-8-sig")))
    if not rows.fieldnames or not REQUIRED.issubset(rows.fieldnames):
        missing = sorted(REQUIRED - set(rows.fieldnames or ()))
        raise ValueError(f"games.csv missing required columns: {missing}")
    wanted = set(seasons)
    outcomes = []
    seen = set()
    for row in rows:
        try:
            season = int(row["season"])
        except (TypeError, ValueError):
            continue
        if season not in wanted:
            continue
        if row["home_score"] in ("", "NA") or row["away_score"] in ("", "NA"):
            continue
        game_id = row["game_id"].strip()
        home_code, away_code = row["home_team"].strip(), row["away_team"].strip()
        if not game_id or game_id in seen:
            raise ValueError(f"missing or duplicate game_id: {game_id!r}")
        if home_code not in TEAM_NAMES or away_code not in TEAM_NAMES:
            raise ValueError(f"unmapped team code: {away_code}@{home_code}")
        try:
            game_date = dt.date.fromisoformat(row["gameday"])
            week = int(row["week"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid date/week for {game_id}") from exc
        outcomes.append({
            "nflverse_game_id": game_id,
            "season": season,
            "week": week,
            "game_type": (row.get("game_type") or "REG").strip(),
            "game_date": game_date,
            "home_code": home_code,
            "away_code": away_code,
            "home_team": TEAM_NAMES[home_code],
            "away_team": TEAM_NAMES[away_code],
            "home_score": _integer(row["home_score"], "home_score"),
            "away_score": _integer(row["away_score"], "away_score"),
        })
        seen.add(game_id)
    if not outcomes:
        raise ValueError("no settled outcomes parsed")
    return outcomes


def fetch_source(url=SOURCE_URL) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-outcomes/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def persist(conn, outcomes: list[dict], source_url: str, source_sha256: str) -> dict:
    columns = ("nflverse_game_id", "season", "week", "game_type", "game_date",
               "home_code", "away_code", "home_team", "away_team",
               "home_score", "away_score")
    inserted = existing = 0
    for outcome in outcomes:
        values = [outcome[c] for c in columns]
        row = conn.execute(
            f"INSERT INTO public.nfl_edge_historical_outcomes "
            f"({', '.join(columns)}, source_url, source_sha256) "
            f"VALUES ({', '.join(['%s'] * len(columns))}, %s, %s) "
            "ON CONFLICT (nflverse_game_id) DO NOTHING RETURNING nflverse_game_id",
            values + [source_url, source_sha256],
        ).fetchone()
        if row:
            inserted += 1
            continue
        stored = conn.execute(
            f"SELECT {', '.join(columns)} FROM public.nfl_edge_historical_outcomes "
            "WHERE nflverse_game_id=%s", (outcome["nflverse_game_id"],)
        ).fetchone()
        expected = tuple(values)
        if stored != expected:
            raise ValueError(f"immutable outcome conflict: {outcome['nflverse_game_id']}")
        existing += 1
    return {"attempted": len(outcomes), "inserted": inserted, "existing": existing}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", default="2022,2023,2024")
    args = parser.parse_args()
    seasons = tuple(int(x) for x in args.seasons.split(",") if x.strip())
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        raise ValueError("NFL_EDGE_DATABASE_URL is required")
    data = fetch_source()
    digest = hashlib.sha256(data).hexdigest()
    outcomes = parse_outcomes(data, seasons)
    import psycopg
    with psycopg.connect(database, connect_timeout=10) as conn:
        with conn.transaction():
            summary = persist(conn, outcomes, SOURCE_URL, digest)
    summary.update({"seasons": list(seasons), "source_url": SOURCE_URL,
                    "source_sha256": digest, "role": "research_data_only"})
    print(json.dumps(summary, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
