"""Idempotent sync of ops/schedule-2026.json into public.games.

The checkpoint worker reads its game list from public.games.  The schedule
file uses nflverse game ids, while older rows may use a provider-independent
legacy id.  Before upserting, resolve every schedule row by a strict natural
key so the sync never creates or reactivates a second identity for the same
game.  Matching is explicit: season, week, normalized teams, and exact UTC
kickoff must all agree.
"""
import datetime as dt
import json
import os
import pathlib
import sys

UTC = dt.timezone.utc
SCHEDULE = pathlib.Path(__file__).parent / "schedule-2026.json"

# Explicit source aliases only.  No fuzzy team-name matching.
TEAM_ALIASES = {
    "AZ": "ARI", "ARI": "ARI",
    "JAC": "JAX", "JAX": "JAX",
    "LA": "LA", "LAR": "LA", "STL": "LA",
    "OAK": "LV", "LV": "LV",
    "SD": "LAC", "LAC": "LAC",
    "WSH": "WAS", "WAS": "WAS",
}


def normalize_team(value):
    team = str(value or "").strip().upper()
    return TEAM_ALIASES.get(team, team)


def natural_game_key(row):
    """Strict cross-source identity key; returns None when incomplete."""
    try:
        season = int(row["season"])
        week = int(row["week"])
        kickoff = row["kickoff"]
        if not isinstance(kickoff, dt.datetime):
            kickoff = dt.datetime.fromisoformat(str(kickoff).replace("Z", "+00:00"))
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=UTC)
        home = normalize_team(row["home_team"])
        away = normalize_team(row["away_team"])
    except (KeyError, TypeError, ValueError):
        return None
    if not home or not away:
        return None
    return season, week, away, home, kickoff.astimezone(UTC)


def prefer_existing_id(game_ids):
    """Deterministic compatibility order used by the checkpoint collector."""
    ids = sorted({str(value) for value in game_ids if value})
    if not ids:
        return None
    return min(ids, key=lambda value: (not value.startswith("nfl-"), value))


def resolve_existing_game_ids(rows, existing_rows):
    """Reuse a strict natural-key identity instead of inserting a duplicate.

    Returns new dictionaries and never mutates either input.  Ambiguity is
    preserved in ``aliases`` for logging; it is resolved deterministically to
    the same legacy-id preference already used by collect_checkpoints.py.
    """
    by_key = {}
    for existing in existing_rows:
        key = natural_game_key(existing)
        if key is not None:
            by_key.setdefault(key, []).append(existing.get("game_id"))

    resolved = []
    aliases = []
    for source in rows:
        row = dict(source)
        key = natural_game_key(row)
        candidates = by_key.get(key, []) if key is not None else []
        chosen = prefer_existing_id(candidates)
        if chosen and chosen != row["game_id"]:
            aliases.append({
                "schedule_game_id": row["game_id"],
                "resolved_game_id": chosen,
                "candidate_count": len(set(candidates)),
            })
            row["game_id"] = chosen
        resolved.append(row)
    return resolved, aliases


def upsert_assignments(writable):
    """Build updates without regressing a terminal game to scheduled."""
    assignments = []
    for column in writable:
        if column == "game_id":
            continue
        if column == "status":
            assignments.append(
                "status=CASE WHEN public.games.status='scheduled' "
                "THEN EXCLUDED.status ELSE public.games.status END"
            )
        else:
            assignments.append(f"{column}=EXCLUDED.{column}")
    return ", ".join(assignments)


def rows_for_schedule(data):
    """Map schedule entries to candidate game rows (pure; tested)."""
    games = data if isinstance(data, list) else data.get("games", [])
    rows = []
    for entry in games:
        try:
            kickoff = dt.datetime.fromisoformat(str(entry["kickoff"]).replace("Z", "+00:00"))
            if kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=UTC)
        except (KeyError, ValueError, TypeError):
            continue
        game_id = str(entry.get("game_id") or "").strip()
        home = str(entry.get("home_team") or "").strip()
        away = str(entry.get("away_team") or "").strip()
        if not game_id or not home or not away:
            continue
        rows.append({
            "game_id": game_id,
            "home_team": home,
            "away_team": away,
            "kickoff": kickoff.astimezone(UTC),
            "status": str(entry.get("status") or "scheduled"),
            "season": entry.get("season"),
            "week": entry.get("week"),
        })
    return rows


def main() -> int:
    try:
        data = json.loads(SCHEDULE.read_text())
    except (OSError, ValueError) as exc:
        print(f"schedule unreadable: {exc}; skipping sync")
        return 0
    rows = rows_for_schedule(data)
    if not rows:
        print("no schedule rows parsed; skipping sync")
        return 0

    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        print("NFL_EDGE_DATABASE_URL is not set; skipping sync")
        return 0
    try:
        import psycopg
        with psycopg.connect(database, connect_timeout=10) as connection:
            connection.execute("SET statement_timeout='15s'")
            cols = {r[0] for r in connection.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='games'").fetchall()}
            if "game_id" not in cols:
                print("public.games has no game_id column; skipping sync")
                return 0

            # Resolve only against the relevant sport/season/week scope.  The
            # exact natural key prevents cross-week or rescheduled-game guesses.
            seasons = sorted({int(r["season"]) for r in rows if r["season"] is not None})
            weeks = sorted({int(r["week"]) for r in rows if r["week"] is not None})
            existing = []
            if seasons and weeks:
                existing_columns = (
                    "game_id", "season", "week", "away_team", "home_team", "kickoff"
                )
                existing = [dict(zip(existing_columns, record)) for record in connection.execute(
                    "SELECT game_id,season,week,away_team,home_team,kickoff "
                    "FROM public.games WHERE sport='nfl' "
                    "AND season = ANY(%s) AND week = ANY(%s)",
                    (seasons, weeks)).fetchall()]
            rows, aliases = resolve_existing_game_ids(rows, existing)

            writable = [c for c in ("game_id", "home_team", "away_team", "kickoff",
                                   "status", "season", "week") if c in cols]
            placeholders = ", ".join(["%s"] * len(writable))
            assignments = upsert_assignments(writable)
            sql = (f"INSERT INTO public.games ({', '.join(writable)}) VALUES ({placeholders}) "
                   f"ON CONFLICT (game_id) DO UPDATE SET {assignments}")
            with connection.transaction():
                for row in rows:
                    values = []
                    for col in writable:
                        value = row[col]
                        if col == "season" and value is not None:
                            value = int(value)
                        if col == "week" and value is not None:
                            value = int(value)
                        values.append(value)
                    connection.execute(sql, values)
        print(f"schedule sync ok: {len(rows)} games upserted; "
              f"{len(aliases)} schedule ids resolved to existing identities")
    except Exception as exc:
        print(f"schedule sync degraded (collection continues): {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
