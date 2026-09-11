"""Idempotent sync of ops/schedule-2026.json into public.games.

The checkpoint worker reads its game list from public.games. This script keeps
that table populated from the versioned schedule file. It introspects the live
table and only writes columns that exist, so it degrades gracefully if the
table gains or loses optional columns. Best-effort: exits 0 even when the
table is unreachable, so a sync hiccup never blocks collection.
"""
import datetime as dt
import json
import os
import pathlib
import sys

UTC = dt.timezone.utc
SCHEDULE = pathlib.Path(__file__).parent / "schedule-2026.json"


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
            writable = [c for c in ("game_id", "home_team", "away_team", "kickoff",
                                   "status", "season", "week") if c in cols]
            placeholders = ", ".join(["%s"] * len(writable))
            assignments = ", ".join(f"{c}=EXCLUDED.{c}" for c in writable if c != "game_id")
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
        print(f"schedule sync ok: {len(rows)} games upserted")
    except Exception as exc:
        print(f"schedule sync degraded (collection continues): {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
