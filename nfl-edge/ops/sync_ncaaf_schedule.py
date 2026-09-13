"""Idempotent sync of the FREE NCAAF events feed into public.games (track D).

NCAAF research contract track D: bank immutable 2026 checkpoints. Uses the
FREE /v4/sports/americanfootball_ncaaf/events endpoint (0 provider credits).

game_id is deterministic: 'ncaaf-' + sha256(home|away|kickoff-date)[:16].
The provider re-mints event ids between snapshots, so matching is by exact
team strings + kickoff, never by provider id; the deterministic game_id keeps
the row stable across re-issues. The provider event id is NOT stored (no
known column); kickoff/teams are refreshed on every sync.

Writes sport='ncaaf' only; never touches NFL rows. A finished game is never
resurrected: status only flips scheduled -> scheduled.

Best-effort: exits 0 even when the feed or DB is unreachable, so a sync
hiccup never blocks collection.
"""
import datetime as dt
import hashlib
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "model"))
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))  # ops.sports

UTC = dt.timezone.utc
# Week-1 Saturday of the 2026 season (verified Saturday). Used ONLY for a
# best-effort informational week number; the collector plans from kickoff.
WEEK1_SATURDAY_2026 = dt.date(2026, 9, 5)


def game_id_for_event(home_team, away_team, kickoff):
    """Deterministic game id, stable across provider event-id re-issues."""
    kickoff_date = kickoff.astimezone(UTC).date().isoformat()
    digest = hashlib.sha256(
        "|".join([home_team, away_team, kickoff_date]).encode()).hexdigest()
    return "ncaaf-" + digest[:16]


def week_for_kickoff(kickoff):
    """Best-effort CFB week number for 2026 kickoffs; None otherwise."""
    d = kickoff.astimezone(UTC).date()
    if d.year != 2026:
        return None
    return (d - WEEK1_SATURDAY_2026).days // 7 + 1


def rows_for_events(events):
    """Map provider events to candidate game rows (pure; tested)."""
    rows = []
    for e in events or []:
        try:
            kickoff = dt.datetime.fromisoformat(
                str(e["commence_time"]).replace("Z", "+00:00"))
            if kickoff.tzinfo is None:
                continue
        except (KeyError, ValueError, TypeError):
            continue
        home = str(e.get("home_team") or "").strip()
        away = str(e.get("away_team") or "").strip()
        if not home or not away:
            continue
        kickoff = kickoff.astimezone(UTC)
        rows.append({
            "game_id": game_id_for_event(home, away, kickoff),
            "home_team": home,
            "away_team": away,
            "kickoff": kickoff,
            "status": "scheduled",
            "season": kickoff.year,
            "week": week_for_kickoff(kickoff),
            "sport": "ncaaf",
        })
    return rows


def main() -> int:
    from provider_oddsapi import fetch_events
    try:
        events, _headers = fetch_events(sport="ncaaf")
    except Exception as exc:
        print(f"ncaaf events feed unreachable: {type(exc).__name__}; skipping sync")
        return 0
    rows = rows_for_events(events)
    if not rows:
        print("no ncaaf events parsed; skipping sync")
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
            if "sport" not in cols:
                print("WARNING: public.games has no sport column "
                      "(migration 014 not applied); rows would be invisible "
                      "to the sport-filtered collector. Skipping sync.")
                return 0
            writable = [c for c in ("game_id", "home_team", "away_team",
                                   "kickoff", "status", "season", "week",
                                   "sport") if c in cols]
            placeholders = ", ".join(["%s"] * len(writable))
            assignments = []
            for c in writable:
                if c == "game_id":
                    continue
                if c == "status":
                    # Never resurrect a finished game.
                    assignments.append(
                        "status = CASE WHEN public.games.status = 'scheduled' "
                        "THEN EXCLUDED.status ELSE public.games.status END")
                else:
                    assignments.append(f"{c}=EXCLUDED.{c}")
            sql = (f"INSERT INTO public.games ({', '.join(writable)}) VALUES ({placeholders}) "
                   f"ON CONFLICT (game_id) DO UPDATE SET {', '.join(assignments)}")
            with connection.transaction():
                for row in rows:
                    values = []
                    for col in writable:
                        value = row[col]
                        if col in ("season", "week") and value is not None:
                            value = int(value)
                        values.append(value)
                    connection.execute(sql, values)
        print(f"ncaaf schedule sync ok: {len(rows)} games upserted")
    except Exception as exc:
        print(f"ncaaf schedule sync degraded (collection continues): {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
