"""Pipeline health check for NFL Edge.

Reads component heartbeats (written by ops/heartbeat.py and the collector),
compares them against freshness thresholds, and reports a coarse status:

    ok       - everything fresh (or the league is idle out of season)
    degraded - settlement stale, or odds credits running low
    down     - a core in-season component (sync/collect/picks) is stale

Exit codes: 0 = ok, 1 = degraded, 2 = down. Prints a JSON report; the JSON
contains no secrets or row-level data, only component freshness.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

# Max acceptable age of each component's last successful run.
THRESHOLDS = {
    "schedule_sync": dt.timedelta(hours=26),
    "collector": dt.timedelta(minutes=75),
    "picks": dt.timedelta(minutes=75),
    "settlement": dt.timedelta(hours=30),
}
# Core components whose staleness means the pipeline is down (in season).
CORE = ("schedule_sync", "collector", "picks")
# Below this many remaining provider credits, report degraded.
CREDITS_WARN_BELOW = 60
# If no game kicks off within this window, the league is idle: stale
# heartbeats are expected and report as idle instead of down.
SEASON_WINDOW = dt.timedelta(days=45)


def evaluate(heartbeats: dict[str, dict], now: dt.datetime,
             upcoming_games: int) -> dict:
    """Pure health logic. heartbeats maps component -> {last_ok_at, detail}."""
    in_season = upcoming_games > 0
    components: dict[str, dict] = {}
    worst = "ok"
    for name, limit in THRESHOLDS.items():
        hb = heartbeats.get(name)
        if not in_season:
            components[name] = {"status": "idle", "age_seconds": None}
            continue
        if hb is None:
            status, age = "missing", None
        else:
            age = (now - hb["last_ok_at"]).total_seconds()
            status = "ok" if age <= limit.total_seconds() else "stale"
        components[name] = {"status": status, "age_seconds":
                            None if age is None else round(age)}
        if status in ("stale", "missing"):
            worst = "down" if name in CORE else "degraded"

    credits_remaining = None
    detail = (heartbeats.get("collector") or {}).get("detail") or {}
    if isinstance(detail, dict):
        credits_remaining = detail.get("credits_remaining")
    if (in_season and isinstance(credits_remaining, (int, float))
            and credits_remaining < CREDITS_WARN_BELOW):
        components["odds_credits"] = {"status": "low",
                                      "remaining": credits_remaining}
        if worst == "ok":
            worst = "degraded"

    return {
        "status": worst,
        "in_season": in_season,
        "upcoming_games": upcoming_games,
        "components": components,
        "checked_at": now.isoformat(),
    }


def check(conn) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT component, last_ok_at, detail "
                "FROM public.nfl_edge_heartbeats")
    heartbeats = {}
    for component, last_ok_at, detail in cur.fetchall():
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except ValueError:
                detail = None
        heartbeats[component] = {"last_ok_at": last_ok_at, "detail": detail}
    try:
        cur.execute("SELECT count(*) FROM public.games "
                    "WHERE kickoff BETWEEN now() AND now() + interval '45 days'")
        upcoming = int(cur.fetchone()[0])
    except Exception:
        upcoming = 0
    now = dt.datetime.now(dt.timezone.utc)
    return evaluate(heartbeats, now, upcoming)


def main() -> int:
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        raise ValueError("NFL_EDGE_DATABASE_URL is required")
    import psycopg
    conn = psycopg.connect(database)
    try:
        report = check(conn)
    finally:
        conn.close()
    print(json.dumps(report))
    return {"ok": 0, "degraded": 1, "down": 2}[report["status"]]


if __name__ == "__main__":
    sys.exit(main())
