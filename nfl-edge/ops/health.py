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
# Collector/picks at 150m (not the workflow's 5-min cron): GitHub schedule
# triggers fire at an effective ~2h cadence in practice (2026-09-16), and the
# health-watch runbook treats 30-150m as the healthy range with backup
# dispatch only beyond 150m. Watchdog vs native-schedule origin is tracked
# separately in the health-watch state file so fallback runs never mask
# missed schedules.
THRESHOLDS = {
    "schedule_sync": dt.timedelta(hours=26),
    "collector": dt.timedelta(minutes=150),
    "picks": dt.timedelta(minutes=150),
    "settlement": dt.timedelta(hours=30),
    # NFL prospective prop layer (non-core research infrastructure): staleness
    # escalates to degraded, never down — it must not flap the shadow
    # pipeline's status. Design §6.2: deliberately non-core, unlike the
    # featured collector which stays core.
    "prop_collector": dt.timedelta(hours=6),
}
# Core components whose staleness means the pipeline is down (in season).
CORE = ("schedule_sync", "collector", "picks")
# Non-core research components that have never produced a heartbeat are
# "not yet commissioned", not broken: they report "missing" WITHOUT
# escalating (pre-calibration the prop layer has no staleness to measure,
# and a permanent pre-launch DEGRADED would spam the §6.3 alert). Once a
# heartbeat exists, staleness escalates to degraded — never down.
NEVER_COMMISSIONED_OK = ("prop_collector",)
# Below this many remaining provider credits, report degraded.
CREDITS_WARN_BELOW = 60
# If no game kicks off within this window, the league is idle: stale
# heartbeats are expected and report as idle instead of down.
SEASON_WINDOW = dt.timedelta(days=45)


# Severity order: a worse state never gets overwritten by a milder one.
_SEVERITY = {"ok": 0, "idle": 0, "degraded": 1, "down": 2}


def evaluate(heartbeats: dict[str, dict], now: dt.datetime,
             upcoming_games: int, completeness_missed: dict | None = None) -> dict:
    """Pure health logic. heartbeats maps component -> {last_ok_at, detail}.

    completeness_missed maps component -> missed-snapshot count for the
    latest checkpoint window (read from nfl_prop_completeness by check());
    it is exposed in the component detail for visibility — a missed single
    snapshot is degraded-worthy context in detail, while the component
    status flips only on sustained staleness.
    """
    in_season = upcoming_games > 0
    components: dict[str, dict] = {}
    worst = "ok"

    def escalate(state: str) -> None:
        nonlocal worst
        if _SEVERITY[state] > _SEVERITY[worst]:
            worst = state

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
        entry: dict = {"status": status, "age_seconds":
                       None if age is None else round(age)}
        if completeness_missed and name in completeness_missed:
            entry["missed_snapshots"] = completeness_missed[name]
        components[name] = entry
        if status in ("stale", "missing"):
            # Non-core components never go down. A non-core research
            # component that never produced a heartbeat is not yet
            # commissioned: visible as "missing", no escalation.
            if name in CORE:
                escalate("down")
            elif status == "missing" and name in NEVER_COMMISSIONED_OK:
                pass
            else:
                escalate("degraded")

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
    # Prop-layer completeness: missed snapshots for the latest checkpoint
    # window, visible in the same health payload immediately (design §6.2).
    # Prop-layer completeness: missed snapshots for the latest (season,
    # week) window, visible in the same health payload immediately
    # (design §6.2). A missed single snapshot is degraded-worthy context in
    # the detail payload; the component status flips on sustained
    # staleness, not on one gap.
    completeness_missed = {}
    try:
        cur.execute("""
            SELECT checkpoint_name, SUM(missed)::int AS missed
            FROM public.nfl_prop_completeness
            WHERE (season, week) = (
                SELECT season, week
                FROM public.nfl_prop_completeness
                ORDER BY season DESC NULLS LAST, week DESC NULLS LAST
                LIMIT 1
            )
            GROUP BY checkpoint_name
        """)
        rows = cur.fetchall()
        if rows:
            by_checkpoint = {r[0]: (r[1] or 0) for r in rows}
            completeness_missed["prop_collector"] = {
                "by_checkpoint": by_checkpoint,
                "total": sum(by_checkpoint.values()),
            }
    except Exception:
        # View/table absent (pre-migration) — completeness is unknown,
        # never fatal to the health check.
        pass
    now = dt.datetime.now(dt.timezone.utc)
    return evaluate(heartbeats, now, upcoming, completeness_missed)


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
