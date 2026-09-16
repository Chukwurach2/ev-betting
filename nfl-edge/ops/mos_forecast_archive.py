#!/usr/bin/env python3
"""Prospective NFL MOS forecast archive collector.

INFRASTRUCTURE collection, not a claim-bearing test. No modeling, no
outcomes, no hypothesis work. Free IEM HTTP calls only -- zero Odds API
credits.

What it does, 4x daily (cron '10 4,10,16,22 * * *' -- each run fetches the
MOS cycle issued ~4h earlier, after the documented dissemination lag):
  1. Loads the 2026 schedule (live nflverse download; bundled snapshot
     nfl-edge/ops/nflverse_schedules_2026.json as fallback).
  2. For each game with kickoff in (now - 3h, now + 8d):
       cutoff = kickoff - 24h  (frozen prediction cutoff, RULE v1)
       resolves stadium -> IEM MOS station via nfl_stadium_mos.json
       (explicit unmapped_stadium state when there is no verified station;
       never a nearest-guess).
  3. While now <= cutoff, archives EVERY issued GFS MOS cycle not already
     stored (one archive row per ftime projection, raw JSONB). retrieved_at
     is always <= cutoff by construction -- the information set is
     prospective, never reconstructed after the decision point.
  4. Appends one selection-log row per game per run: the mechanical rule
     (latest eligible cycle = max runtime with runtime + 4h <= cutoff).
  5. Logs every attempt (available / absent / request_failed /
     unmapped_stadium / skipped_after_cutoff). Failures are preserved, never
     backfilled.

The archive table is append-only (DB trigger rejects UPDATE/DELETE); this
script only ever INSERTs.
"""
import argparse
import csv
import gzip
import io
import json
import os
import pathlib
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

# Frozen research rules (RULE_VERSION v1). Changing any of these is a
# versioned rule change, not a silent edit.
RULE_VERSION = "v1"
FORECAST_MODEL = "GFS"
CUTOFF_HOURS = 24            # prediction_cutoff = kickoff - 24h
DISSEMINATION_LAG_HOURS = 4  # cycle eligible iff runtime + 4h <= cutoff
MOS_CYCLE_HOURS = (0, 6, 12, 18)
CYCLE_FRESHNESS_MIN = 30     # only fetch cycles issued >= 30 min ago
LOOKBACK_HOURS = 72          # bound on how far back a run looks for cycles
LOOKAHEAD_DAYS = 8
ACTIVE_PAST_HOURS = 3
HTTP_TIMEOUT = 30
HTTP_PACING_S = 1.2
MAX_HTTP_CALLS = 400

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
IEM_MOS_URL = "https://mesonet.agron.iastate.edu/api/1/mos.json"
NFLVERSE_SCHEDULES_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/"
    "schedules/games.csv.gz"
)
USER_AGENT = "nfl-edge-mos-archive/1.0 (prospective forecast collection)"

HERE = pathlib.Path(__file__).parent
MAPPING_PATH = HERE / "nfl_stadium_mos.json"
SCHEDULE_FALLBACK_PATH = HERE / "nflverse_schedules_2026.json"


# ---------------------------------------------------------------- pure logic

def parse_kickoff(gameday: str, gametime: str) -> datetime:
    """nflverse gameday 'YYYY-MM-DD' + gametime 'HH:MM' (ET) -> UTC."""
    for fmt in ("%H:%M", "%I:%M%p"):
        try:
            t = datetime.strptime(gametime.strip(), fmt).time()
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"unparseable gametime: {gametime!r}")
    d = datetime.strptime(gameday.strip(), "%Y-%m-%d").date()
    return datetime.combine(d, t, tzinfo=ET).astimezone(UTC)


def prediction_cutoff(kickoff: datetime) -> datetime:
    return kickoff - timedelta(hours=CUTOFF_HOURS)


def is_eligible(runtime: datetime, cutoff: datetime) -> bool:
    return runtime + timedelta(hours=DISSEMINATION_LAG_HOURS) <= cutoff


def select_cycle(runtimes, cutoff):
    """Mechanical selection: max eligible runtime, or None."""
    eligible = [r for r in runtimes if is_eligible(r, cutoff)]
    return max(eligible) if eligible else None


def candidate_runtimes(now: datetime, lookback_hours: int = LOOKBACK_HOURS):
    """6-hour MOS grid times in [now - lookback, now - freshness]."""
    start = (now - timedelta(hours=lookback_hours)).replace(
        minute=0, second=0, microsecond=0)
    # floor to grid
    start = start.replace(hour=(start.hour // 6) * 6)
    latest = (now - timedelta(minutes=CYCLE_FRESHNESS_MIN)).replace(
        minute=0, second=0, microsecond=0)
    latest = latest.replace(hour=(latest.hour // 6) * 6)
    out, cur = [], start
    while cur <= latest:
        out.append(cur)
        cur += timedelta(hours=6)
    return out


# ---------------------------------------------------------------- I/O

def http_get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return json.loads(resp.read().decode())


def fetch_mos_cycle(station: str, runtime: datetime):
    rt = runtime.strftime("%Y-%m-%dT%H:00:00Z")
    url = f"{IEM_MOS_URL}?station={station}&model={FORECAST_MODEL}&runtime={rt}"
    payload = http_get_json(url)
    return payload.get("data", []) or []


def parse_mos_time(s: str) -> datetime:
    # IEM returns "YYYY-MM-DD HH:MM" in UTC.
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


def load_schedule(season: int):
    """Live nflverse download first; bundled snapshot as fallback."""
    games = None
    source = None
    try:
        req = urllib.request.Request(
            NFLVERSE_SCHEDULES_URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        rows = list(csv.DictReader(
            gzip.open(io.BytesIO(raw), "rt")))
        games = [r for r in rows
                 if r["season"] == str(season) and r["game_type"] == "REG"]
        source = "live-nflverse"
    except Exception as exc:  # noqa: BLE001 - fallback is the point
        print(f"live schedule download failed ({exc}); using bundled snapshot")
    if games is None:
        snap = json.loads(SCHEDULE_FALLBACK_PATH.read_text())
        games = [g for g in snap["games"] if str(g["season"]) == str(season)]
        source = "bundled-snapshot"
    return games, source


# ---------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--out", default="/tmp/mos_archive.json")
    ap.add_argument("--dry-run", action="store_true",
                    help="no DB writes; fetch real IEM data and report")
    ap.add_argument("--max-http-calls", type=int, default=MAX_HTTP_CALLS)
    ap.add_argument("--lookback-hours", type=int, default=LOOKBACK_HOURS)
    args = ap.parse_args()

    now = datetime.now(UTC)
    mapping = json.loads(MAPPING_PATH.read_text())
    games, sched_source = load_schedule(args.season)

    summary = {
        "run_at": now.isoformat(),
        "rule_version": RULE_VERSION,
        "model": FORECAST_MODEL,
        "season": args.season,
        "schedule_source": sched_source,
        "dry_run": args.dry_run,
        "status": "ok",
        "n_games_active": 0,
        "n_games_unmapped": 0,
        "n_http_calls": 0,
        "n_cycles_archived": 0,
        "n_rows_inserted": 0,
        "attempts": [],
        "games": [],
    }

    if not games:
        summary["status"] = "schedule_unavailable"
        pathlib.Path(args.out).write_text(json.dumps(summary, indent=2))
        print("schedule_unavailable: no games loaded")
        return 0

    active = []
    for g in games:
        try:
            kickoff = parse_kickoff(g["gameday"], g["gametime"])
        except ValueError as exc:
            summary["attempts"].append({
                "nflverse_game_id": g.get("game_id"),
                "status": "bad_kickoff", "detail": str(exc)})
            continue
        if (now - timedelta(hours=ACTIVE_PAST_HOURS)
                <= kickoff <= now + timedelta(days=LOOKAHEAD_DAYS)):
            active.append((g, kickoff))
    summary["n_games_active"] = len(active)

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    conn = None
    if not args.dry_run:
        if not dsn:
            print("NFL_EDGE_DATABASE_URL is not set; refusing live run")
            return 1
        import psycopg
        from psycopg.types.json import Jsonb
        conn = psycopg.connect(dsn, connect_timeout=10)

    def db_archived_runtimes(game_id, station):
        if conn is None:
            return set()
        rows = conn.execute(
            "SELECT DISTINCT runtime FROM public.nfl_mos_forecast_archive "
            "WHERE nflverse_game_id = %s AND mos_station = %s "
            "AND forecast_model = %s",
            (game_id, station, FORECAST_MODEL)).fetchall()
        return {r[0] for r in rows}

    def db_insert_archive_row(row):
        cur = conn.execute(
            "INSERT INTO public.nfl_mos_forecast_archive "
            "(nflverse_game_id, season, week, home_team, away_team, stadium,"
            " mos_station, forecast_model, runtime, ftime, kickoff,"
            " prediction_cutoff, raw) "
            "VALUES (%(nflverse_game_id)s, %(season)s, %(week)s,"
            " %(home_team)s, %(away_team)s, %(stadium)s, %(mos_station)s,"
            " %(forecast_model)s, %(runtime)s, %(ftime)s, %(kickoff)s,"
            " %(prediction_cutoff)s, %(raw)s) "
            "ON CONFLICT DO NOTHING",
            row)
        return cur.rowcount  # 1 inserted, 0 already archived

    def db_insert(table, cols, vals):
        placeholders = ", ".join(["%s"] * len(vals))
        conn.execute(
            f"INSERT INTO public.{table} ({', '.join(cols)}) "
            f"VALUES ({placeholders})", vals)

    http_calls = 0
    for g, kickoff in sorted(active, key=lambda t: t[1]):
        game_id = g["game_id"]
        week = int(g["week"])
        stadium = g["stadium"]
        cutoff = prediction_cutoff(kickoff)
        entry = mapping.get(stadium)
        game_rec = {"game_id": game_id, "week": week, "stadium": stadium,
                    "kickoff": kickoff.isoformat(),
                    "cutoff": cutoff.isoformat()}

        if not entry or entry.get("status") != "verified" or not entry.get("mos_station"):
            summary["n_games_unmapped"] += 1
            summary["attempts"].append({
                "nflverse_game_id": game_id, "stadium": stadium,
                "status": "unmapped_stadium",
                "detail": (entry or {}).get("unavailable_reason")
                          or "no mapping entry"})
            if conn is not None:
                db_insert("nfl_mos_retrieval_attempts",
                          ["nflverse_game_id", "stadium", "forecast_model",
                           "status", "detail"],
                          [game_id, stadium, FORECAST_MODEL,
                           "unmapped_stadium",
                           (entry or {}).get("unavailable_reason")])
                db_insert("nfl_mos_cycle_selection_log",
                          ["nflverse_game_id", "season", "week",
                           "rule_version", "prediction_cutoff",
                           "selected_runtime", "n_cycles_archived",
                           "n_eligible_cycles", "is_final"],
                          [game_id, args.season, week, RULE_VERSION, cutoff,
                           None, 0, 0, now > cutoff])
            game_rec["status"] = "unmapped_stadium"
            summary["games"].append(game_rec)
            continue

        station = entry["mos_station"]
        game_rec["station"] = station
        archived = db_archived_runtimes(game_id, station)

        if now > cutoff:
            summary["attempts"].append({
                "nflverse_game_id": game_id, "stadium": stadium,
                "mos_station": station,
                "status": "skipped_after_cutoff",
                "detail": f"cutoff {cutoff.isoformat()} passed"})
            runtimes = sorted(archived)
            selected = select_cycle(runtimes, cutoff)
            if conn is not None:
                db_insert("nfl_mos_cycle_selection_log",
                          ["nflverse_game_id", "season", "week",
                           "rule_version", "prediction_cutoff",
                           "selected_runtime", "n_cycles_archived",
                           "n_eligible_cycles", "is_final"],
                          [game_id, args.season, week, RULE_VERSION, cutoff,
                           selected, len(runtimes),
                           sum(1 for r in runtimes if is_eligible(r, cutoff)),
                           True])
            game_rec["status"] = "skipped_after_cutoff"
            game_rec["selected_runtime"] = (
                selected.isoformat() if selected else None)
            summary["games"].append(game_rec)
            continue

        new_cycles = 0
        for rt in candidate_runtimes(now, args.lookback_hours):
            if rt in archived:
                continue
            if http_calls >= args.max_http_calls:
                summary["attempts"].append({
                    "nflverse_game_id": game_id, "status": "call_budget_hit",
                    "detail": f"stopped at {args.max_http_calls} calls"})
                break
            http_calls += 1
            summary["n_http_calls"] = http_calls
            try:
                rows = fetch_mos_cycle(station, rt)
                time.sleep(HTTP_PACING_S)
            except Exception as exc:  # noqa: BLE001 - failures are data
                summary["attempts"].append({
                    "nflverse_game_id": game_id, "stadium": stadium,
                    "mos_station": station, "runtime": rt.isoformat(),
                    "status": "request_failed", "detail": str(exc)[:200]})
                if conn is not None:
                    db_insert("nfl_mos_retrieval_attempts",
                              ["nflverse_game_id", "stadium", "mos_station",
                               "forecast_model", "runtime", "status",
                               "detail"],
                              [game_id, stadium, station, FORECAST_MODEL, rt,
                               "request_failed", str(exc)[:500]])
                continue
            if not rows:
                summary["attempts"].append({
                    "nflverse_game_id": game_id, "mos_station": station,
                    "runtime": rt.isoformat(), "status": "absent",
                    "detail": "HTTP 200, zero MOS rows"})
                if conn is not None:
                    db_insert("nfl_mos_retrieval_attempts",
                              ["nflverse_game_id", "stadium", "mos_station",
                               "forecast_model", "runtime", "status",
                               "detail"],
                              [game_id, stadium, station, FORECAST_MODEL, rt,
                               "absent", "HTTP 200, zero MOS rows"])
                continue
            inserted = 0
            sample = None
            for fr in rows:
                ftime = parse_mos_time(fr["ftime"])
                sample = sample or {k: fr.get(k) for k in
                                    ("tmp", "wdr", "wsp", "p06")}
                if conn is not None:
                    inserted += db_insert_archive_row({
                        "nflverse_game_id": game_id, "season": args.season,
                        "week": week, "home_team": g["home_team"],
                        "away_team": g["away_team"], "stadium": stadium,
                        "mos_station": station,
                        "forecast_model": FORECAST_MODEL, "runtime": rt,
                        "ftime": ftime, "kickoff": kickoff,
                        "prediction_cutoff": cutoff,
                        "raw": Jsonb(fr)})
            new_cycles += 1
            archived.add(rt)
            summary["n_cycles_archived"] += 1
            summary["n_rows_inserted"] += inserted if conn is not None else len(rows)
            summary["attempts"].append({
                "nflverse_game_id": game_id, "mos_station": station,
                "runtime": rt.isoformat(), "status": "available",
                "detail": f"{len(rows)} ftime rows", "sample": sample})
            if conn is not None:
                db_insert("nfl_mos_retrieval_attempts",
                          ["nflverse_game_id", "stadium", "mos_station",
                           "forecast_model", "runtime", "status", "detail"],
                          [game_id, stadium, station, FORECAST_MODEL, rt,
                           "available", f"{len(rows)} ftime rows"])

        runtimes = sorted(archived)
        selected = select_cycle(runtimes, cutoff)
        if conn is not None:
            db_insert("nfl_mos_cycle_selection_log",
                      ["nflverse_game_id", "season", "week", "rule_version",
                       "prediction_cutoff", "selected_runtime",
                       "n_cycles_archived", "n_eligible_cycles", "is_final"],
                      [game_id, args.season, week, RULE_VERSION, cutoff,
                       selected, len(runtimes),
                       sum(1 for r in runtimes if is_eligible(r, cutoff)),
                       False])
        game_rec["status"] = "active"
        game_rec["cycles_archived_this_run"] = new_cycles
        game_rec["selected_runtime"] = (
            selected.isoformat() if selected else None)
        summary["games"].append(game_rec)

    if conn is not None:
        conn.commit()
        conn.close()

    pathlib.Path(args.out).write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: summary[k] for k in
                      ("status", "n_games_active", "n_games_unmapped",
                       "n_http_calls", "n_cycles_archived",
                       "n_rows_inserted")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
