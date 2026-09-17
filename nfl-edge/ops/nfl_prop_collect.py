#!/usr/bin/env python3
"""NFL prospective prop/alternative-market collector (design:
nfl-edge/docs/nfl-prospective-prop-layer.md).

Collects prop quotes prospectively with controlled decision snapshots.
Collects NOTHING analytical: no thresholds, no features, no market
selection, no shadow signals, no pick engine, no app surface.

MODES
  --dry-run (default): executes the full cycle — schedule enumeration,
    checkpoint planning, ID resolution, per-run projection, cap check —
    and makes ZERO /odds calls and ZERO writes to production tables.
    Output is an immutable JSON artifact (the record of the run).
    In dry-run the events list comes from --fixture-events (a recorded
    live payload); the free live events endpoint is NEVER probed without
    the user-authorized calibration gate, per the design.

  --live: ONLY when env NFL_PROP_LIVE_OK=1. Any other value (or unset)
    makes live mode REFUSE with a loud non-zero exit. Live mode performs
    the real paid /odds pulls, DB writes, quota logging and heartbeat.

HARD IDENTITY RULE (from the H-D incident 2026-09-16): the provider
re-issues event IDs across snapshots. A provider event_id is valid ONLY
for the tick in which it was resolved from the tick-fresh events list.
This collector never reads a previously stored provider ID and reuses
it; each snapshot stores its own provider_event_id_at_snapshot.

Credit discipline: the run projects paid calls (due checkpoints x the
planned per-call cost) and aborts with ZERO paid calls when the
projection exceeds NFL_PROP_RUN_CREDIT_CAP (default 90) — the
nfl-hd-pull.yml ceiling-abort pattern, applied per run.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import checkpoints  # noqa: E402
import nfl_prop_identity  # noqa: E402

UTC = dt.timezone.utc
ET = ZoneInfo("America/New_York")

API = "https://api.the-odds-api.com"
SPORT_KEY = "americanfootball_nfl"

# v1 market set (bounded, probe-proven at T-24 2026-09-16).
# player_sacks EXCLUDED (probe-absent at T-24) — do not collect it.
PROP_MARKETS = ["player_pass_yds", "player_pass_attempts", "player_rush_attempts"]
PROP_BOOKMAKERS = (
    "draftkings,fanduel,betmgm,betrivers,espnbet,williamhill_us,fanatics"
)
REGIONS = ["us"]

# Default checkpoints: configurable via NFL_PROP_CHECKPOINTS as a JSON list
# of [name, minute-offset] pairs — no code change to retune.
DEFAULT_CHECKPOINTS = [
    ("T-24h", 1440),
    ("T-12h", 720),
    ("T-6h", 360),
    ("T-3h", 180),
    ("T-90m", 90),
    ("Close", 5),
]

# Economics model §4: A1 nominal per-call cost = (# markets) x (# regions).
# A5 planning margin (2x) applies until the empirical cost model replaces it.
PLANNING_MARGIN = 2.0

# Attempt statuses (match the migration 020 CHECK constraint).
ST_OK = "ok"
ST_NO_EVENT = "no_event_id"
ST_EMPTY = "empty_response"
ST_FAILED = "request_failed"
ST_SKIPPED_BUDGET = "skipped_budget"
ST_ABORTED_CAP = "aborted_cap"
ST_AMBIGUOUS = "ambiguous_match"

HERE = os.path.dirname(os.path.abspath(__file__))
BUNDLED_SCHEDULE = os.path.join(HERE, "nflverse_schedules_2026.json")
STADIUM_MOS_PATH = os.path.join(HERE, "nfl_stadium_mos.json")


# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------

def load_checkpoints_config():
    raw = os.environ.get("NFL_PROP_CHECKPOINTS")
    if not raw:
        return list(DEFAULT_CHECKPOINTS)
    parsed = json.loads(raw)
    out = []
    for name, minutes in parsed:
        out.append((str(name), int(minutes)))
    if not out:
        raise ValueError("NFL_PROP_CHECKPOINTS must be a non-empty list")
    return out


def credit_cap():
    return int(os.environ.get("NFL_PROP_RUN_CREDIT_CAP", "90"))


def nominal_per_call(n_markets=None):
    """A1: (# unique markets present) x (# regions), 1 credit each."""
    return (n_markets if n_markets is not None else len(PROP_MARKETS)) * len(REGIONS)


def planned_per_call(n_markets=None):
    """Planning number: A1 nominal x the A5 2x margin until calibration lands."""
    return nominal_per_call(n_markets) * PLANNING_MARGIN


# --------------------------------------------------------------------------
# game enumeration
# --------------------------------------------------------------------------

def parse_kickoff(entry):
    """Kickoff as an aware UTC datetime.

    Bundled/live schedule rows carry gameday + gametime (America/New_York);
    fixtures may carry an ISO 'kickoff' directly.
    """
    if entry.get("kickoff"):
        t = dt.datetime.fromisoformat(str(entry["kickoff"]).replace("Z", "+00:00"))
        if t.tzinfo is None:
            raise ValueError("fixture kickoff must include timezone")
        return t.astimezone(UTC)
    gameday = entry.get("gameday")
    gametime = (entry.get("gametime") or "").strip()
    if not gameday or not gametime:
        raise ValueError(f"game {entry.get('game_id')} has no kickoff info")
    naive = dt.datetime.strptime(f"{gameday} {gametime}", "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=ET).astimezone(UTC)


def load_games_dry_run(schedule_path):
    with open(schedule_path) as f:
        payload = json.load(f)
    games = payload["games"] if isinstance(payload, dict) else payload
    out = []
    for g in games:
        out.append({
            "game_id": g["game_id"],
            "home_team": g.get("home_team"),
            "away_team": g.get("away_team"),
            "stadium": g.get("stadium"),
            "kickoff": parse_kickoff(g),
            "season": int(g.get("season") or 0) or None,
            "week": int(g.get("week")) if g.get("week") else None,
            "status": g.get("status", "scheduled"),
        })
    return out


def load_games_live(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT game_id, home_team, away_team, kickoff, status, season, week "
            "FROM public.games WHERE sport = 'nfl'"
        )
        rows = cur.fetchall()
    return [
        {"game_id": r[0], "home_team": r[1], "away_team": r[2],
         "stadium": None, "kickoff": r[3],
         "status": r[4], "season": r[5], "week": r[6]}
        for r in rows
    ]


def stadium_of(game, bundle_lookup=None):
    if game.get("stadium"):
        return game["stadium"]
    if bundle_lookup and game["game_id"] in bundle_lookup:
        return bundle_lookup[game["game_id"]].get("stadium")
    return None


# --------------------------------------------------------------------------
# checkpoint planning (reuses ops/checkpoints.py plan() with prop windows)
# --------------------------------------------------------------------------

def plan_prop_checkpoints(games, now, checkpoint_config):
    """Reuse checkpoints.plan() with the prop window config.

    The module reads WINDOWS/WINDOW_MINUTES/OPENER_MAX_MINUTES at call time,
    so the prop config is applied by setting module attributes before the
    call and restoring afterwards. WINDOW_MINUTES is already 90 (the
    wide-window fix landed for the featured collector and applies
    identically here). OPENER is disabled for props: the probe showed props
    are rarely posted before T-24, and there is no opener checkpoint in v1.
    """
    saved = (checkpoints.WINDOWS, checkpoints.WINDOW_MINUTES,
             checkpoints.OPENER_MAX_MINUTES)
    try:
        checkpoints.WINDOWS = tuple(checkpoint_config)
        checkpoints.WINDOW_MINUTES = 90
        checkpoints.OPENER_MAX_MINUTES = 1440  # suppresses the opener branch
        return checkpoints.plan(games, now)
    finally:
        (checkpoints.WINDOWS, checkpoints.WINDOW_MINUTES,
         checkpoints.OPENER_MAX_MINUTES) = saved


# --------------------------------------------------------------------------
# provider calls (live only)
# --------------------------------------------------------------------------

def api_get(path, params, api_key):
    qs = urllib.parse.urlencode({**params, "apiKey": api_key})
    req = urllib.request.Request(f"{API}{path}?{qs}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), {"error": e.read().decode()[:500]}


def _int_header(headers, name):
    try:
        return int(float(headers.get(name))) if headers.get(name) is not None else None
    except (TypeError, ValueError):
        return None


def fetch_events_list(api_key):
    """ONE free events-list call per tick (design §2.3 step 1)."""
    return api_get(f"/v4/sports/{SPORT_KEY}/events", {}, api_key)


def fetch_event_odds(api_key, event_id, markets, bookmakers):
    params = {
        "regions": ",".join(REGIONS),
        "markets": ",".join(markets),
        "bookmakers": bookmakers,
        "oddsFormat": "american",
    }
    return api_get(f"/v4/sports/{SPORT_KEY}/events/{event_id}/odds", params, api_key)


# --------------------------------------------------------------------------
# MOS context (§5.5): references only, never copied data
# --------------------------------------------------------------------------

def load_stadium_mos():
    try:
        with open(STADIUM_MOS_PATH) as f:
            return json.load(f)
    except OSError:
        return {}


def build_context(game, bundle_lookup, conn=None, stadium_mos=None):
    """Snapshot-time context: mos_station + eligible MOS runtime reference.

    The eligible cycle is the mechanically-latest MOS runtime under the
    frozen H-N2 rule (runtime + 4h <= kickoff - 24h), read as a free join
    against nfl_mos_forecast_archive. Injury/lineup context is a v1
    non-goal (source + retrieval timestamp only, later).
    """
    stadium = stadium_of(game, bundle_lookup)
    entry = (stadium_mos or {}).get(stadium or "", {})
    ctx = {"mos_station": entry.get("mos_station")}
    if conn is not None and ctx["mos_station"]:
        cutoff = game["kickoff"] - dt.timedelta(hours=24)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(runtime) FROM public.nfl_mos_forecast_archive "
                "WHERE mos_station = %s AND runtime + INTERVAL '4 hours' <= %s",
                (ctx["mos_station"], cutoff),
            )
            row = cur.fetchone()
        ctx["mos_eligible_runtime"] = row[0].isoformat() if row and row[0] else None
        ctx["mos_rule"] = "frozen H-N2: runtime + 4h <= kickoff - 24h"
    return ctx


# --------------------------------------------------------------------------
# snapshot / quote parsing
# --------------------------------------------------------------------------

def snapshot_key_for(canonical_game_id, checkpoint_name, provider_event_id, captured_at):
    identity = "|".join([
        canonical_game_id, checkpoint_name,
        provider_event_id or "", captured_at.isoformat(),
    ])
    return hashlib.sha256(identity.encode()).hexdigest()


def parse_odds_response(snapshot_key, odds_body, captured_at):
    """Parse one /odds response into raw quote rows (exact table schema).

    Stores ALL participants verbatim — never filters to a 'designated'
    player (design §3.4). Designation rules are analysis design frozen in
    a future prereg.
    """
    quotes = []
    bookmakers = odds_body.get("bookmakers", []) if isinstance(odds_body, dict) else []
    for book in bookmakers:
        book_key = book.get("key")
        for market in book.get("markets", []):
            mkey = market.get("key")
            if mkey not in PROP_MARKETS:
                continue  # never silently widen the v1 market set
            for outcome in market.get("outcomes", []):
                name = str(outcome.get("name", "")).lower()
                if name not in ("over", "under"):
                    continue
                point = outcome.get("point")
                price = outcome.get("price")
                if point is None or price is None:
                    continue
                quotes.append({
                    "snapshot_key": snapshot_key,
                    "book": book_key,
                    "market": mkey,
                    "participant_raw": outcome.get("description"),
                    "participant_key": None,  # NULL until alias-resolved (§5.6)
                    "side": name,
                    "line": point,
                    "price_american": price,
                    "captured_at": captured_at.isoformat(),
                })
    return quotes


# --------------------------------------------------------------------------
# per-tick resolution + projection + cap check
# --------------------------------------------------------------------------

def resolve_due(due_checkpoints, results):
    """Map each due (game, checkpoint) to a tick-fresh provider event id.

    Returns (resolutions, attempts): resolutions maps checkpoint key ->
    {canonical_game_id, provider_event_id, match_type, status}; attempts
    are the pre-cap attempt records (ok / no_event_id / ambiguous_match).
    """
    by_canonical = nfl_prop_identity.match_by_canonical(results)
    ambiguous_ids = set()
    for a in results.get("ambiguous", []):
        for c in a.get("candidates", []):
            ambiguous_ids.add(c.get("nflverse_game_id"))

    resolutions = {}
    attempts = []
    for cp in due_checkpoints:
        gid = cp.game_id
        matches = by_canonical.get(gid, [])
        if len(matches) == 1 and gid not in ambiguous_ids:
            m = matches[0]
            resolutions[cp.key] = {
                "canonical_game_id": gid,
                "provider_event_id": m["provider_event_id"],
                "match_type": m["match_type"],
                "status": ST_OK,
            }
            attempts.append({
                "canonical_game_id": gid, "checkpoint_name": cp.name,
                "provider_event_id_resolved": m["provider_event_id"],
                "status": ST_OK,
            })
        elif gid in ambiguous_ids or len(matches) > 1:
            # Ambiguous: recorded, never guessed.
            resolutions[cp.key] = {
                "canonical_game_id": gid, "provider_event_id": None,
                "match_type": None, "status": ST_AMBIGUOUS,
            }
            attempts.append({
                "canonical_game_id": gid, "checkpoint_name": cp.name,
                "provider_event_id_resolved": None, "status": ST_AMBIGUOUS,
            })
        else:
            # Game not listed (or no match): zero paid calls, never fall
            # back to a previously resolved ID.
            resolutions[cp.key] = {
                "canonical_game_id": gid, "provider_event_id": None,
                "match_type": None, "status": ST_NO_EVENT,
            }
            attempts.append({
                "canonical_game_id": gid, "checkpoint_name": cp.name,
                "provider_event_id_resolved": None, "status": ST_NO_EVENT,
            })
    return resolutions, attempts


def project_cost(n_paid_calls, empirical_per_call=None):
    """Per-run projection: paid calls x planned per-call cost.

    Before calibration, the planned number is A1 nominal x the A5 2x
    margin (6/call for the 3 v1 markets). After calibration, the
    empirical cost-model value replaces the margin (design §4.5, §7).
    """
    per_call = (empirical_per_call if empirical_per_call is not None
                else planned_per_call())
    return {
        "n_paid_calls": n_paid_calls,
        "per_call_nominal": nominal_per_call(),
        "per_call_planned": planned_per_call(),
        "per_call_used": per_call,
        "per_call_source": ("empirical" if empirical_per_call is not None
                            else "A1_nominal_x2_margin"),
        "projected_credits": n_paid_calls * per_call,
    }


def apply_cap_check(attempts, projection, cap):
    """Ceiling-abort: if the projection exceeds the cap, the run aborts
    with ZERO paid calls (the nfl-hd-pull.yml pattern, per run)."""
    would_abort = projection["projected_credits"] > cap
    if would_abort:
        for a in attempts:
            if a["status"] == ST_OK:
                a["status"] = ST_ABORTED_CAP
                a["provider_event_id_resolved"] = None
    return would_abort


def already_captured(conn, season):
    """(canonical_game_id, checkpoint_name) pairs already snapshotted."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT canonical_game_id, checkpoint_name "
            "FROM public.nfl_prop_snapshots WHERE season = %s",
            (season,))
        return set(cur.fetchall())


# --------------------------------------------------------------------------
# quota logging (extends ops/quota_log.py)
# --------------------------------------------------------------------------

def log_prop_call(conn, run_id, endpoint, n_markets, quota_before,
                  quota_after, billed_override=None):
    """Log one provider call. Free calls are logged at 0 so a future
    audit can see the full request pattern."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import quota_log  # noqa: E402
    quota_log.log_quota(
        "nfl", endpoint,
        quota_before, quota_after, n_events=0,
        endpoint=endpoint, n_markets=n_markets,
        billed_credits=billed_override, run_id=run_id)


# --------------------------------------------------------------------------
# dry-run artifact
# --------------------------------------------------------------------------

def dry_run(now, games, checkpoint_config, events_payload, simulate_odds,
            cap, stadium_mos, bundle_lookup):
    """Full cycle, zero /odds calls, zero prod-table writes.

    The JSON artifact is the record of the run (design §4.4).
    """
    checkpoints_planned = plan_prop_checkpoints(games, now, checkpoint_config)
    due = [c for c in checkpoints_planned if c.state == "due"]
    missed = [c for c in checkpoints_planned if c.state == "missed"]
    games_by_id = {g["game_id"]: g for g in games}

    seasons = sorted({g["season"] for g in games if g.get("season")})
    nv_games = [
        {"game_id": g["game_id"], "gameday": g["kickoff"].strftime("%Y-%m-%d"),
         "home_team": g["home_team"], "away_team": g["away_team"],
         "season": g["season"], "week": g.get("week")}
        for g in games
    ]
    results = nfl_prop_identity.resolve_tick(
        events_payload, nv_games, seasons)
    resolutions, attempts = resolve_due(due, results)

    n_paid = sum(1 for r in resolutions.values() if r["status"] == ST_OK)
    projection = project_cost(n_paid)
    would_abort = apply_cap_check(attempts, projection, cap)

    # Raw rows that WOULD be written, from the mock /odds responses only.
    # Validates the exact schema path without any HTTP call.
    raw_snapshots = []
    raw_quotes = []
    if not would_abort and simulate_odds:
        for cp in due:
            r = resolutions[cp.key]
            if r["status"] != ST_OK:
                continue
            body = simulate_odds.get(r["provider_event_id"])
            if body is None:
                continue
            captured_at = now
            skey = snapshot_key_for(
                r["canonical_game_id"], cp.name,
                r["provider_event_id"], captured_at)
            game = games_by_id[r["canonical_game_id"]]
            raw_snapshots.append({
                "snapshot_key": skey,
                "canonical_game_id": r["canonical_game_id"],
                "provider_event_id_at_snapshot": r["provider_event_id"],
                "match_type": r["match_type"],
                "resolved_at": now.isoformat(),
                "season": game["season"],
                "week": game.get("week"),
                "checkpoint_name": cp.name,
                "checkpoint_target_at": cp.target.isoformat(),
                "kickoff": cp.kickoff.isoformat(),
                "captured_at": captured_at.isoformat(),
                "credits_consumed": nominal_per_call(),
                "quota_before": None,
                "quota_after": None,
                "context": build_context(game, bundle_lookup,
                                         stadium_mos=stadium_mos),
            })
            raw_quotes.extend(parse_odds_response(skey, body, captured_at))

    counts = {
        "due": len(due),
        "captured": sum(1 for a in attempts if a["status"] == ST_OK),
        "missed": len(missed),
        "empty": sum(1 for a in attempts if a["status"] == ST_EMPTY),
    }
    artifact = {
        "mode": "dry_run",
        "design_doc": "nfl-edge/docs/nfl-prospective-prop-layer.md",
        "identity_rule": ("provider event IDs are snapshot-scoped; "
                          "resolved fresh this tick, never reused"),
        "tick_at": now.isoformat(),
        "config": {
            "checkpoints": checkpoint_config,
            "v1_markets": PROP_MARKETS,
            "bookmakers": PROP_BOOKMAKERS,
            "regions": REGIONS,
            "run_credit_cap": cap,
            "planning_margin": PLANNING_MARGIN,
        },
        "games_enumerated": len(games),
        "checkpoints_planned": len(checkpoints_planned),
        "checkpoints_due": [
            {"key": c.key, "game_id": c.game_id, "name": c.name,
             "target": c.target.isoformat(), "deadline": c.deadline.isoformat()}
            for c in due
        ],
        "checkpoints_missed": [
            {"game_id": c.game_id, "name": c.name,
             "target": c.target.isoformat()}
            for c in missed
        ],
        "events_list_rows": len(events_payload),
        "id_resolution": {
            "matched": len(results["matched"]),
            "ambiguous": len(results["ambiguous"]),
            "unmatched_odds": len(results["unmatched_odds"]),
        },
        "resolved_pairs": [
            {"canonical_game_id": r["canonical_game_id"],
             "provider_event_id": r["provider_event_id"],
             "match_type": r["match_type"], "status": r["status"],
             "checkpoint": next(c.name for c in due if c.key == key)}
            for key, r in resolutions.items()
        ],
        "attempts": attempts,
        "projection": projection,
        "cap_check": {"cap": cap, "would_abort": would_abort},
        "paid_calls_made": 0,
        "prod_table_writes": 0,
        "raw_snapshots_sample": raw_snapshots,
        "raw_quotes_sample": raw_quotes,
        "heartbeat_payload": {
            "due": counts["due"], "captured": counts["captured"],
            "missed": counts["missed"], "empty": counts["empty"],
            "credits_used": 0, "run_id": "dry_run",
            "dry_run": True, "credits_remaining": None,
            "trigger_source": trigger_source,
        },
        "completeness_note": ("dry-run makes zero DB writes; attempts rows "
                              "are NOT inserted (they would pollute "
                              "completeness accounting). The artifact is the "
                              "record."),
    }
    return artifact


# --------------------------------------------------------------------------
# live mode (only with NFL_PROP_LIVE_OK=1; never exercised in the build phase)
# --------------------------------------------------------------------------

def empirical_per_call(conn):
    """Rolling empirical billed credits per odds call (design §7).

    Returns None before calibration — the caller then uses the
    A1-nominal-x2-margin planning number (design §4.5).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT p50_billed FROM public.nfl_prop_cost_model "
            "WHERE endpoint = 'nfl_prop_odds_call' LIMIT 1")
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def insert_attempt(conn, run_id, attempt, credits=0, quota_before=None,
                   quota_after=None, detail=None):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO public.nfl_prop_collection_attempts
               (run_id, canonical_game_id, checkpoint_name,
                provider_event_id_resolved, status, credits_consumed,
                quota_before, quota_after, detail)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (run_id, attempt["canonical_game_id"], attempt["checkpoint_name"],
             attempt["provider_event_id_resolved"], attempt["status"],
             credits, quota_before, quota_after, detail))
    conn.commit()


def live_run(now, api_key, conn, games, checkpoint_config, cap, stadium_mos,
             bundle_lookup, run_id, trigger_source="unknown"):
    """Live collection. Refused unless NFL_PROP_LIVE_OK=1 (checked in main)."""
    import heartbeat  # noqa: E402

    season = next((g["season"] for g in games if g.get("season")), None)
    captured = already_captured(conn, season) if season else set()

    planned = plan_prop_checkpoints(games, now, checkpoint_config)
    due = [c for c in planned
           if c.state == "due" and (c.game_id, c.name) not in captured]

    # Step 1: ONE free events-list call per tick (bulk discipline).
    st, hd, body = fetch_events_list(api_key)
    log_prop_call(conn, run_id, "nfl_prop_events_list", 0,
                  {"used": None,
                   "remaining": _int_header(hd, "x-requests-remaining")},
                  {"used": _int_header(hd, "x-requests-used"),
                   "remaining": _int_header(hd, "x-requests-remaining")},
                  billed_override=0)  # free by A4; logged at 0 for the audit trail
    if st != 200:
        raise RuntimeError(f"events list failed with HTTP {st}")
    events_payload = body.get("data", body) if isinstance(body, dict) else body

    seasons = sorted({g["season"] for g in games if g.get("season")})
    nv_games = [
        {"game_id": g["game_id"], "gameday": g["kickoff"].strftime("%Y-%m-%d"),
         "home_team": g["home_team"], "away_team": g["away_team"],
         "season": g["season"], "week": g.get("week")}
        for g in games
    ]
    results = nfl_prop_identity.resolve_tick(events_payload, nv_games, seasons)
    resolutions, attempts = resolve_due(due, results)

    projection = project_cost(
        sum(1 for r in resolutions.values() if r["status"] == ST_OK),
        empirical_per_call(conn))
    would_abort = apply_cap_check(attempts, projection, cap)

    credits_used = 0
    counts = {"captured": 0, "missed": 0, "empty": 0}
    credits_remaining = None
    # Per-call billed cost = delta of x-requests-used between consecutive
    # responses (the events-list response seeds the baseline).
    prev_used = _int_header(hd, "x-requests-used")

    # Settle the read-only planning transaction before any network I/O.
    # psycopg opens an implicit transaction on the planning SELECTs above
    # (already_captured, empirical_per_call); each fetch_event_odds call
    # below takes minutes, and Neon terminates connections that sit
    # idle-in-transaction (observed 2026-09-16: two runs died in
    # build_context with IdleInTransactionSessionTimeout). Committing here
    # is a semantic no-op for the reads and keeps the connection alive.
    conn.commit()

    for cp in due:
        r = resolutions[cp.key]
        if r["status"] != ST_OK:
            insert_attempt(conn, run_id, next(
                a for a in attempts
                if a["canonical_game_id"] == r["canonical_game_id"]
                and a["checkpoint_name"] == cp.name))
            continue
        if would_abort:
            a = next(a for a in attempts
                     if a["canonical_game_id"] == r["canonical_game_id"]
                     and a["checkpoint_name"] == cp.name)
            insert_attempt(conn, run_id, a)
            continue

        game = next(g for g in games if g["game_id"] == r["canonical_game_id"])
        captured_at = dt.datetime.now(UTC)
        skey = snapshot_key_for(r["canonical_game_id"], cp.name,
                                r["provider_event_id"], captured_at)

        # Paid call, max 1 retry on request failure only (Saturday timing
        # lesson); the retry writes under the same tick timestamp.
        last_exc = None
        resp = None
        for attempt_no in range(2):
            try:
                resp = fetch_event_odds(api_key, r["provider_event_id"],
                                        PROP_MARKETS, PROP_BOOKMAKERS)
                break
            except (urllib.error.URLError, TimeoutError) as e:
                last_exc = e
                print(f"tick {now.isoformat()} retry {attempt_no+1}/1 "
                      f"after request failure: {e}", file=sys.stderr)
        if resp is None:
            insert_attempt(conn, run_id, {
                "canonical_game_id": r["canonical_game_id"],
                "checkpoint_name": cp.name,
                "provider_event_id_resolved": r["provider_event_id"],
                "status": ST_FAILED}, detail=f"request failed: {last_exc}")
            counts["missed"] += 1
            continue

        st, hd, body = resp
        # Per-call billed credits: delta of x-requests-used vs the previous
        # response (the H-D lesson — billed, never assumed).
        before = {"used": prev_used,
                  "remaining": None}
        after = {"used": _int_header(hd, "x-requests-used"),
                 "remaining": _int_header(hd, "x-requests-remaining")}
        if after["used"] is not None:
            prev_used = after["used"]
        spent = ((after["used"] or 0) - (before["used"] or 0)) if (
            after["used"] is not None and before["used"] is not None) else None
        if st != 200:
            insert_attempt(conn, run_id, {
                "canonical_game_id": r["canonical_game_id"],
                "checkpoint_name": cp.name,
                "provider_event_id_resolved": r["provider_event_id"],
                "status": ST_FAILED}, detail=f"HTTP {st}")
            counts["missed"] += 1
            continue

        quotes = parse_odds_response(skey, body, captured_at)
        markets_present = sorted({q["market"] for q in quotes})
        log_prop_call(conn, run_id, "nfl_prop_odds_call", len(markets_present),
                      before, after, billed_override=spent)
        credits_remaining = after["remaining"]

        if not quotes:
            # Empty-data responses cost 0 (A3) — recorded, never inferred.
            insert_attempt(conn, run_id, {
                "canonical_game_id": r["canonical_game_id"],
                "checkpoint_name": cp.name,
                "provider_event_id_resolved": r["provider_event_id"],
                "status": ST_EMPTY}, credits=0,
                quota_before=before["used"], quota_after=after["used"],
                detail="no v1 markets in response")
            counts["empty"] += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO public.nfl_prop_snapshots
                   (snapshot_key, canonical_game_id,
                    provider_event_id_at_snapshot, match_type, resolved_at,
                    season, week, checkpoint_name, checkpoint_target_at,
                    kickoff, captured_at, credits_consumed, quota_before,
                    quota_after, context)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (skey, r["canonical_game_id"], r["provider_event_id"],
                 r["match_type"], now, game["season"], game.get("week"),
                 cp.name, cp.target, cp.kickoff, captured_at,
                 spent or 0, before["used"], after["used"],
                 json.dumps(build_context(game, bundle_lookup, conn,
                                          stadium_mos))))
            cur.executemany(
                """INSERT INTO public.nfl_prop_quotes
                   (snapshot_key, book, market, participant_raw,
                    participant_key, side, line, price_american, captured_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                [(q["snapshot_key"], q["book"], q["market"],
                  q["participant_raw"], q["participant_key"], q["side"],
                  q["line"], q["price_american"], q["captured_at"])
                 for q in quotes])
        conn.commit()
        credits_used += spent or 0
        insert_attempt(conn, run_id, {
            "canonical_game_id": r["canonical_game_id"],
            "checkpoint_name": cp.name,
            "provider_event_id_resolved": r["provider_event_id"],
            "status": ST_OK}, credits=spent or 0,
            quota_before=before["used"], quota_after=after["used"],
            detail=f"markets: {','.join(markets_present)}")
        counts["captured"] += 1

    detail = {"due": len(due), "captured": counts["captured"],
              "missed": counts["missed"], "empty": counts["empty"],
              "credits_used": credits_used, "run_id": run_id,
              "dry_run": False, "credits_remaining": credits_remaining,
              "malformed_rows_skipped": results.get("malformed_rows_skipped", 0),
              "trigger_source": trigger_source}
    heartbeat.record_heartbeat(conn, "prop_collector", detail)
    print(json.dumps({"run_id": run_id, "detail": detail}))
    return 0


# --------------------------------------------------------------------------
# entrypoint
# --------------------------------------------------------------------------

def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="NFL prospective prop collector. --dry-run (default): "
                    "full cycle, zero /odds calls, zero prod-table writes, "
                    "JSON artifact output. --live: only with NFL_PROP_LIVE_OK=1.")
    ap.add_argument("--live", action="store_true",
                    help="Live mode. REFUSES unless NFL_PROP_LIVE_OK=1.")
    ap.add_argument("--out", default="/tmp/nfl_prop_dryrun.json",
                    help="Dry-run artifact path.")
    ap.add_argument("--fixture-schedule", default=None,
                    help="Dry-run: schedule JSON (defaults to bundled 2026).")
    ap.add_argument("--fixture-events", default=None,
                    help="Dry-run: recorded live events-list payload JSON. "
                         "REQUIRED for dry-run (the free live endpoint is "
                         "never probed without the user-authorized "
                         "calibration gate).")
    ap.add_argument("--fixture-now", default=None,
                    help="Dry-run: pin the tick time (ISO-8601). Default: now UTC.")
    ap.add_argument("--simulate-odds", default=None,
                    help="Dry-run: JSON mapping provider_event_id -> mock "
                         "/odds response body, used to build sample raw "
                         "quote rows with the exact schema. No HTTP calls.")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--trigger-source", default=None,
                    help="How this run was triggered: schedule | watchdog | "
                         "close-watch | manual. Defaults to "
                         "NFL_PROP_TRIGGER_SOURCE env, else 'unknown'. "
                         "Recorded in the heartbeat for audit.")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    checkpoint_config = load_checkpoints_config()
    cap = credit_cap()
    stadium_mos = load_stadium_mos()
    run_id = args.run_id or (
        "prop-" + dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    trigger_source = (
        args.trigger_source or os.environ.get("NFL_PROP_TRIGGER_SOURCE")
        or "unknown")

    if args.live:
        # Hard gate: live mode refuses without NFL_PROP_LIVE_OK=1.
        if os.environ.get("NFL_PROP_LIVE_OK") != "1":
            print("REFUSING live mode: NFL_PROP_LIVE_OK is not '1'. "
                  "Build phase is dry-run only.", file=sys.stderr)
            return 2
        import psycopg
        dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
        if not dsn:
            print("NFL_EDGE_DATABASE_URL is required for live mode",
                  file=sys.stderr)
            return 2
        api_key = os.environ.get("THE_ODDS_API_KEY")
        if not api_key:
            print("THE_ODDS_API_KEY is required for live mode",
                  file=sys.stderr)
            return 2
        now = dt.datetime.now(UTC)
        with psycopg.connect(dsn) as conn:
            games = load_games_live(conn)
            bundle_lookup = None
            if os.path.exists(BUNDLED_SCHEDULE):
                with open(BUNDLED_SCHEDULE) as f:
                    payload = json.load(f)
                rows = payload["games"] if isinstance(payload, dict) else payload
                bundle_lookup = {g["game_id"]: g for g in rows}
            return live_run(now, api_key, conn, games, checkpoint_config,
                            cap, stadium_mos, bundle_lookup, run_id,
                            trigger_source)

    # ---------------- dry-run ----------------
    if not args.fixture_events:
        print("--fixture-events is required for dry-run (recorded live "
              "events-list payload). The free live endpoint is never probed "
              "without the user-authorized calibration gate.",
              file=sys.stderr)
        return 2
    schedule_path = args.fixture_schedule or BUNDLED_SCHEDULE
    games = load_games_dry_run(schedule_path)
    bundle_lookup = {g["game_id"]: g for g in games}
    with open(args.fixture_events) as f:
        events_payload = json.load(f)
    if isinstance(events_payload, dict):
        events_payload = events_payload.get("data", events_payload)
    simulate_odds = None
    if args.simulate_odds:
        with open(args.simulate_odds) as f:
            simulate_odds = json.load(f)
    if args.fixture_now:
        now = dt.datetime.fromisoformat(
            args.fixture_now.replace("Z", "+00:00"))
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
    else:
        now = dt.datetime.now(UTC)

    artifact = dry_run(now, games, checkpoint_config, events_payload,
                       simulate_odds, cap, stadium_mos, bundle_lookup)
    artifact["run_id"] = run_id
    with open(args.out, "w") as f:
        json.dump(artifact, f, indent=2)
    summary = {
        "mode": "dry_run",
        "games": artifact["games_enumerated"],
        "due": artifact["heartbeat_payload"]["due"],
        "resolved": artifact["id_resolution"]["matched"],
        "projected_credits": artifact["projection"]["projected_credits"],
        "cap": cap,
        "would_abort": artifact["cap_check"]["would_abort"],
        "paid_calls_made": 0,
        "prod_table_writes": 0,
        "out": args.out,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
