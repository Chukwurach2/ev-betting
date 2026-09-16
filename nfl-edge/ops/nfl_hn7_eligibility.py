#!/usr/bin/env python3
"""H-N7 eligible-N gate (DRAFT prereg
`docs/preregistrations/nfl-h-n7-special-teams-totals-draft.md`).

Computes the exact eligible sample size for the H-N7 preregistration
(lagged special-teams efficiency -> NFL totals) MECHANICALLY, with zero
outcome inspection and zero Odds API credits. READ-ONLY on the database
(SELECTs only); free nflverse HTTP for play-by-play (2021-2024, REG only).

Eligibility funnel (each step is a hard filter; attrition reported):
  1. NFL regular season 2022-2024 game in the bundled nflverse schedules.
  2. Canonical identity match -> >=1 Odds event id (frozen dataset).
  3. Week >= 2 (mechanical: no point-in-time-safe kicker baseline in wk1).
  4. Decision consensus: Wed 12:00 UTC slot of the game's week, totals,
     >=3 books incl. Pinnacle at the consensus line (frozen construction).
     No fallbacks.
  5. Closing consensus: latest cadence slot strictly before kickoff,
     same construction. No fallbacks.
  6. Trailing ST data: both teams have 8 prior REG games (spanning into
     2021), >=4 with >=1 ST play.
  7. Kicker determinable for both teams: unambiguous (>=75% of team
     FG+XP attempts in the window), no in-window kicker change, primary
     kicker attempted >=1 kick in the team's most recent trailing game.

OUTCOME-BLINDNESS: the script reads only non-score pbp columns. The
FORBIDDEN set below is asserted empty against the columns actually read;
any outcome read aborts the run. Output: JSON funnel + per-game CSV (NO
outcomes, NO residuals, NO model fits). The script STOPS at eligibility;
it never runs the test.

Feasibility verdict (frozen in the draft prereg):
  - INFEASIBLE if eligible N < 200.
  - RETIRE-ON-POWER if MDE > 0.50 pts/ST-pt at exact N, where
      MDE = 2.4865 * 13.5 / (1.60 * sqrt(N))   (one-sided alpha=0.05,
      power=0.8; sigma_res=13.5 pts planning constant; sd_F=1.60 pts
      frozen planning assumption from the outcome-blind feature summary).
  - else PROCEED-TO-FREEZE (still requires user approval to freeze).
  The empirical sd_F is reported as an outcome-blind sensitivity only;
  >25% discordance vs 1.60 is flagged for pre-freeze DRAFT amendment,
  never silently substituted.
"""

import argparse
import csv
import io
import json
import math
import statistics
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import sports  # noqa: E402
import nfl_canonical_identity as nci  # noqa: E402
import nfl_hn6_coverage_gate as gate  # noqa: E402 (build_consensus, slot machinery)
import mos_forecast_archive as mos  # noqa: E402 (parse_kickoff only; no MOS use)

# ---- frozen constants -------------------------------------------------------
MANIFEST_V1_FINGERPRINT = "0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920"
FROZEN_QUOTE_COUNT = 291586
FROZEN_N_SNAPSHOTS = 162
SEASONS = (2022, 2023, 2024)
TRAIL_SEASONS = (2021, 2022, 2023, 2024)  # 2021 supplies the trailing window

KAPPA = 0.06                # frozen: points per yard of field position
SHRINK_K = 4                # pseudo-games toward 0
TRAIL_N = 8                 # trailing REG games per team
KICKER_MIN_SHARE = 0.75
MIN_ST_GAMES = 4            # of the 8 trailing games with >=1 ST play

N_FLOOR = 200               # eligible N below this -> INFEASIBLE
MDE_Z_SUM = 2.4865          # z_0.95 + z_0.80, one-sided alpha=0.05, power=0.8
MDE_SIGMA_RES = 13.5        # planning constant: residual SD (pts)
MDE_SD_F = 1.60             # frozen planning assumption: feature SD (pts)
MDE_CAP = 0.50              # MDE above this (pts/ST-pt) -> RETIRE-ON-POWER
DISCORD_TOL = 0.25          # empirical sd_F discordance tolerance

NFLVERSE_SCHEDULES_URL = ("https://github.com/nflverse/nflverse-data/releases"
                          "/download/schedules/games.csv")
PBP_URL = ("https://github.com/nflverse/nflverse-data/releases/download/pbp/"
           "play_by_play_{season}.parquet")

ST_PLAY_TYPES = ("field_goal", "extra_point", "kickoff", "punt")

# pbp columns the gate is allowed to read. Outcome-blindness is enforced by
# an explicit blocklist of score/spread/total/model columns; anything not
# on this list but score-like fails closed via the 'vegas' substring rule.
# NOTE: field_goal_result / extra_point_result are play descriptors
# (made/missed/good), not game outcomes, and are intentionally allowed.
PBP_COLUMNS = ["game_id", "season", "week", "season_type", "posteam", "defteam",
               "play_type", "field_goal_result", "extra_point_result",
               "kicker_player_name", "touchback", "touchdown", "td_team",
               "safety", "yardline_100", "kick_distance", "return_yards",
               "punt_blocked", "kickoff_out_of_bounds"]
FORBIDDEN_COLUMNS = {
    "total_home_score", "total_away_score", "posteam_score", "defteam_score",
    "score_differential", "posteam_score_post", "defteam_score_post",
    "score_differential_post", "home_score", "away_score",
    "spread_line", "total_line", "result", "total",
    "no_score_prob", "opp_fg_prob", "opp_safety_prob", "opp_td_prob",
    "fg_prob", "safety_prob", "td_prob", "extra_point_prob",
    "two_point_conversion_prob",
    "ep", "epa", "wp", "def_wp", "home_wp", "away_wp", "wpa",
    "success",
}
FORBIDDEN_SUBSTRINGS = ("vegas",)


# ---- pure functions (unit-tested) -------------------------------------------

def tb_kick_spot(season):
    """Touchback spot on kickoffs: 25 pre-2024, 30 under the 2024 dynamic kickoff."""
    return 30 if season == 2024 else 25


def kickoff_start(play, season):
    """Opponent start (yards from own goal) after a kickoff, or None to skip.

    play: dict-like with yardline_100, kick_distance, return_yards,
    touchback, kickoff_out_of_bounds. Kicking-team frame: posteam on
    kickoffs is the RECEIVING team, so this is called for K = defteam.
    """
    if play.get("touchback") == 1:
        return tb_kick_spot(season)
    if play.get("kickoff_out_of_bounds") == 1:
        return 40
    L, kd = play.get("yardline_100"), play.get("kick_distance")
    if L is None or (isinstance(kd, float) and math.isnan(kd)) or kd is None:
        return None
    ry = play.get("return_yards") or 0
    return min(99, max(1, 100 - L - kd + ry))


def punt_start(play):
    """Opponent start (yards from own goal) after a punt, or None to skip.

    Punting-team frame: posteam on punts is the PUNTING team; yardline_100
    is the line of scrimmage measured from the receiving team's own goal.
    """
    if play.get("touchback") == 1:
        return 20
    L, kd = play.get("yardline_100"), play.get("kick_distance")
    if L is None or (isinstance(kd, float) and math.isnan(kd)) or kd is None:
        return None
    ry = play.get("return_yards") or 0
    return min(99, max(1, L - kd + ry))


def shrink_mean(mean, n=TRAIL_N, k=SHRINK_K):
    """Shrink a trailing mean toward 0 with k pseudo-games."""
    return (n * mean) / (n + k)


def mde_slope(n, z_sum=MDE_Z_SUM, sigma=MDE_SIGMA_RES, sd_f=MDE_SD_F):
    """Minimum detectable slope (pts per ST-pt), one-sided alpha=0.05, power=0.8."""
    return z_sum * sigma / (sd_f * math.sqrt(n))


def feasibility_verdict(n):
    """(verdict, mde) under the frozen draft rules."""
    if n < N_FLOOR:
        return "INFEASIBLE", None
    m = mde_slope(n)
    if m > MDE_CAP:
        return "RETIRE-ON-POWER", m
    return "PROCEED-TO-FREEZE", m


def kicker_status(attempts_by_kicker, last_game_attempts_by_kicker):
    """Kicker availability determination (frozen rule).

    attempts_by_kicker: {kicker: attempts} over the trailing window.
    last_game_attempts_by_kicker: {kicker: attempts} in the most recent
    trailing game. Returns (primary_kicker, status) where status is
    'ok' or an exclusion reason.
    """
    total = sum(attempts_by_kicker.values())
    if total == 0:
        return None, "kicker_no_attempts_in_window"
    primary = max(attempts_by_kicker, key=lambda k: attempts_by_kicker[k])
    if attempts_by_kicker[primary] / total < KICKER_MIN_SHARE:
        return None, "ambiguous_kicker"
    if not last_game_attempts_by_kicker:
        return None, "kicker_unknown_last_game"
    last_primary = max(last_game_attempts_by_kicker,
                       key=lambda k: last_game_attempts_by_kicker[k])
    if last_primary != primary:
        return None, "kicker_change_in_window"
    return primary, "ok"


# ---- I/O --------------------------------------------------------------------

def assert_outcome_blind(columns):
    bad = [c for c in columns
           if c.lower() in FORBIDDEN_COLUMNS
           or any(s in c.lower() for s in FORBIDDEN_SUBSTRINGS)]
    if bad:
        raise SystemExit(f"OUTCOME-BLINDNESS VIOLATION: refusing to read {bad}")


def http_get(url, timeout=120, desc=""):
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-hn7-elig"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_kickoffs(url=NFLVERSE_SCHEDULES_URL):
    """game_id -> kickoff_utc. Reads ONLY gameday/gametime/stadium.

    nflverse schedules carry scores; this loader never reads them.
    """
    raw = http_get(url, desc="schedules").decode("utf-8")
    out = {}
    for row in csv.DictReader(io.StringIO(raw)):
        if row.get("season") not in ("2022", "2023", "2024"):
            continue
        if row.get("game_type") != "REG":
            continue
        gid = row.get("game_id")
        try:
            ko = mos.parse_kickoff(row["gameday"], row["gametime"])
        except Exception:
            continue
        out[gid] = ko
    return out


def load_pbp(pbp_dir=None):
    """Load REG play-by-play 2021-2024, outcome-blind column subset."""
    import pandas as pd
    assert_outcome_blind(PBP_COLUMNS)
    frames = []
    for season in TRAIL_SEASONS:
        if pbp_dir:
            path = str(Path(pbp_dir) / f"play_by_play_{season}.parquet")
            df = pd.read_parquet(path, columns=PBP_COLUMNS)
        else:
            url = PBP_URL.format(season=season)
            raw = http_get(url, timeout=300, desc=f"pbp {season}")
            df = pd.read_parquet(io.BytesIO(raw), columns=PBP_COLUMNS)
        df = df[df["season_type"] == "REG"].copy()
        frames.append(df)
    pbp = pd.concat(frames, ignore_index=True)
    return pbp


def team_game_st_values(pbp):
    """Per (game_id, season, team): ST component aggregates.

    Returns dict game_id -> {team: dict(fg, xp, retd, kfp_n, kfp_sum,
    pfp_n, pfp_sum, st_plays, kickers, last_game kickers...)}.
    Kicker attempt tracking is returned separately per team-game.
    """
    import pandas as pd
    vals = {}
    kickers = {}  # (game_id, team) -> {kicker: fg+xp attempts}
    for (gid, season), g in pbp.groupby(["game_id", "season"]):
        teams = pd.unique(g[["posteam", "defteam"]].values.ravel())
        teams = [t for t in teams if isinstance(t, str)]
        v = {t: {"fg": 0.0, "xp": 0.0, "retd": 0.0, "kfp_n": 0, "kfp_sum": 0.0,
                 "pfp_n": 0, "pfp_sum": 0.0, "st_plays": 0} for t in teams}
        kg = {}
        for row in g.itertuples():
            pt = row.play_type
            if pt not in ST_PLAY_TYPES:
                continue
            d = row._asdict()
            if pt == "field_goal":
                A = d["posteam"]
                if not isinstance(A, str):
                    continue
                v[A]["st_plays"] += 1
                k = d["kicker_player_name"]
                if isinstance(k, str):
                    kg.setdefault(A, {}).setdefault(k, 0)
                    kg[A][k] += 1
                if d["touchdown"] == 1 and isinstance(d["td_team"], str):
                    v[d["td_team"]]["retd"] += 6.0
                    continue
                if d["field_goal_result"] == "made":
                    v[A]["fg"] += 3.0
            elif pt == "extra_point":
                A = d["posteam"]
                if not isinstance(A, str):
                    continue
                v[A]["st_plays"] += 1
                k = d["kicker_player_name"]
                if isinstance(k, str):
                    kg.setdefault(A, {}).setdefault(k, 0)
                    kg[A][k] += 1
                if d["extra_point_result"] == "good":
                    v[A]["xp"] += 1.0
            elif pt == "kickoff":
                K, R = d["defteam"], d["posteam"]  # posteam = receiving team
                if not isinstance(K, str):
                    continue
                v[K]["st_plays"] += 1
                if d["touchdown"] == 1 and isinstance(d["td_team"], str):
                    v[d["td_team"]]["retd"] += 6.0
                    continue
                if d["safety"] == 1:  # K concedes: +2 to the kicking team
                    v[K]["retd"] += 2.0
                    continue
                s = kickoff_start(d, season)
                if s is None:
                    continue
                v[K]["kfp_n"] += 1
                v[K]["kfp_sum"] += s
            elif pt == "punt":
                K, R = d["posteam"], d["defteam"]  # posteam = punting team
                if not isinstance(K, str):
                    continue
                v[K]["st_plays"] += 1
                if d["touchdown"] == 1 and isinstance(d["td_team"], str):
                    v[d["td_team"]]["retd"] += 6.0
                    continue
                if d["safety"] == 1:
                    if isinstance(R, str):
                        v[R]["retd"] += 2.0
                    continue
                if d["punt_blocked"] == 1:
                    continue
                s = punt_start(d)
                if s is None:
                    continue
                v[K]["pfp_n"] += 1
                v[K]["pfp_sum"] += s
        vals[(gid, season)] = v
        for (team, kd) in kg.items():
            kickers[(gid, season, team)] = kd
    return vals, kickers


def cumulative_baselines(vals, game_order):
    """Point-in-time-safe league baselines.

    For each target (season, week): E[fg], E[xp], E[retd] per team-game and
    E[kick start], E[punt start] per kick/punt, computed over REG games
    with (season == S, week < W); if fewer than 64 team-games, the full
    prior REG season (S-1) is used instead. Never includes the target
    week or later: no look-ahead.
    """
    import pandas as pd
    rows = []
    for (gid, season), teams in vals.items():
        so = game_order.get(gid)
        if so is None:
            continue
        _s, w = so
        for t, v in teams.items():
            rows.append((season, w, v["fg"], v["xp"], v["retd"],
                         v["kfp_n"], v["kfp_sum"], v["pfp_n"], v["pfp_sum"]))
    df = pd.DataFrame(rows, columns=["season", "week", "fg", "xp", "retd",
                                     "kfp_n", "kfp_sum", "pfp_n", "pfp_sum"])

    def pool_base(pool):
        n = len(pool)
        if n == 0:
            return None
        return {
            "fg": pool["fg"].mean(), "xp": pool["xp"].mean(),
            "retd": pool["retd"].mean(),
            "kick_start": (pool["kfp_sum"].sum() / pool["kfp_n"].sum()
                           if pool["kfp_n"].sum() > 0 else None),
            "punt_start": (pool["pfp_sum"].sum() / pool["pfp_n"].sum()
                           if pool["pfp_n"].sum() > 0 else None),
            "team_games": n,
        }

    full_season = {}
    for season, d in df.groupby("season"):
        full_season[season] = pool_base(d)

    out = {}
    for season in TRAIL_SEASONS:
        for week in range(1, 19):
            pool = df[(df["season"] == season) & (df["week"] < week)]
            if len(pool) >= 64:
                out[(season, week)] = pool_base(pool)
            else:
                out[(season, week)] = full_season.get(season - 1)
    return out


def v_of_team_game(v, base):
    """V(T,g): total-points-space ST value as deviation from baseline."""
    kfp = (-KAPPA * (v["kfp_sum"] - v["kfp_n"] * base["kick_start"])
           if v["kfp_n"] > 0 and base["kick_start"] is not None else 0.0)
    pfp = (-KAPPA * (v["pfp_sum"] - v["pfp_n"] * base["punt_start"])
           if v["pfp_n"] > 0 and base["punt_start"] is not None else 0.0)
    return ((v["fg"] - base["fg"]) + (v["xp"] - base["xp"])
            + (v["retd"] - base["retd"]) + kfp + pfp)


def trailing_features(vals, kickers, baselines, game_order):
    """Per (game_id, team): (Vbar shrunk trailing-8 mean, kicker status,
    n_trailing, n_st_games). game_order: game_id -> (season, week)."""
    import pandas as pd
    # team -> chronological list of (season, week, game_id)
    team_games = defaultdict(list)
    for (gid, season) in vals:
        so = game_order.get(gid)
        if so is None:
            continue
        s, w = so
        for t in vals[(gid, season)]:
            team_games[t].append((s, w, gid))
    for t in team_games:
        team_games[t].sort()
    out = {}
    for t, games in team_games.items():
        # baseline selector per target game computed lazily below
        vlist = []
        for (s, w, gid) in games:
            v = vals[(gid, s)][t]
            vlist.append((s, w, gid, v))
        for i, (s, w, gid) in enumerate(
                [(s, w, gid) for (s, w, gid, v) in vlist]):
            if i < TRAIL_N:
                continue
            window = vlist[i - TRAIL_N:i]
            base = baselines.get((s, w))
            if base is None:
                out[(gid, t)] = (None, "no_baseline", len(window), None, None)
                continue
            vs = [v_of_team_game(vv, base) for (ss, ww, gg, vv) in window]
            mean = sum(vs) / len(vs)
            vbar = shrink_mean(mean)
            st_games = sum(1 for (ss, ww, gg, vv) in window if vv["st_plays"] > 0)
            # kicker aggregation over window
            att = defaultdict(int)
            for (ss, ww, gg, vv) in window:
                for k, n in kickers.get((gg, ss, t), {}).items():
                    att[k] += n
            last_gid = window[-1][2]
            last_ss = window[-1][0]
            last_att = kickers.get((last_gid, last_ss, t), {})
            primary, status = kicker_status(dict(att), dict(last_att))
            out[(gid, t)] = (vbar, status, len(window), st_games,
                             primary)
    return out


def decision_slot_for_game(season, week, slots):
    """The Wed 12:00 UTC slot of the game's week."""
    for (s, w, slot, inst) in slots:
        if s == season and w == week and slot == "wed":
            return slot, inst
    return None, None


def closing_slot_for_game(season, week, kickoff, slots):
    """Latest cadence slot of the game's week strictly before kickoff."""
    cands = [(slot, inst) for (s, w, slot, inst) in slots
             if s == season and w == week and inst < kickoff]
    if not cands:
        return None, None
    return max(cands, key=lambda t: t[1])


def consensus_for(game_eids, s, w, slot, oa, quotes_by_slot):
    over = []
    if oa is not None:
        for eid in game_eids:
            for (book, sel, line) in quotes_by_slot.get((eid, s, w, slot), []):
                if sel == "Over":
                    over.append((book, line))
    return gate.build_consensus(over)


def run_eligibility(conn, pbp_dir=None):
    report = {
        "prereg": "docs/preregistrations/nfl-h-n7-special-teams-totals-draft.md",
        "status": "DRAFT -- not frozen",
        "manifest_v1_fingerprint": MANIFEST_V1_FINGERPRINT,
        "api_credits_used": 0,
        "db_writes": 0,
        "outcomes_inspected": False,
        "outcome_rows_read": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    funnel = {}
    per_game = []

    # ---- 0. freeze check ------------------------------------------------------
    hq_table = sports.historical_quotes_table("nfl")
    n_quotes = conn.execute(
        "SELECT COUNT(*) FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')" % hq_table
    ).fetchone()[0]
    n_snaps = conn.execute(
        "SELECT COUNT(DISTINCT observed_at) FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')" % hq_table
    ).fetchone()[0]
    freeze_ok = (n_quotes == FROZEN_QUOTE_COUNT and n_snaps == FROZEN_N_SNAPSHOTS)
    report["freeze_check"] = {"n_quotes": n_quotes, "n_snapshots": n_snaps,
                              "ok": freeze_ok}
    if not freeze_ok:
        report["gate"] = "ABORTED_FREEZE_MISMATCH"
        return report

    # ---- reference data -------------------------------------------------------
    bundle_games = [g for g in json.load(open(str(nci.NFLVERSE_PATH)))
                    if g.get("season") in SEASONS and g.get("game_type") == "REG"]
    funnel["bundle_reg_games"] = len(bundle_games)
    kickoffs = load_kickoffs()

    mh_table = sports.market_history_table("nfl")
    rows = conn.execute(gate.ODDS_EVENTS_SQL.format(mh=mh_table)).fetchall()
    odds_events = [{"event_id": eid, "home": home, "away": away,
                    "kickoff": kickoff}
                   for (eid, home, away, kickoff) in rows]
    results = nci.match_events(odds_events, bundle_games, nci.load_aliases(),
                               list(SEASONS))
    game_to_eids = defaultdict(list)
    for m in results["matched"]:
        game_to_eids[m["nflverse_game_id"]].append(m["provider_event_id"])
    funnel["identity_matched"] = len(game_to_eids)
    funnel["identity_unmatched"] = len(results["unmatched_nflverse"])
    funnel["identity_ambiguous"] = len(results["ambiguous"])

    # ---- slot machinery ---------------------------------------------------------
    slots = gate.snapshot_slots()
    actual_oas = [r[0] for r in conn.execute(
        "SELECT DISTINCT observed_at FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL') ORDER BY 1"
        % hq_table).fetchall()]
    slot_oa, unmatched_oas = gate.match_observed_to_slots(actual_oas, slots)
    oa_to_slot = {(s, w, slot): oa for (s, w, slot), oa in slot_oa.items()}
    slot_by_oa = {oa: key for key, oa in oa_to_slot.items()}

    all_eids = sorted({eid for eids in game_to_eids.values() for eid in eids})
    qrows = conn.execute(
        "SELECT provider_event_id, book_key, selection, line, observed_at "
        "FROM public.%s "
        "WHERE market = 'FULL_GAME_TOTAL' AND provider_event_id = ANY(%%s)"
        % hq_table, (all_eids,)).fetchall()
    quotes_by_slot = defaultdict(list)
    for eid, book, sel, line, oa in qrows:
        key = slot_by_oa.get(oa)
        if key is None:
            continue
        quotes_by_slot[(eid,) + key].append((book, sel, line))

    # ---- ST features (outcome-blind pbp) ---------------------------------------
    pbp = load_pbp(pbp_dir)
    report["pbp_rows_read"] = int(len(pbp))
    report["pbp_columns_read"] = list(PBP_COLUMNS)
    vals, kickers = team_game_st_values(pbp)
    # game chronological order for trailing windows
    game_order = {}
    for (gid, season) in vals:
        try:
            parts = gid.split("_")
            game_order[gid] = (int(parts[0]), int(parts[1]))
        except Exception:
            continue
    baselines = cumulative_baselines(vals, game_order)
    feat = trailing_features(vals, kickers, baselines, game_order)
    funnel["teams_with_trailing_features"] = len(
        {t for (gid, t) in feat})

    # ---- per-game funnel --------------------------------------------------------
    counts = defaultdict(int)
    features = []
    for g in bundle_games:
        gid = g["game_id"]
        s, w = g["season"], g["week"]
        home, away = g["home_team"], g["away_team"]
        rec = {"nflverse_game_id": gid, "season": s, "week": w,
               "home_team": home, "away_team": away,
               "status": None, "exclusion_reason": None}
        eids = game_to_eids.get(gid, [])
        if not eids:
            rec.update(status="excluded", exclusion_reason="no_identity")
            counts["no_identity"] += 1
            per_game.append(rec)
            continue
        if w < 2:
            rec.update(status="excluded", exclusion_reason="week_lt_2")
            counts["week_lt_2"] += 1
            per_game.append(rec)
            continue
        ko = kickoffs.get(gid)
        if ko is None:
            rec.update(status="excluded", exclusion_reason="no_kickoff")
            counts["no_kickoff"] += 1
            per_game.append(rec)
            continue
        rec["kickoff_utc"] = ko.isoformat()

        dslot, dinst = decision_slot_for_game(s, w, slots)
        doa = oa_to_slot.get((s, w, dslot)) if dslot else None
        dcons = consensus_for(eids, s, w, dslot, doa, quotes_by_slot)
        rec["decision_consensus"] = dcons["consensus_line"]
        rec["decision_books_at_line"] = dcons["n_books_at_line"]
        rec["decision_pinnacle"] = dcons["pinnacle_present"]
        if not dcons["eligible"]:
            rec.update(status="excluded",
                       exclusion_reason="no_decision_consensus")
            counts["no_decision_consensus"] += 1
            per_game.append(rec)
            continue

        cslot, cinst = closing_slot_for_game(s, w, ko, slots)
        coa = oa_to_slot.get((s, w, cslot)) if cslot else None
        ccons = consensus_for(eids, s, w, cslot, coa, quotes_by_slot)
        rec["closing_slot"] = cslot
        rec["closing_consensus"] = ccons["consensus_line"]
        rec["closing_books_at_line"] = ccons["n_books_at_line"]
        rec["closing_pinnacle"] = ccons["pinnacle_present"]
        if not ccons["eligible"]:
            rec.update(status="excluded",
                       exclusion_reason="no_closing_consensus")
            counts["no_closing_consensus"] += 1
            per_game.append(rec)
            continue

        fh = feat.get((gid, home))
        fa = feat.get((gid, away))
        rec["home_kicker_status"] = fh[1] if fh else "no_trailing_data"
        rec["away_kicker_status"] = fa[1] if fa else "no_trailing_data"
        ok = True
        for side, f in (("home", fh), ("away", fa)):
            if f is None or f[0] is None:
                rec.update(status="excluded",
                           exclusion_reason="insufficient_trailing:"
                           + (f[1] if f else "missing"))
                counts["insufficient_trailing"] += 1
                ok = False
                break
            if f[3] is not None and f[3] < MIN_ST_GAMES:
                rec.update(status="excluded",
                           exclusion_reason="few_st_games")
                counts["few_st_games"] += 1
                ok = False
                break
            if f[1] != "ok":
                rec.update(status="excluded",
                           exclusion_reason="kicker:" + f[1])
                counts["kicker:" + f[1]] += 1
                ok = False
                break
        if not ok:
            per_game.append(rec)
            continue

        F = fh[0] + fa[0]
        rec["Vbar_home"] = round(fh[0], 4)
        rec["Vbar_away"] = round(fa[0], 4)
        rec["F"] = round(F, 4)
        rec.update(status="eligible", exclusion_reason="")
        counts["eligible"] += 1
        features.append(F)
        per_game.append(rec)

    funnel["attrition"] = dict(counts)
    n = counts["eligible"]
    funnel["eligible_N"] = n

    # ---- feature summary (outcome-blind) ----------------------------------------
    import statistics as stats
    feat_summary = {}
    if features:
        feat_summary = {"n": n, "mean": round(stats.mean(features), 4),
                        "sd": round(stats.pstdev(features), 4),
                        "min": round(min(features), 4),
                        "max": round(max(features), 4)}
    funnel["feature_summary_F"] = feat_summary

    # ---- feasibility verdict (frozen rule) ---------------------------------------
    verdict, mde = feasibility_verdict(n)
    emp_sd = feat_summary.get("sd")
    discordant = (emp_sd is not None
                  and abs(emp_sd - MDE_SD_F) / MDE_SD_F > DISCORD_TOL)
    report["mde"] = {"n": n, "z_sum": MDE_Z_SUM, "sigma_res": MDE_SIGMA_RES,
                     "sd_F_planning": MDE_SD_F, "mde": mde, "mde_cap": MDE_CAP,
                     "empirical_sd_F": emp_sd,
                     "empirical_discordant": discordant}
    report["gate"] = ("DISCORDANT_FEATURE_SD" if verdict == "PROCEED-TO-FREEZE"
                      and discordant else verdict)
    report["funnel"] = funnel
    return report, per_game


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--per-game-csv", required=True)
    ap.add_argument("--pbp-dir", default=None)
    args = ap.parse_args()
    import psycopg, os
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NFL_EDGE_DATABASE_URL is required")
    with psycopg.connect(dsn) as conn:
        report, per_game = run_eligibility(conn, pbp_dir=args.pbp_dir)
    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    cols = ["nflverse_game_id", "season", "week", "home_team", "away_team",
            "kickoff_utc", "status", "exclusion_reason", "decision_consensus",
            "decision_books_at_line", "decision_pinnacle", "closing_slot",
            "closing_consensus", "closing_books_at_line", "closing_pinnacle",
            "Vbar_home", "Vbar_away", "F",
            "home_kicker_status", "away_kicker_status"]
    with open(args.per_game_csv, "w", newline="") as fh:
        wr = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        wr.writeheader()
        wr.writerows(per_game)
    print(json.dumps({"gate": report["gate"], "eligible_N": report["funnel"].get("eligible_N"),
                      "mde": report["mde"].get("mde"),
                      "outcome_rows_read": report["outcome_rows_read"],
                      "api_credits_used": 0, "db_writes": 0}, indent=2))


if __name__ == "__main__":
    main()
