#!/usr/bin/env python3
"""H-N8 eligible-N gate (DRAFT prereg
`docs/preregistrations/nfl-h-n8-redzone-conversion-totals-draft.md`).

Computes the exact eligible sample size for the H-N8 preregistration
(lagged red-zone/situational conversion -> NFL totals) MECHANICALLY, with
zero outcome inspection and zero Odds API credits. READ-ONLY on the
database (SELECTs only); free nflverse HTTP for play-by-play and schedule
kickoff times (score columns never read -- the pbp loader uses a strict
column allowlist that excludes every score column).

Eligibility funnel (each step is a hard filter; attrition reported):
  1. NFL regular season 2022-2024 game in the bundled nflverse schedules.
  2. Canonical identity match -> >=1 Odds event id (frozen dataset).
  3. Decision snapshot = the latest frozen totals snapshot with
     snapshot_instant < kickoff - 24h. Totals consensus at that snapshot:
     >=3 books incl. Pinnacle with Over quotes at the consensus line
     (median-of-medians, within 0.01) -- same construction as the frozen
     de-vig work. No fallbacks.
  4. Feature history: both teams have 8 prior REG games (2021-2024,
     (season, week) strictly before, postseason excluded) AND >=12
     offensive red-zone trips each inside the trailing-8 window.

Output: JSON funnel + per-game CSV (NO outcomes, NO residuals, NO model
fits). The script STOPS at eligibility; it never runs the test.

Feasibility verdict (frozen in the draft prereg):
  - INFEASIBLE if eligible N < 250.
  - RETIRE-ON-POWER if MDE > 10.0 pts/unit at exact N, where
      MDE = 2.4865 * 13.5 / (sd_x * sqrt(N))   (one-sided alpha=0.05,
      power=0.8; sigma_res=13.5 pts planning constant; sd_x = observed SD
      of the primary feature x = home-minus-away lagged offensive
      RZ-TD% differential -- a feature summary, not an outcome).
  - else PROCEED-TO-FREEZE (still requires user approval to freeze).
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
import mos_forecast_archive as mos  # noqa: E402 (parse_kickoff)

# ---- frozen constants -------------------------------------------------------
MANIFEST_V1_FINGERPRINT = "0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920"
FROZEN_QUOTE_COUNT = 291586
FROZEN_N_SNAPSHOTS = 162
SEASONS = (2022, 2023, 2024)
HISTORY_SEASONS = (2021, 2022, 2023, 2024)  # feature-history source only

CUTOFF_HOURS = 24               # prediction cutoff = kickoff - 24h
TRAILING_GAMES = 8              # trailing REG games per team
MIN_RZ_TRIPS = 12               # min offensive RZ trips per team in window

N_FLOOR = 250                   # eligible N below this -> INFEASIBLE
MDE_Z_SUM = 2.4865              # z_0.95 + z_0.80, one-sided alpha=0.05, power=0.8
MDE_SIGMA_RES = 13.5            # planning constant: residual SD (pts), literature
MDE_CAP = 10.0                  # MDE above this (pts/unit slope) -> RETIRE-ON-POWER

NFLVERSE_SCHEDULES_URL = ("https://github.com/nflverse/nflverse-data/releases"
                          "/download/schedules/games.csv")
PBP_URL = ("https://github.com/nflverse/nflverse-data/releases"
           "/download/pbp/play_by_play_{season}.parquet")

# Strict column allowlist: play-level feature inputs ONLY. Every score
# column (posteam_score*, defteam_score*, total_*, result, ...) is
# excluded by construction -- the gate cannot read game outcomes.
PBP_COLUMNS = ["game_id", "season", "week", "season_type", "posteam",
               "drive", "play_type", "yardline_100", "touchdown", "td_team",
               "goal_to_go", "down", "third_down_converted",
               "third_down_failed", "fourth_down_converted",
               "fourth_down_failed"]

COUNTED_PLAY_TYPES = {"pass", "run"}  # frozen: genuine offensive snaps only


# ---- pure functions (unit-tested) -------------------------------------------

def decision_slot_for_game(season, week, kickoff, slots):
    """Latest frozen cadence slot strictly before kickoff - 24h."""
    cutoff = kickoff - timedelta(hours=CUTOFF_HOURS)
    cands = [(slot, inst) for (s, w, slot, inst) in slots
             if s == season and w == week and inst < cutoff]
    if not cands:
        return None, None
    return max(cands, key=lambda t: t[1])


def mde_slope(n, sd_x, z_sum=MDE_Z_SUM, sigma=MDE_SIGMA_RES):
    """Minimum detectable slope (pts per 1.0 of x), one-sided a=0.05, p=0.8."""
    return z_sum * sigma / (sd_x * math.sqrt(n))


def feasibility_verdict(n, sd_x):
    """(verdict, mde) under the frozen draft rules."""
    if n < N_FLOOR:
        return "INFEASIBLE", None
    m = mde_slope(n, sd_x)
    if m > MDE_CAP:
        return "RETIRE-ON-POWER", m
    return "PROCEED-TO-FREEZE", m


def drive_aggregates(df):
    """Per (game_id, drive, posteam) situational aggregates.

    df: pandas DataFrame with exactly PBP_COLUMNS (no score columns).
    Returns dict keyed (game_id, drive, posteam) -> dict with:
      rz_trip (bool), rz_td (bool), gtg (bool), gtg_td (bool),
      third_att, third_conv, fourth_att, fourth_conv (ints).
    Only REG rows are considered by the caller.
    """
    out = {}
    for (gid, drive, team), g in df.groupby(["game_id", "drive", "posteam"],
                                            dropna=False):
        if team is None or (isinstance(team, float) and math.isnan(team)):
            continue
        gc = g[g["play_type"].isin(COUNTED_PLAY_TYPES)]  # genuine snaps only
        rz_trip = bool(((gc["yardline_100"] <= 20)).any())
        td_plays = g[(g["touchdown"] == 1) & (g["td_team"] == team)]
        gtg = bool(((gc["goal_to_go"] == 1)).any())
        t3 = g[(g["third_down_converted"] == 1) | (g["third_down_failed"] == 1)]
        t4 = g[(g["fourth_down_converted"] == 1) | (g["fourth_down_failed"] == 1)]
        out[(gid, drive, team)] = {
            "rz_trip": rz_trip,
            "rz_td": rz_trip and len(td_plays) > 0,
            "gtg": gtg,
            "gtg_td": gtg and len(td_plays) > 0,
            "third_att": int(len(t3)),
            "third_conv": int((t3["third_down_converted"] == 1).sum()),
            "fourth_att": int(len(t4)),
            "fourth_conv": int((t4["fourth_down_converted"] == 1).sum()),
        }
    return out


def team_game_metrics(drive_aggs):
    """Per (game_id, team): window metric numerators/denominators.

    Returns dict (game_id, team) -> dict(rz_trips, rz_tds, third_att,
    third_conv, gtg_drives, gtg_tds, fourth_att, fourth_conv).
    """
    out = defaultdict(lambda: {"rz_trips": 0, "rz_tds": 0, "third_att": 0,
                                "third_conv": 0, "gtg_drives": 0,
                                "gtg_tds": 0, "fourth_att": 0,
                                "fourth_conv": 0})
    for (gid, _drive, team), a in drive_aggs.items():
        m = out[(gid, team)]
        m["rz_trips"] += 1 if a["rz_trip"] else 0
        m["rz_tds"] += 1 if a["rz_td"] else 0
        m["third_att"] += a["third_att"]
        m["third_conv"] += a["third_conv"]
        m["gtg_drives"] += 1 if a["gtg"] else 0
        m["gtg_tds"] += 1 if a["gtg_td"] else 0
        m["fourth_att"] += a["fourth_att"]
        m["fourth_conv"] += a["fourth_conv"]
    return dict(out)


def trailing_window_metrics(team_games, game_order, gid, team,
                            n_games=TRAILING_GAMES):
    """Aggregate metrics over the trailing-N REG games strictly before gid.

    team_games: dict team -> list of game_ids in chronological order.
    game_order: dict game_id -> (season, week) for ordering.
    Returns dict of summed numerators/denominators, or None if fewer
    than n_games of history exist.
    """
    hist = [g for g in team_games.get(team, [])
            if game_order[g] < game_order[gid]]
    if len(hist) < n_games:
        return None
    window = hist[-n_games:]
    return window


def summarize_window(window, team_game, team):
    """Sum metric numerators/denominators for team over a list of game_ids."""
    s = {"rz_trips": 0, "rz_tds": 0, "third_att": 0, "third_conv": 0,
         "gtg_drives": 0, "gtg_tds": 0, "fourth_att": 0, "fourth_conv": 0}
    for gid in window:
        m = team_game.get((gid, team))
        if m is None:
            continue
        for k in s:
            s[k] += m[k]
    return s


def window_rates(s):
    """Rates for the four closed metrics; None where denominator is 0."""
    def rate(num, den):
        return (num / den) if den > 0 else None
    return {
        "rz_td_pct": rate(s["rz_tds"], s["rz_trips"]),
        "third_pct": rate(s["third_conv"], s["third_att"]),
        "gtg_td_pct": rate(s["gtg_tds"], s["gtg_drives"]),
        "fourth_pct": rate(s["fourth_conv"], s["fourth_att"]),
        "rz_trips": s["rz_trips"],
    }


# ---- I/O --------------------------------------------------------------------

def load_kickoffs(url=NFLVERSE_SCHEDULES_URL):
    """game_id -> kickoff_utc. Reads ONLY gameday/gametime/stadium.

    nflverse schedules carry scores; this loader never reads them.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-hn8-elig"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read().decode("utf-8")
    out = {}
    for row in csv.DictReader(io.StringIO(raw)):
        if row.get("season") not in ("2021", "2022", "2023", "2024"):
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


def load_pbp_history(seasons=HISTORY_SEASONS):
    """Team-game situational aggregates for REG games, 2021-2024.

    Downloads nflverse parquet (free HTTP) reading ONLY PBP_COLUMNS.
    Returns (team_game, game_order, team_games):
      team_game: (game_id, team) -> metric sums for that game
      game_order: game_id -> (season, week)
      team_games: team -> chronologically ordered list of game_ids
    """
    import pandas as pd  # lazy: runner only
    frames = []
    for season in seasons:
        url = PBP_URL.format(season=season)
        req = urllib.request.Request(url,
                                     headers={"User-Agent": "nfl-edge-hn8-elig"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            data = resp.read()
        df = pd.read_parquet(io.BytesIO(data), columns=PBP_COLUMNS)
        df = df[(df["season_type"] == "REG") & (df["season"] == season)]
        frames.append(df)
    pbp = pd.concat(frames, ignore_index=True)
    game_order = (pbp[["game_id", "season", "week"]]
                  .drop_duplicates()
                  .set_index("game_id")[["season", "week"]]
                  .apply(tuple, axis=1).to_dict())
    aggs = drive_aggregates(pbp)
    team_game = team_game_metrics(aggs)
    team_games = defaultdict(list)
    for (gid, team) in team_game:
        team_games[team].append(gid)
    for team in team_games:
        team_games[team] = sorted(set(team_games[team]),
                                 key=lambda g: game_order[g])
    return team_game, game_order, dict(team_games)


def run_eligibility(conn):
    report = {
        "prereg": "docs/preregistrations/nfl-h-n8-redzone-conversion-totals-draft.md",
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

    # ---- 0. freeze check (frozen scope: spread+total) -------------------------
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
    funnel["kickoffs_loaded"] = len(kickoffs)

    # canonical identity (in-memory, same as the H-N2/H-N6 gates)
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

    # ---- slot machinery (frozen cadence) --------------------------------------
    slots = gate.snapshot_slots()
    actual_oas = [r[0] for r in conn.execute(
        "SELECT DISTINCT observed_at FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL') ORDER BY 1"
        % hq_table).fetchall()]
    slot_oa, unmatched_oas = gate.match_observed_to_slots(actual_oas, slots)
    report["snapshot_slot_match"] = {"matched": len(slot_oa),
                                     "expected": len(slots),
                                     "unmatched_observed_at": len(unmatched_oas)}

    # ---- totals quotes bucketed by slot ---------------------------------------
    all_eids = sorted({eid for eids in game_to_eids.values() for eid in eids})
    qrows = conn.execute(
        "SELECT provider_event_id, book_key, selection, line, observed_at "
        "FROM public.%s "
        "WHERE market = 'FULL_GAME_TOTAL' AND provider_event_id = ANY(%%s)"
        % hq_table, (all_eids,)).fetchall()
    oa_to_slot = {(s, w, slot): oa for (s, w, slot), oa in slot_oa.items()}
    slot_by_oa = {oa: key for key, oa in oa_to_slot.items()}
    quotes_by_slot = defaultdict(list)
    n_unbucketed = 0
    for eid, book, sel, line, oa in qrows:
        key = slot_by_oa.get(oa)
        if key is None:
            n_unbucketed += 1
            continue
        quotes_by_slot[(eid,) + key].append((book, sel, line))
    report["quotes_accounting"] = {"totals_quote_rows_read": len(qrows),
                                   "rows_outside_frozen_slots": n_unbucketed}

    # ---- conversion feature history (free nflverse pbp; no score cols) ---------
    team_game, game_order, team_games = load_pbp_history()
    report["pbp_history"] = {"seasons": list(HISTORY_SEASONS),
                             "team_game_rows": len(team_game),
                             "teams": len(team_games),
                             "pbp_columns_read": PBP_COLUMNS}

    # ---- per-game funnel --------------------------------------------------------
    counts = defaultdict(int)
    feature_vals = {"x_primary": [], "rz_td_pct": [], "third_pct": [],
                    "gtg_td_pct": [], "fourth_pct": []}
    for g in bundle_games:
        gid = g["game_id"]
        s, w = g["season"], g["week"]
        rec = {"nflverse_game_id": gid, "season": s, "week": w,
               "home_team": g["home_team"], "away_team": g["away_team"],
               "status": None, "exclusion_reason": None}
        eids = game_to_eids.get(gid, [])
        if not eids:
            rec.update(status="excluded", exclusion_reason="no_identity")
            counts["no_identity"] += 1
            per_game.append(rec)
            continue
        kickoff = kickoffs.get(gid)
        if kickoff is None:
            rec.update(status="excluded", exclusion_reason="no_kickoff")
            counts["no_kickoff"] += 1
            per_game.append(rec)
            continue
        rec["kickoff_utc"] = kickoff.isoformat()

        slot, inst = decision_slot_for_game(s, w, kickoff, slots)
        if slot is None:
            rec.update(status="excluded", exclusion_reason="no_pre_cutoff_slot")
            counts["no_pre_cutoff_slot"] += 1
            per_game.append(rec)
            continue
        rec["decision_slot"] = slot
        oa = oa_to_slot.get((s, w, slot))
        over = []
        if oa is not None:
            for eid in eids:
                for (book, sel, line) in quotes_by_slot.get((eid, s, w, slot), []):
                    if sel == "Over":
                        over.append((book, line))
        cons = gate.build_consensus(over)
        rec["consensus_line"] = cons["consensus_line"]
        rec["n_books_at_line"] = cons["n_books_at_line"]
        rec["pinnacle_present"] = cons["pinnacle_present"]
        if not cons["eligible"]:
            rec.update(status="excluded",
                       exclusion_reason="no_decision_consensus")
            counts["no_decision_consensus"] += 1
            per_game.append(rec)
            continue

        # feature history: trailing-8 + >=12 RZ trips, both teams
        if gid not in game_order:
            rec.update(status="excluded",
                       exclusion_reason="no_pbp_game_order")
            counts["no_pbp_game_order"] += 1
            per_game.append(rec)
            continue
        ok = True
        rates = {}
        for role, team in (("home", g["home_team"]), ("away", g["away_team"])):
            window = trailing_window_metrics(team_games, game_order, gid,
                                             team)
            if window is None:
                rec.update(status="excluded", exclusion_reason=
                           f"insufficient_history:{role}")
                counts["insufficient_history"] += 1
                ok = False
                break
            sums = summarize_window(window, team_game, team)
            r = window_rates(sums)
            if r["rz_trips"] < MIN_RZ_TRIPS:
                rec.update(status="excluded", exclusion_reason=
                           f"min_rz_trips:{role}:{r['rz_trips']}")
                counts["min_rz_trips"] += 1
                ok = False
                break
            rates[role] = r
        if not ok:
            per_game.append(rec)
            continue
        x = rates["home"]["rz_td_pct"] - rates["away"]["rz_td_pct"]
        rec["x_primary_rz_td_diff"] = round(x, 4)
        rec["home_rz_trips"] = rates["home"]["rz_trips"]
        rec["away_rz_trips"] = rates["away"]["rz_trips"]
        feature_vals["x_primary"].append(x)
        for key, mkey in (("rz_td_pct", "rz_td_pct"), ("third_pct", "third_pct"),
                          ("gtg_td_pct", "gtg_td_pct"),
                          ("fourth_pct", "fourth_pct")):
            vals = [rates["home"][mkey], rates["away"][mkey]]
            vals = [v for v in vals if v is not None]
            feature_vals[key].extend(vals)
        rec.update(status="eligible", exclusion_reason=None)
        counts["eligible"] += 1
        per_game.append(rec)

    funnel["attrition"] = dict(counts)
    funnel["eligible_n"] = counts["eligible"]
    report["funnel"] = funnel

    # ---- feature summaries (features only; never outcomes) ----------------------
    feat = {}
    for key, vals in feature_vals.items():
        if vals:
            feat[key] = {"n": len(vals),
                         "mean": round(statistics.fmean(vals), 4),
                         "sd": round(statistics.pstdev(vals), 4)
                         if len(vals) > 1 else None,
                         "min": round(min(vals), 4),
                         "max": round(max(vals), 4)}
        else:
            feat[key] = {"n": 0}
    report["feature_summaries"] = feat

    sd_x = feat["x_primary"]["sd"]
    verdict, mde = ("INFEASIBLE", None) if counts["eligible"] < N_FLOOR else \
        feasibility_verdict(counts["eligible"], sd_x or 0.0)
    report["feasibility"] = {
        "n_floor": N_FLOOR,
        "mde_cap_pts_per_unit": MDE_CAP,
        "sd_x_observed": sd_x,
        "mde_pts_per_unit": (round(mde, 3) if mde is not None else None),
        "mde_inputs": {"z_sum": MDE_Z_SUM, "sigma_res_pts": MDE_SIGMA_RES},
        "verdict": verdict,
    }
    report["per_game"] = per_game
    return report


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--per-game-csv", default=None)
    args = ap.parse_args(argv)
    dsn = __import__("os").environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    import psycopg  # lazy: runner only
    with psycopg.connect(dsn) as conn:
        report = run_eligibility(conn)
    Path(args.out).write_text(json.dumps(report, indent=2))
    if args.per_game_csv:
        with open(args.per_game_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "nflverse_game_id", "season", "week", "home_team", "away_team",
                "kickoff_utc", "decision_slot", "consensus_line",
                "n_books_at_line", "pinnacle_present",
                "x_primary_rz_td_diff", "home_rz_trips", "away_rz_trips",
                "status", "exclusion_reason"])
            w.writeheader()
            for r in report.get("per_game", []):
                w.writerow({k: r.get(k) for k in w.fieldnames})
    n = report.get("funnel", {}).get("eligible_n")
    v = report.get("feasibility", {}).get("verdict")
    print(json.dumps({"eligible_n": n, "verdict": v,
                      "outcome_rows_read": report.get("outcome_rows_read"),
                      "api_credits_used": 0, "db_writes": 0}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
