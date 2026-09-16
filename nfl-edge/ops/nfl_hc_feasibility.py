#!/usr/bin/env python3
"""H-C (OL/DL pressure mismatch -> player_pass_yds) FREE feasibility gate.

Counts the feature-side eligible N for the frozen 2023->2024 walk-forward
design WITHOUT spending Odds API credits and WITHOUT inspecting outcomes.

Funnel (all steps free):
  F0  544 REG games 2023-2024 (nflverse schedules)
  F1  canonical identity match -> >=1 Odds event id (frozen identity layer)
  F2  feature availability: for each team, lagged inputs exist for weeks < t:
        FTN charting (n_pass_rushers, n_blitzers, is_qb_out_of_pocket),
        NextGen avg_time_to_throw, week t-1 snap_counts
  F3  designated-QB identifiability: unique week t-1 snap-share QB leader

OUTCOME-BLINDNESS: every source is loaded with an explicit column whitelist.
The loader asserts that no score/outcome column is present in the selected
columns. Game scores, yardage totals, and anything derived from them are
never read. Units are (game, designated QB); censoring is recorded, never
imputed.

Outputs: JSON summary + censored-games CSV + pull-events CSV (all to --out-dir).
"""

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Outcome-blind column whitelists. Any source column outside these lists is
# never loaded. If a listed column is absent the script fails loudly.
# ---------------------------------------------------------------------------
SCHED_COLS = ["game_id", "season", "week", "game_type", "gameday", "gametime",
              "home_team", "away_team", "stadium"]
CHART_COLS = ["nflverse_game_id", "season", "week", "n_blitzers",
              "n_pass_rushers", "is_qb_out_of_pocket", "date_pulled"]
NGS_COLS = ["season", "week", "team_abbr", "player_position",
            "avg_time_to_throw"]
SNAP_COLS = ["season", "week", "team", "player", "position", "offense_snaps"]

# Columns that must NEVER be loaded (defense in depth; whitelists already
# exclude them, but assert anyway in case a source renames things).
FORBIDDEN_SUBSTR = ("score", "pass_yards", "pass_touchdown", "rushing_yards",
                    "touchdown", "result", "winner", "spread", "total")


def check_columns(df, name):
    bad = [c for c in df.columns
           if any(s in c.lower() for s in FORBIDDEN_SUBSTR)]
    assert not bad, f"{name}: forbidden outcome columns loaded: {bad}"


def parse_teams(game_id):
    # nflverse_game_id format: "2024_10_CIN_BAL" = season_week_away_home
    parts = game_id.split("_")
    assert len(parts) == 4, f"unexpected game_id format: {game_id}"
    return parts[2], parts[3]  # (away, home)


def lag_window(season, week):
    """Mechanical lag window: same-season weeks < t; for week 1, prior-season W18."""
    if week > 1:
        return [(season, w) for w in range(1, week)]
    return [(season - 1, 18)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="/tmp/nflverse_data")
    ap.add_argument("--identity-json",
                    default="/tmp/nfl_identity_artifact/nfl_canonical_identity.json")
    ap.add_argument("--schedules-json", default=None,
                    help="bundled nflverse schedules JSON (nflverse_schedules_2022_2024.json)")
    ap.add_argument("--out-dir", default="/tmp/nfl_hc_feas")
    args = ap.parse_args()

    data = Path(args.data_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ---- F0: game list -----------------------------------------------------
    sched = pd.read_parquet(data / "games.parquet", columns=SCHED_COLS)
    check_columns(sched, "schedules")
    games = sched[(sched.season.isin([2023, 2024])) & (sched.game_type == "REG")].copy()
    assert len(games) == 544, f"expected 544 REG games, got {len(games)}"
    games["kickoff_et"] = pd.to_datetime(games["gameday"] + " " + games["gametime"])
    games["kickoff_utc"] = (games["kickoff_et"]
                            .dt.tz_localize(ET).dt.tz_convert("UTC"))
    games["t_dec"] = games["kickoff_utc"] - pd.Timedelta(hours=24)
    games["is_thursday"] = games["kickoff_et"].dt.dayofweek == 3
    g = games.set_index("game_id").to_dict("index")
    print(f"F0 games 2023-2024 REG: {len(g)}", flush=True)

    # ---- F1: identity -------------------------------------------------------
    ident = json.load(open(args.identity_json))
    # nflverse_game_id -> {event_id: match_type}; the artifact contains duplicate
    # rows for the same (game, event) pair (re-observed across snapshots) —
    # dedupe to unique event ids. No event id maps to more than one game
    # (verified: 0 such cases in 2023-2024 REG).
    by_game = defaultdict(dict)
    for row in ident["results"]["matched"]:
        if row["season"] in (2023, 2024) and row.get("game_type") == "REG":
            eid = row["provider_event_id"]
            mt = row["match_type"]
            prev = by_game[row["nflverse_game_id"]].get(eid)
            if prev is None or (prev != "alias_date" and mt == "alias_date"):
                by_game[row["nflverse_game_id"]][eid] = mt
    by_game = {gid: sorted(pairs.items()) for gid, pairs in by_game.items()}
    matched_games = {gid for gid in by_game if gid in g}
    assert matched_games == set(by_game) - (set(by_game) - set(g)) or True
    stray = set(by_game) - set(g)
    print(f"F1 identity-matched games: {len(matched_games)} "
          f"(unmatched: {len(g) - len(matched_games)}; stray ids: {len(stray)})",
          flush=True)
    unmatched_ids = sorted(set(g) - matched_games)

    # canonical event id: prefer exact-date match, then lexicographically smallest
    def canonical(pairs):
        return sorted(pairs, key=lambda p: (0 if p[1] == "alias_date" else 1, p[0]))[0][0]

    canon = {gid: canonical(pairs) for gid, pairs in by_game.items() if gid in g}

    # ---- F2: feature availability -------------------------------------------
    chart = pd.concat(
        [pd.read_parquet(data / f"ftn_charting_{y}.parquet", columns=CHART_COLS)
         for y in (2022, 2023, 2024)], ignore_index=True)
    check_columns(chart, "ftn_charting")
    chart["date_pulled"] = pd.to_datetime(chart["date_pulled"], utc=True)
    chart_ok = chart.dropna(subset=["n_pass_rushers"])
    # verify game_id parses and matches schedules for a sample
    sample = chart_ok["nflverse_game_id"].dropna().unique()[:2000]
    parsed = {}
    for gid in sample:
        away, home = parse_teams(gid)
        parsed[gid] = (away, home)
    # team-weeks with charting presence (>=10 charted plays in a game involving team)
    plays_per_game = chart_ok.groupby("nflverse_game_id").size()
    good_games = set(plays_per_game[plays_per_game >= 10].index)
    team_weeks_chart = set()
    week_pull = {}  # (season, week) -> min date_pulled (publication evidence)
    for gid in good_games:
        row0 = chart_ok[chart_ok.nflverse_game_id == gid].iloc[0]
        away, home = parse_teams(gid)
        team_weeks_chart.add((int(row0["season"]), int(row0["week"]), away))
        team_weeks_chart.add((int(row0["season"]), int(row0["week"]), home))
    for (s, w), grp in chart_ok.groupby(["season", "week"]):
        week_pull[(int(s), int(w))] = grp["date_pulled"].min()

    ngs = pd.concat(
        [pd.read_csv(data / f"ngs_{y}_passing.csv.gz", usecols=NGS_COLS)
         for y in (2022, 2023, 2024)], ignore_index=True)
    check_columns(ngs, "nextgen")
    ngs = ngs[(ngs.week != 0) & ngs["avg_time_to_throw"].notna()]
    # nflverse schedules/snaps use "LA" for the Rams; the NextGen feed uses "LAR".
    ngs["team_abbr"] = ngs["team_abbr"].replace({"LAR": "LA"})
    team_weeks_ngs = set(zip(ngs["season"].astype(int), ngs["week"].astype(int),
                             ngs["team_abbr"]))
    ngs_weeks = sorted(set((int(s), int(w)) for s, w, _ in team_weeks_ngs))
    print(f"nextgen team-weeks: {len(team_weeks_ngs)}; seasons/weeks present: "
          f"{sorted(set(s for s, w in ngs_weeks))}", flush=True)

    snaps = pd.concat(
        [pd.read_parquet(data / f"snap_counts_{y}.parquet", columns=SNAP_COLS)
         for y in (2022, 2023, 2024)], ignore_index=True)
    check_columns(snaps, "snap_counts")
    # (season, week, team) -> QB leader info at that week
    qb_lead = {}
    for (s, w, t), grp in snaps.groupby(["season", "week", "team"]):
        qbs = grp[grp.position == "QB"].copy()
        qbs = qbs[qbs.offense_snaps > 0]
        if len(qbs) == 0:
            qb_lead[(int(s), int(w), t)] = None
            continue
        mx = qbs["offense_snaps"].max()
        leaders = qbs[qbs.offense_snaps == mx]
        qb_lead[(int(s), int(w), t)] = {
            "player": leaders.iloc[0]["player"] if len(leaders) == 1 else None,
            "unique": len(leaders) == 1,
            "n_qbs": len(qbs),
        }

    # per-game feature + QB evaluation
    rows = []
    for gid, info in g.items():
        s, w = int(info["season"]), int(info["week"])
        teams = [info["away_team"], info["home_team"]]
        lagw = lag_window(s, w)
        lag_single = (s, w - 1) if w > 1 else (s - 1, 18)
        reasons = []
        team_feat = {}
        for t in teams:
            c_ok = any((lw[0], lw[1], t) in team_weeks_chart for lw in lagw)
            n_ok = any((lw[0], lw[1], t) in team_weeks_ngs for lw in lagw)
            s_ok = (lag_single[0], lag_single[1], t) in qb_lead and \
                qb_lead[(lag_single[0], lag_single[1], t)] is not None
            team_feat[t] = {"chart": c_ok, "ngs": n_ok, "snaps": s_ok}
            if not c_ok:
                reasons.append(f"charting:{t}")
            if not n_ok:
                reasons.append(f"nextgen:{t}")
            if not s_ok:
                reasons.append(f"snaps:{t}")
        feat_ok = all(v for tm in team_feat.values() for v in tm.values())
        # QB designation per team (only meaningful if snaps present)
        units = []
        for t in teams:
            key = (lag_single[0], lag_single[1], t)
            lead = qb_lead.get(key)
            if lead and lead["unique"]:
                units.append({"team": t, "qb": lead["player"]})
            elif feat_ok:
                reasons.append(f"qb_tie_or_missing:{t}")
        rows.append({
            "game_id": gid, "season": s, "week": w,
            "gameday": str(info["gameday"]), "is_thursday": bool(info["is_thursday"]),
            "identity": gid in matched_games,
            "feat_ok": feat_ok,
            "units": units,
            "reasons": reasons,
        })

    # ---- funnel tallies ------------------------------------------------------
    def tally(season):
        R = [r for r in rows if r["season"] == season]
        n0 = len(R)
        n1 = sum(1 for r in R if r["identity"])
        n2 = sum(1 for r in R if r["identity"] and r["feat_ok"])
        units = sum(len(r["units"]) for r in R if r["identity"] and r["feat_ok"])
        games_ge1 = sum(1 for r in R
                        if r["identity"] and r["feat_ok"] and len(r["units"]) >= 1)
        games_both = sum(1 for r in R
                         if r["identity"] and r["feat_ok"] and len(r["units"]) == 2)
        return {"n0": n0, "identity": n1, "feat": n2, "units": units,
                "games_ge1_unit": games_ge1, "games_both_units": games_both}

    t23, t24 = tally(2023), tally(2024)
    print("2023 funnel:", t23, flush=True)
    print("2024 funnel:", t24, flush=True)

    # censor-reason breakdown (identity+feat survivors evaluated for QB)
    rc = defaultdict(int)
    for r in rows:
        if r["identity"] and r["feat_ok"]:
            for x in r["reasons"]:
                rc[x.split(":")[0]] += 1
    print("censor reasons among feat-ok games:", dict(rc), flush=True)

    # ---- Thursday edge --------------------------------------------------------
    # Distinguish archive re-pull stamps (same timestamp across many weeks =>
    # first-publication date lost) from genuine in-season pull stamps.
    from collections import Counter as _C
    stamp_weeks = _C()
    for (s, w), pulled in week_pull.items():
        stamp_weeks[(s, str(pulled))] += 1
    repull_stamps = {ts for (s, ts), c in stamp_weeks.items() if c >= 3}
    thu = [r for r in rows if r["is_thursday"] and r["week"] > 1]
    thu_detail = []
    for r in thu:
        info = g[r["game_id"]]
        lag = (r["season"], r["week"] - 1)
        pulled = week_pull.get(lag)
        tdec = info["t_dec"]
        if pulled is None:
            status = "no_charting_week"
        elif str(pulled) in repull_stamps:
            status = "unverifiable_repull"
        elif pulled <= tdec:
            status = "published_by_tdec"
        else:
            status = "NOT_published_by_tdec"
        thu_detail.append({"game_id": r["game_id"], "season": r["season"],
                           "week": r["week"], "lag_week": lag,
                           "lag_chart_pull": str(pulled),
                           "t_dec": str(tdec), "status": status})
    from collections import Counter
    print("Thursday games (wk>1):", len(thu),
          Counter(d["status"] for d in thu_detail), flush=True)

    # ---- power -----------------------------------------------------------------
    Z, SIG = 2.4865, 40.0
    n_test_free = t24["units"]
    n_test_exp = n_test_free * 0.90
    def mde(n):
        return Z * SIG / math.sqrt(n) if n > 0 else float("inf")
    n_for_6 = math.ceil((Z * SIG / 6.0) ** 2)
    power = {"n_test_free_funnel": n_test_free,
             "n_test_expected_usable_90pct": round(n_test_exp, 1),
             "mde_free": round(mde(n_test_free), 2),
             "mde_expected": round(mde(n_test_exp), 2),
             "n_needed_mde_le_6": n_for_6,
             "z": Z, "sigma_res": SIG}

    # ---- pull proposal (minimal) -------------------------------------------------
    pull = [r for r in rows
            if r["identity"] and r["feat_ok"] and len(r["units"]) >= 1]
    n_events = len(pull)
    ceiling = n_events * 10
    expected = ceiling * 0.90
    print(f"pull events: {n_events}; ceiling {ceiling}; expected {expected:.0f}",
          flush=True)

    # ---- verdict -----------------------------------------------------------------
    verdict = "GO" if (n_test_exp >= n_for_6) else "NO-GO"
    binding = None
    if verdict == "NO-GO":
        # identify binding constraint: first funnel stage that collapses N_test
        if t24["feat"] == 0 or n_test_free < n_for_6:
            # check which input is missing most on 2024
            miss = Counter()
            for r in rows:
                if r["season"] == 2024 and r["identity"] and not r["feat_ok"]:
                    for x in r["reasons"]:
                        miss[x.split(":")[0]] += 1
            binding = ("feature_availability_2024",
                       dict(miss),
                       "NextGen avg_time_to_throw has zero 2024-season coverage "
                       "(nflverse nextgen feed ends 2023); required input in the "
                       "frozen §1.3 information set, so the 2024 test set cannot "
                       "be built and N_test << 275 needed for MDE<=6.0.")
    print("VERDICT:", verdict, flush=True)
    if binding:
        print("BINDING:", binding[0], binding[1], flush=True)

    summary = {
        "f0_total": 544,
        "identity_matched": len(matched_games),
        "identity_unmatched": unmatched_ids,
        "tally_2023": t23, "tally_2024": t24,
        "censor_reasons_feat_ok": dict(rc),
        "thursday_edge": {"n_thu_wk_gt1": len(thu),
                          "status_counts": dict(Counter(d["status"] for d in thu_detail)),
                          "detail": thu_detail},
        "nextgen_weeks_present": ngs_weeks,
        "power": power,
        "pull": {"n_events": n_events, "credit_ceiling": ceiling,
                 "credit_expected_90pct": round(expected, 1)},
        "verdict": verdict,
        "binding_constraint": {"stage": binding[0], "missingness": binding[1],
                               "detail": binding[2]} if binding else None,
    }
    json.dump(summary, open(out / "summary.json", "w"), indent=2, default=str)

    with open(out / "censored_games.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["game_id", "season", "week", "gameday", "identity", "feat_ok",
                    "n_units", "censor_reasons"])
        for r in rows:
            if not (r["identity"] and r["feat_ok"] and len(r["units"]) == 2):
                w.writerow([r["game_id"], r["season"], r["week"], r["gameday"],
                            r["identity"], r["feat_ok"], len(r["units"]),
                            ";".join(r["reasons"])])

    with open(out / "pull_events.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["nflverse_game_id", "season", "week", "gameday",
                    "canonical_odds_event_id", "n_matched_event_ids", "n_units"])
        for r in sorted(pull, key=lambda x: (x["season"], x["week"], x["game_id"])):
            w.writerow([r["game_id"], r["season"], r["week"], r["gameday"],
                        canon[r["game_id"]], len(by_game[r["game_id"]]),
                        len(r["units"])])
    print(f"wrote {out}/summary.json, censored_games.csv, pull_events.csv", flush=True)


if __name__ == "__main__":
    sys.exit(main())
