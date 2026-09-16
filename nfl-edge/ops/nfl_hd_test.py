#!/usr/bin/env python3
"""H-D Phase 5: frozen statistical test (runs ONLY after integrity PASS).

Reads the pull artifact + integrity report (usable units). Reads OUTCOMES
here for the first time: nflverse weekly player_stats (realized attempts).

Frozen test (nfl-h-d-precipitation-attempts-frozen.md):
  - Unit: (game, designated QB1/RB1). R = realized attempts
    (column map: pass attempts <- player_stats.attempts,
                 rush attempts <- player_stats.carries).
  - Target: residual R - L (L = consensus line at T_dec).
  - Season demeaning; one-sided one-sample t per endpoint with
    season-week-clustered SEs; Holm step-down across the 2 endpoints;
    alpha = 0.05. t df = G-1 (G = season-week clusters).
  - Directional gate: significant wrong-sign = FAIL on mechanism.
  - Stage 2 economic: Over 1u rush / Under 1u pass at consensus line,
    best available price among books at the line (within 0.01), ties ->
    alphabetical book_key; pushes return stake. Bar +0.005u/unit.
  - Voids: designated player DNP/inactive at kickoff -> excluded, recorded.
  - PASS needs BOTH endpoints Holm-significant correct-sign AND
    economic >= +0.005u/unit. Significance and economics reported
    SEPARATELY; significance alone never implies economic usefulness.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from scipy import stats as sps

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import nfl_hd_common as C

ALPHA = 0.05
ECON_BAR = 0.005


def cluster_t(vals, clusters, alternative):
    """One-sample t with cluster-robust SE. Returns (mean, se, t, df, p)."""
    import numpy as np
    vals = np.asarray(vals, dtype=float)
    n = len(vals)
    mean = vals.mean()
    clu = {}
    for v, c in zip(vals, clusters):
        clu[c] = clu.get(c, 0.0) + v
    sums = np.array(list(clu.values()))
    se = float((sums ** 2).sum() ** 0.5) / n
    G = len(clu)
    t = mean / se if se > 0 else 0.0
    df = G - 1
    if alternative == "greater":
        p = float(sps.t.sf(t, df))
    else:
        p = float(sps.t.sf(-t, df))
    return mean, se, t, df, p, G


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pull", required=True)
    ap.add_argument("--integrity", required=True)
    ap.add_argument("--data-dir", default="/tmp")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    integ = json.load(open(args.integrity))
    if integ.get("verdict") != "PASS":
        raise SystemExit("integrity gate did not PASS; refusing to run test")
    usable = integ["usable_units"]

    ps = pd.concat(
        [pd.read_csv(f"{args.data_dir}/player_stats_{s}.csv",
                     usecols=["player_id", "player_name", "position",
                              "recent_team", "season", "week",
                              "attempts", "carries"])
         for s in (2022, 2023, 2024)], ignore_index=True)
    ps["nname"] = ps["player_name"].map(C.norm_name)

    snaps = pd.concat(
        [pd.read_csv(f"{args.data_dir}/snap_counts_{s}.csv",
                     usecols=["season", "week", "player", "position",
                              "team", "offense_snaps"])
         for s in (2022, 2023, 2024)], ignore_index=True)

    receipt = {"test": "nfl-h-d frozen test",
               "run_utc": datetime.now(timezone.utc).isoformat(),
               "prereg": ("nfl-edge/docs/preregistrations/"
                          "nfl-h-d-precipitation-attempts-frozen.md"),
               "column_map": {"pass_attempts": "player_stats.attempts",
                              "rush_attempts": "player_stats.carries"},
               "outcome_rows_read": int(len(ps)),
               "endpoints": {}}

    # Verify the frozen attempts tie-break binds nowhere.
    tiebreak_diffs = []
    for mkt, pos, att_col in (("player_pass_attempts", "QB", "attempts"),
                              ("player_rush_attempts", "RB", "carries")):
        for u in usable[mkt]:
            season, week = u["season"], u["week"]
            team = u["game"].split("_")[3]
            weeks = C.lagged_weeks(team, season, week, snaps)
            pos_df = snaps[(snaps["team"] == team)
                           & (snaps["position"] == pos)]
            players = sorted(pos_df["player"].dropna().unique())
            snap = {(p, w): 0 for p in players for w in weeks}
            for r in pos_df.itertuples():
                key = (r.player, (int(r.season), int(r.week)))
                if key in snap:
                    snap[key] = int(r.offense_snaps or 0)
            att = {}
            if weeks:
                w1s, w1w = weeks[0]
                lag = ps[(ps["season"] == w1s) & (ps["week"] == w1w)
                         & (ps["recent_team"] == team)]
                for r in lag.itertuples():
                    nm = C.norm_name(r.player_name)
                    att[nm] = att.get(nm, 0) + int(getattr(r, att_col) or 0)

            def key_full(p):
                return (tuple(-snap[(p, w)] for w in weeks)
                        + (-att.get(C.norm_name(p), 0), C.norm_name(p)))
            best = min(players, key=key_full)
            if C.norm_name(best) != C.norm_name(u["player"]):
                tiebreak_diffs.append(
                    {"game": u["game"], "endpoint": mkt,
                     "integrity_pick": u["player"], "full_rule_pick": best})
    receipt["attempts_tiebreak_diffs"] = tiebreak_diffs
    receipt["attempts_tiebreak_binds"] = len(tiebreak_diffs) > 0

    results = {}
    for mkt, pos, att_col, direction in (
            ("player_pass_attempts", "QB", "attempts", "under"),
            ("player_rush_attempts", "RB", "carries", "over")):
        units, voids = [], []
        for u in usable[mkt]:
            season, week = u["season"], u["week"]
            team = u["game"].split("_")[3]
            nm = C.norm_name(u["player"])
            cand = ps[(ps["season"] == season) & (ps["week"] == week)
                      & (ps["nname"] == nm)]
            row = cand[cand["recent_team"] == team]
            if row.empty:
                row = cand  # traded-player fallback
            if row.empty:
                voids.append({"game": u["game"], "player": u["player"],
                              "reason": "no_player_stats_row_DNP"})
                continue
            R = float(row.iloc[0][att_col])
            L = float(u["L"])
            # Stage 2 entry: best available price among books quoting at the
            # consensus line (within 0.01); alternate lines handled per row;
            # ties -> alphabetical book_key.
            key = "over" if direction == "over" else "under"
            priced = []
            for b in u["books"]:
                for ln in b.get("lines", []):
                    if abs(ln["line"] - L) <= 0.01 and ln.get(key) is not None:
                        priced.append((b["book"], ln[key]))
            price = None
            book_used = None
            if priced:
                # best price = max decimal; ties -> alphabetical book_key
                priced.sort(key=lambda x: (-C.american_to_decimal(x[1]),
                                           x[0]))
                book_used, price = priced[0]
            units.append({"game": u["game"], "season": season, "week": week,
                          "player": u["player"], "R": R, "L": L,
                          "resid": R - L, "book": book_used, "price": price,
                          "n_books": len(u["books"])})
        # season demeaning
        df = pd.DataFrame(units)
        df["resid_dm"] = df.groupby("season")["resid"].transform(
            lambda s: s - s.mean())
        df["cluster"] = df["season"].astype(str) + "_w" + df["week"].astype(str)
        alt = "greater" if direction == "over" else "less"
        mean, se, t, tdf, p, G = cluster_t(df["resid_dm"].tolist(),
                                           df["cluster"].tolist(), alt)
        # economic P&L
        pnls, bets = [], 0
        for _, r in df.iterrows():
            if r["price"] is None:
                continue
            bets += 1
            if abs(r["R"] - r["L"]) < 1e-9:
                pnls.append({"pnl": 0.0, "cluster": r["cluster"],
                             "res": "push"})
            elif (r["R"] > r["L"]) == (direction == "over"):
                pnls.append({"pnl": C.american_profit(r["price"]),
                             "cluster": r["cluster"], "res": "win"})
            else:
                pnls.append({"pnl": -1.0, "cluster": r["cluster"],
                             "res": "loss"})
        pm, pse, pt, pdf, _, pG = cluster_t(
            [x["pnl"] for x in pnls], [x["cluster"] for x in pnls], "greater") \
            if pnls else (0, 0, 0, 0, 1, 0)
        ci_lo = pm - sps.t.ppf(0.975, pdf) * pse if pnls else 0
        ci_hi = pm + sps.t.ppf(0.975, pdf) * pse if pnls else 0
        team_means = df.groupby(df["game"].str.split("_").str[3])[
            "resid_dm"].mean().round(3).to_dict()
        results[mkt] = {
            "n_usable": len(units), "n_voids": len(voids), "voids": voids,
            "direction": direction, "alternative": alt,
            "mean_resid_dm": round(mean, 4), "se_cluster": round(se, 4),
            "t": round(t, 4), "df": tdf, "p_one_sided": round(p, 6),
            "n_clusters": G,
            "econ": {"bets": bets, "mean_pnl": round(pm, 4),
                     "se": round(pse, 4), "ci95": [round(ci_lo, 4),
                                                   round(ci_hi, 4)],
                     "wins": sum(1 for x in pnls if x["res"] == "win"),
                     "losses": sum(1 for x in pnls if x["res"] == "loss"),
                     "pushes": sum(1 for x in pnls if x["res"] == "push")},
            "team_resid_means_descriptive": team_means,
        }
    receipt["endpoints"] = results

    # Holm step-down across the two endpoints (m=2)
    order = sorted(results, key=lambda m: results[m]["p_one_sided"])
    holm = {}
    p1 = results[order[0]]["p_one_sided"]
    holm[order[0]] = {"reject": bool(p1 <= ALPHA / 2),
                      "threshold": ALPHA / 2, "p": p1}
    if holm[order[0]]["reject"]:
        p2 = results[order[1]]["p_one_sided"]
        holm[order[1]] = {"reject": bool(p2 <= ALPHA),
                          "threshold": ALPHA, "p": p2}
    else:
        holm[order[1]] = {"reject": False, "threshold": None,
                          "p": results[order[1]]["p_one_sided"]}
    receipt["holm"] = holm

    # directional gate + disposition (frozen rule)
    def correct_sign(mkt):
        t = results[mkt]["t"]
        want = "greater" if mkt == "player_rush_attempts" else "less"
        return (t > 0) if want == "greater" else (t < 0)

    sig_correct = {m: holm[m]["reject"] and correct_sign(m) for m in results}
    sig_wrong = {m: holm[m]["reject"] and not correct_sign(m) for m in results}
    econ = {m: results[m]["econ"]["mean_pnl"] for m in results}
    receipt["significant_correct_sign"] = sig_correct
    receipt["significant_wrong_sign"] = sig_wrong

    if any(sig_wrong.values()):
        disp = "FAIL"
        why = "endpoint significant with wrong sign (mechanism rejected)"
    elif not all(sig_correct.values()):
        disp = "FAIL"
        why = "not both endpoints Holm-significant with correct sign"
    elif not all(econ[m] >= ECON_BAR for m in econ):
        disp = "INCONCLUSIVE"
        why = (f"significant correct-sign but economic < +{ECON_BAR}u/unit "
               f"(pass={econ['player_pass_attempts']}, "
               f"rush={econ['player_rush_attempts']})")
    else:
        disp = "PASS"
        why = ("both endpoints Holm-significant correct-sign AND "
               f"economic >= +{ECON_BAR}u/unit")
    receipt["econ_bar"] = ECON_BAR
    receipt["econ_mean_pnl"] = econ
    receipt["disposition"] = disp
    receipt["disposition_why"] = why
    Path(args.out).write_text(json.dumps(receipt, indent=2))
    print(json.dumps({"disposition": disp, "why": why,
                      "holm": holm,
                      "econ": econ,
                      "p_values": {m: results[m]["p_one_sided"]
                                   for m in results}}, indent=2))


if __name__ == "__main__":
    main()
