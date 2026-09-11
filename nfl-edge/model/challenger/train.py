"""Train the challenger: walk nflverse games chronologically, fit Elo,
validate on a holdout window against closing lines, write a versioned
artifact. Never promotes automatically; the gate decides.

Usage:
    python -m model.challenger.train --version v1 [--fetch]
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from model.challenger import data as data_mod
from model.challenger.elo import (Phi, cover_prob_home, expected_scores,
                                  init_ratings, over_prob, update_ratings)
from model.challenger.validate import evaluate, gate_decision

LEAGUE_AVG = 22.0
HFA = 1.2
K = 0.15
TRAIN_END = 2021
VAL_START, VAL_END = 2022, 2024


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="e.g. v1")
    ap.add_argument("--fetch", action="store_true",
                    help="download fresh nflverse data first")
    ap.add_argument("--train-end", type=int, default=TRAIN_END)
    ap.add_argument("--val-start", type=int, default=VAL_START)
    ap.add_argument("--val-end", type=int, default=VAL_END)
    args = ap.parse_args()

    if args.fetch:
        print("fetching nflverse data...", flush=True)
        data_mod.fetch(force=True)
    games = data_mod.load_games()
    print("games: %d (%d-%d)" % (len(games), games[0]["season"],
                                 games[-1]["season"]), flush=True)

    teams = sorted({g["home"] for g in games} | {g["away"] for g in games})
    off, deff = init_ratings(teams)

    train_margin_res, train_total_res = [], []
    predictions = []  # scored on the validation window only
    base_home_wins = []
    sm = st = None  # fitted on train residuals once training ends

    for g in games:
        h, a = g["home"], g["away"]
        exp_h, exp_a = expected_scores(off, deff, h, a, LEAGUE_AVG, HFA)
        mu_margin = exp_h - exp_a
        mu_total = exp_h + exp_a

        if g["season"] <= args.train_end:
            train_margin_res.append((g["home_score"] - g["away_score"]) - mu_margin)
            train_total_res.append((g["home_score"] + g["away_score"]) - mu_total)
            if g["home_score"] > g["away_score"]:
                base_home_wins.append(1)
            elif g["home_score"] < g["away_score"]:
                base_home_wins.append(0)
        elif args.val_start <= g["season"] <= args.val_end:
            # Predict with ratings BEFORE this game (no lookahead).
            # Sigmas come from training residuals only.
            if sm is None:
                sm = statistics.pstdev(train_margin_res)
                st = statistics.pstdev(train_total_res)
            bw = sum(base_home_wins) / len(base_home_wins)
            predictions.append({
                "task": "win",
                "p": Phi(mu_margin / sm),
                "y": 1 if g["home_score"] > g["away_score"] else 0,
                "base": bw,
            })
            if g["spread_line"] is not None:
                m = g["home_score"] - g["away_score"]
                if abs(m - g["spread_line"]) > 1e-9:  # not a push
                    predictions.append({
                        "task": "spread",
                        "p": cover_prob_home(mu_margin, g["spread_line"], sm),
                        "y": 1 if m - g["spread_line"] > 0 else 0,
                        "base": 0.5,
                    })
            if g["total_line"] is not None:
                t = g["home_score"] + g["away_score"]
                predictions.append({
                    "task": "total",
                    "p": over_prob(mu_total, g["total_line"], st),
                    "y": 1 if t - g["total_line"] > 0 else 0,
                    "base": 0.5,
                })
        # Always update ratings chronologically (final ratings stay current).
        update_ratings(off, deff, h, a, g["home_score"], g["away_score"],
                       LEAGUE_AVG, HFA, K)

    sigma_margin = statistics.pstdev(train_margin_res)
    sigma_total = statistics.pstdev(train_total_res)
    metrics = evaluate(predictions)
    promoted, reasons = gate_decision(metrics)

    artifact = {
        "version": args.version,
        "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "challenger" if promoted else "research",
        "params": {
            "league_avg": LEAGUE_AVG, "hfa": HFA, "k": K,
            "sigma_margin": sigma_margin, "sigma_total": sigma_total,
            "train_end": args.train_end,
            "val_window": [args.val_start, args.val_end],
            "train_games": len(train_margin_res),
        },
        "ratings": {
            "off": {t: round(off[t], 4) for t in teams},
            "def": {t: round(deff[t], 4) for t in teams},
        },
        "validation": metrics,
        "gate": {"promoted": promoted, "reasons": reasons},
        "data": {"games": len(games),
                 "first_season": games[0]["season"],
                 "last_season": games[-1]["season"],
                 "source": "nflverse schedules/games.csv.gz"},
    }

    outdir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "artifacts", "challenger-%s" % args.version)
    os.makedirs(outdir, exist_ok=True)
    model_path = os.path.join(outdir, "model.json")
    with open(model_path, "w") as f:
        json.dump(artifact, f, indent=1, sort_keys=True)
    digest = hashlib.sha256(open(model_path, "rb").read()).hexdigest()
    with open(os.path.join(outdir, "MANIFEST"), "w") as f:
        f.write("model.json sha256:%s\n" % digest)

    print("sigma_margin=%.2f sigma_total=%.2f" % (sigma_margin, sigma_total))
    for task in ("win", "spread", "total"):
        m = metrics[task]
        print("%-6s n=%4d ll=%.4f (base %.4f) brier=%.4f (base %.4f) %s" % (
            task, m["n"], m["log_loss"], m["base_log_loss"],
            m["brier"], m["base_brier"],
            "BEATS" if m.get("beats_baseline") else "does not beat"))
    print("gate promoted=%s" % promoted)
    for r in reasons:
        print("  -", r)
    print("wrote", model_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
