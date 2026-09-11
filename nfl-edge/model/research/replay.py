"""Historical market replay lab.

Walks registered model families through prior NFL seasons exactly as if the
system were running live: every prediction uses only games already observed,
fitted parameters freeze at season boundaries, and probabilities are scored
against the historical closing spread/total. No future-data leakage by
construction: families are chronological state machines (see families.py).

Beyond Brier/log-loss, the lab simulates flat 1u wagers at -110 whenever the
model's price disagrees with the closing line by more than --edge-min. This
is the "tried" evidence: beating a baseline on log-loss is not profit;
beating the closing price historically is the closest offline analog to CLV.

Data honesty notes (read before citing results):
  * nflverse spread_line/total_line is a consensus line, not strictly the
    closing line at one book, and there are no moneylines. The TRUE
    closing-line test runs on forward captures (opener/T-24/T-3/T-90/Close).
  * There is no historical intraday line movement in this data, so the lab
    cannot evaluate opener-vs-close timing. Forward captures will fill that.
  * Simulated ROI assumes -110 both sides and ignores limits, line moves
    after the bet, and pushes (excluded, as in training).

Usage:
    python -m model.research.replay --family elo-v1 --test-start 2010 --test-end 2024
    python -m model.research.replay --family elo-v1 --test-start 2010 --test-end 2024 --fetch
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from model.challenger import data as data_mod
from model.challenger.elo import cover_prob_home, over_prob
from model.challenger.validate import (brier, calibration_slope, log_loss)
from model.research.families import family_names, get_family

BREAKEVEN_110 = 110.0 / 210.0  # 0.52381
PROFIT_110 = 100.0 / 110.0     # 0.90909


def simulate_bets(probs, edge_min):
    """Flat 1u at -110 on model-vs-close disagreements.

    probs: list of (p_home, y_home) with the HOME side as reference.
    Each leg is normalized to the taken side first. Returns economics dict.
    """
    n = wins = 0
    profit = 0.0
    edge_pp_sum = 0.0
    for p_home, y_home in probs:
        # Normalize to the side the model prefers.
        p, y = (p_home, y_home) if p_home >= 0.5 else (1.0 - p_home,
                                                      1 - y_home)
        if abs(p - 0.5) < edge_min:
            continue
        n += 1
        wins += y == 1
        profit += PROFIT_110 if y == 1 else -1.0
        edge_pp_sum += (p - BREAKEVEN_110) * 100.0
    return {
        "n_bets": n,
        "wins": wins,
        "losses": n - wins,
        "profit_u": round(profit, 2),
        "roi": round(profit / n, 4) if n else 0.0,
        "win_rate": round(wins / n, 4) if n else 0.0,
        "avg_edge_pp": round(edge_pp_sum / n, 2) if n else 0.0,
    }


def score_task(rows, base_p):
    """rows: list of (p, y). base_p: baseline probability or None."""
    if not rows:
        return {"n": 0}
    ps = [p for p, _ in rows]
    ys = [y for _, y in rows]
    m = {
        "n": len(rows),
        "brier": round(brier(ps, ys), 4),
        "log_loss": round(log_loss(ps, ys), 4),
        "calibration_slope": calibration_slope(ps, ys),
    }
    if base_p is not None:
        bs = [base_p] * len(rows)
        m["base_brier"] = round(brier(bs, ys), 4)
        m["base_log_loss"] = round(log_loss(bs, ys), 4)
        m["beats_baseline"] = (m["log_loss"] < m["base_log_loss"]
                               and m["brier"] < m["base_brier"])
    if m["calibration_slope"] is not None:
        m["calibration_slope"] = round(m["calibration_slope"], 3)
    return m


def run(family_name, test_start, test_end, edge_min=0.03, fetch=False,
        cache_path=None):
    if fetch:
        print("fetching nflverse data...", flush=True)
        data_mod.fetch(force=True)
    games = data_mod.load_games() if cache_path is None else \
        data_mod.load_games(cache_path)
    print("games: %d (%d-%d)" % (len(games), games[0]["season"],
                                 games[-1]["season"]), flush=True)
    teams = sorted({g["home"] for g in games} | {g["away"] for g in games})
    fam = get_family(family_name)(teams)

    by_season: dict[int, list] = {}
    for g in games:
        by_season.setdefault(g["season"], []).append(g)

    per_season = {}
    pooled = {"win": [], "spread": [], "total": []}
    pooled_bets = {"spread": [], "total": []}
    home_wins = 0
    home_n = 0

    for season in sorted(by_season):
        season_games = by_season[season]
        if season < test_start:
            for g in season_games:
                fam.observe(g)
                if g["home_score"] != g["away_score"]:
                    home_n += 1
                    home_wins += g["home_score"] > g["away_score"]
            continue
        if season > test_end:
            for g in season_games:
                fam.observe(g)
            continue
        # Test season: freeze fitted context from prior seasons only.
        fam.fit_context()
        base_win = (home_wins / home_n) if home_n else 0.57
        s_rows = {"win": [], "spread": [], "total": []}
        s_bets = {"spread": [], "total": []}
        for g in season_games:
            pred = fam.predict(g)
            hs, aws = g["home_score"], g["away_score"]
            if pred is not None:
                if hs != aws:
                    p = pred["win_prob_home"]
                    y = 1 if hs > aws else 0
                    s_rows["win"].append((p, y))
                    pooled["win"].append((p, y))
                sm, st = fam.sm, fam.st
                if g["spread_line"] is not None and sm:
                    margin = hs - aws
                    if abs(margin - g["spread_line"]) > 1e-9:  # not a push
                        p = cover_prob_home(pred["pred_margin_home"],
                                            g["spread_line"], sm)
                        y = 1 if margin - g["spread_line"] > 0 else 0
                        s_rows["spread"].append((p, y))
                        pooled["spread"].append((p, y))
                        s_bets["spread"].append((p, y))
                        pooled_bets["spread"].append((p, y))
                if g["total_line"] is not None and st:
                    total = hs + aws
                    if abs(total - g["total_line"]) > 1e-9:
                        p = over_prob(pred["pred_total"], g["total_line"], st)
                        y = 1 if total - g["total_line"] > 0 else 0
                        s_rows["total"].append((p, y))
                        pooled["total"].append((p, y))
                        s_bets["total"].append((p, y))
                        pooled_bets["total"].append((p, y))
            fam.observe(g)
            if hs != aws:
                home_n += 1
                home_wins += hs > aws
        per_season[season] = {
            task: {**score_task(s_rows[task],
                               base_win if task == "win" else 0.5),
                   **({"bets": simulate_bets(s_bets[task], edge_min)}
                      if task in s_bets else {})}
            for task in ("win", "spread", "total")
        }

    pooled_metrics = {
        task: score_task(pooled[task],
                         (home_wins / home_n) if task == "win" and home_n
                         else (0.5 if task != "win" else None))
        for task in ("win", "spread", "total")
    }
    for task in ("spread", "total"):
        pooled_metrics[task]["bets"] = simulate_bets(pooled_bets[task],
                                                     edge_min)
    return {
        "family": family_name,
        "ran_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "test_window": [test_start, test_end],
        "edge_min": edge_min,
        "games": len(games),
        "data": "nflverse schedules/games.csv.gz (spread_line/total_line are "
                "consensus lines, not book-specific closes; no moneylines)",
        "per_season": per_season,
        "pooled": pooled_metrics,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", default="elo-v1",
                    help="one of %s" % family_names())
    ap.add_argument("--test-start", type=int, default=2010)
    ap.add_argument("--test-end", type=int, default=2024)
    ap.add_argument("--edge-min", type=float, default=0.03)
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    result = run(args.family, args.test_start, args.test_end,
                 edge_min=args.edge_min, fetch=args.fetch)
    out = args.out or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "results",
        "replay-%s-%d-%d.json" % (args.family, args.test_start,
                                  args.test_end))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(result, f, indent=1, sort_keys=True)

    p = result["pooled"]
    print("family=%s seasons=%d-%d edge_min=%.2f"
          % (args.family, args.test_start, args.test_end, args.edge_min))
    for task in ("win", "spread", "total"):
        m = p[task]
        line = ("%-6s n=%5d brier=%.4f ll=%.4f cal=%.2f %s" % (
            task, m["n"], m["brier"], m["log_loss"],
            m["calibration_slope"]
            if m["calibration_slope"] is not None else float("nan"),
            "BEATS" if m.get("beats_baseline") else "no-beat"))
        if "bets" in m:
            b = m["bets"]
            line += (" | bets n=%d w=%d roi=%+.3f edge=%+.2fpp"
                     % (b["n_bets"], b["wins"], b["roi"], b["avg_edge_pp"]))
        print(line)
    print("wrote", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
