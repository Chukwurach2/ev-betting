"""NCAAF F3 — Market Baseline Completion.

Prereg: docs/preregistrations/ncaaf-market-baseline-f3.md
- Favorite/underdog spread cover rates (CFBD sign oracle)
- Totals calibration (fine bins)
- Baseline edge distribution (|p - 0.5|)

Descriptive only. No strategy, no picks, no 2025.
"""
import argparse, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_outcomes as mo
import market_structure as ms

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"

def edge_dist(edges):
    """Distribution of |p - 0.5| (market-implied absolute edge)."""
    edges = sorted(edges)
    n = len(edges)
    if n == 0:
        return {}
    return {
        "n": n,
        "mean_abs_edge": round(sum(edges) / n, 4),
        "p50": round(edges[n // 2], 4),
        "p90": round(edges[int(n * 0.9)], 4),
        "p99": round(edges[int(n * 0.99)], 4),
        "frac_gt_024": round(sum(1 for e in edges if e > 0.024) / n, 4),
        "frac_gt_05": round(sum(1 for e in edges if e > 0.05) / n, 4),
        "frac_gt_10": round(sum(1 for e in edges if e > 0.10) / n, 4),
    }

def summarize(rows):
    if not rows:
        return {"rate": None, "n": 0}
    return {"rate": round(sum(r[0] for r in rows) / len(rows), 4),
            "n": len(rows)}

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--games", required=True)
    ap.add_argument("--lines", required=True,
                    help="CFBD pre-game lines for sign oracle (A4 method)")
    ap.add_argument("--out", default="/tmp/market_baseline_f3.json")
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]

    games_idx = {}
    for gp in args.games.split(","):
        for key, glist in mo.load_games(gp.strip()).items():
            games_idx.setdefault(key, []).extend(glist)
    lines_by_id = {}
    for lp in args.lines.split(","):
        lines_by_id.update(mo.load_lines(lp.strip()))

    plan = ms.build_plan(args.sport, seasons)
    expected_snapshots = len(plan)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    hq = ms.sports.historical_quotes_table(args.sport)
    cols = ("provider_event_id, home_team, away_team, kickoff, book_key,"
            " market, selection, line, american_odds, fair_probability,"
            " observed_at")
    with psycopg.connect(dsn) as conn:
        qrows = conn.execute(f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "home_team", "away_team", "kickoff",
         "book_key", "market", "selection", "line", "american_odds",
         "fair_probability", "observed_at"], r)) for r in qrows]

    selected, integrity, matched_instants = ms.select_pairs(recs, plan, seasons)
    freeze_ok = len(matched_instants) == expected_snapshots

    events = {}
    for (eid, season, week, window, book, market), v in selected.items():
        events.setdefault((eid, season),
                          (v["home"], v["away"], mo.parse_ts(v["kickoff"])))
    matched, match_integrity = mo.match_events(events, games_idx)

    # --- F3 Analysis 1: Favorite/underdog spread covers ---
    # Build cross-book consensus per (eid, season, window, market) like
    # mo.analyze(), then split by home favorite vs home dog (CFBD oracle).
    import statistics
    cons = {}
    for (eid, season, week, window, book, market), v in selected.items():
        cons.setdefault((eid, season, window, market),
                        {"lines": [], "probs": []})
        c = cons[(eid, season, window, market)]
        c["lines"].append(v["line"])
        c["probs"].append(v["fair_prob"])

    fav_rows = []  # (cover, season, window)
    dog_rows = []
    n_no_sign = 0
    for (eid, season, window, market), c in cons.items():
        if market != "FULL_GAME_SPREAD" or season not in seasons:
            continue
        m = matched.get((eid, season))
        if not m:
            continue
        game = m["game"]
        if game.get("completed") is not True:
            continue
        gid = game.get("id")
        cfbd_spread = lines_by_id.get(gid)
        if cfbd_spread is None:
            n_no_sign += 1
            continue
        # A4 sign logic (same as mo.analyze)
        if m["swapped"]:
            hp, ap_ = game["awayPoints"], game["homePoints"]
            home_favored = cfbd_spread > 0
        else:
            hp, ap_ = game["homePoints"], game["awayPoints"]
            home_favored = cfbd_spread < 0
        if hp is None or ap_ is None:
            continue
        home_margin = hp - ap_
        line = abs(statistics.median(c["lines"]))
        signed = -line if home_favored else line
        diff = home_margin + signed
        if diff == 0:
            continue  # push
        cover = 1.0 if diff > 0 else 0.0
        (fav_rows if home_favored else dog_rows).append((cover, season, window))

    def summ(rows):
        return summarize(rows)

    fav_dog = {
        "home_favorite": summ(fav_rows),
        "home_underdog": summ(dog_rows),
        "by_season": {},
    }
    for s in seasons:
        fav_dog["by_season"][str(s)] = {
            "home_favorite": summ([r for r in fav_rows if r[1] == s]),
            "home_underdog": summ([r for r in dog_rows if r[1] == s]),
        }

    # --- F3 Analysis 2: Totals calibration (fine bins) ---
    # Consensus per (eid, season, window), like mo.analyze().
    total_rows = []  # (fair_p, over, window)
    for (eid, season, window, market), c in cons.items():
        if market != "FULL_GAME_TOTAL" or season not in seasons:
            continue
        m = matched.get((eid, season))
        if not m:
            continue
        game = m["game"]
        if game.get("completed") is not True:
            continue
        if m["swapped"]:
            hp, ap_ = game["awayPoints"], game["homePoints"]
        else:
            hp, ap_ = game["homePoints"], game["awayPoints"]
        if hp is None or ap_ is None:
            continue
        total_pts = hp + ap_
        line = statistics.median(c["lines"])
        p = statistics.median(c["probs"])  # P(Over), ref_sel="Over"
        if total_pts == line:
            continue  # push
        over = 1.0 if total_pts > line else 0.0
        total_rows.append((p, over, window))

    bins = [(0.35, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.65)]
    totals_cal = {}
    for lo, hi in bins:
        br = [(p, o, w) for p, o, w in total_rows if lo <= p < hi]
        totals_cal[f"[{lo},{hi})"] = {
            "mean_p": round(sum(p for p, _, _ in br) / len(br), 4) if br else None,
            "rate": round(sum(o for _, o, _ in br) / len(br), 4) if br else None,
            "n": len(br),
        }

    # --- F3 Analysis 3: Baseline edge distribution ---
    # |p - 0.5| for consensus probabilities (no outcome needed).
    # Question: how often does the market itself imply a meaningful edge?
    edge_spread = []
    edge_total = []
    for (eid, season, window, market), c in cons.items():
        if season not in seasons:
            continue
        p = statistics.median(c["probs"])
        e = abs(p - 0.5)
        if market == "FULL_GAME_SPREAD":
            edge_spread.append(e)
        elif market == "FULL_GAME_TOTAL":
            edge_total.append(e)

    result = {
        "preregistration": "docs/preregistrations/ncaaf-market-baseline-f3.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "scope": {"sport": args.sport, "seasons": seasons},
        "freeze_check": {"matched_snapshots": len(matched_instants),
                         "expected_snapshots": expected_snapshots,
                         "pass": freeze_ok},
        "integrity": {"matching": match_integrity},
        "n_spread_no_sign": n_no_sign,
        "favorite_underdog": fav_dog,
        "totals_calibration_fine": totals_cal,
        "edge_distribution": {
            "FULL_GAME_SPREAD": edge_dist(edge_spread),
            "FULL_GAME_TOTAL": edge_dist(edge_total),
        },
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {args.out}")
    print(f"Freeze: {len(matched_instants)}/{expected_snapshots}")
    print(f"Fav: {fav_dog['home_favorite']}, Dog: {fav_dog['home_underdog']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
