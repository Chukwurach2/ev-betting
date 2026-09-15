"""H3: Football-Information Residual Model (revised 2026-09-15).

Prereg: docs/preregistrations/ncaaf-h3-residual-model.md
- Elo mechanics FROZEN (challenger/elo.py: k=0.15, hfa=1.2, league_avg=22.0).
- 2019-21 = burn-in for initialization ONLY. Not for tuning or evaluation.
- Sensitivity: (a) zero-init, (b) 2020-21 only burn-in.
- Primary: OLS residual ~ elo_edge (spread), rolling week-level OOS,
  week-clustered SEs.

Stage 1 only. Tests if Elo adds information beyond the market line.
"""
import argparse, json, os, sys, statistics
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "..", "model", "challenger"))
import market_outcomes as mo
import market_structure as ms
from elo import init_ratings, update_ratings, expected_scores

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"
# Frozen Elo mechanics (from challenger/elo.py, NOT tuned on 2022-24)
ELO_K = 0.15
ELO_HFA = 1.2
ELO_LEAGUE_AVG = 22.0

def load_games_list(paths):
    games = []
    for p in paths:
        games.extend(json.load(open(p.strip())))
    # Sort chronologically
    games.sort(key=lambda g: (g.get("startDate") or "", g.get("id", 0)))
    return games

def build_elo(games, burnin_years):
    """Run Elo chronologically. Returns dict game_id -> pre-game (pred_margin, pred_total).
    Only games in burnin_years + 2022-24 are processed. Ratings are point-in-time.
    """
    teams = set()
    for g in games:
        if g.get("homeTeam"): teams.add(g["homeTeam"])
        if g.get("awayTeam"): teams.add(g["awayTeam"])
    off, deff = init_ratings(teams)
    
    pregame = {}  # game_id -> (pred_margin_home, pred_total)
    for g in games:
        if not g.get("completed"): continue
        hs, aws = g.get("homePoints"), g.get("awayPoints")
        if hs is None or aws is None: continue
        home, away = g["homeTeam"], g["awayTeam"]
        if home not in off or away not in off: continue
        
        # Record pre-game prediction (for 2022-24 games)
        if g["season"] >= 2022:
            exp_h, exp_a = expected_scores(off, deff, home, away,
                                          league_avg=ELO_LEAGUE_AVG, hfa=ELO_HFA)
            pregame[g["id"]] = (exp_h - exp_a, exp_h + exp_a)
        
        # Update ratings
        update_ratings(off, deff, home, away, hs, aws,
                       league_avg=ELO_LEAGUE_AVG, hfa=ELO_HFA, k=ELO_K)
    return pregame

def ols_single(X, y):
    """OLS for y ~ a + b*X. Returns (a, b, se_b_clustered, n)."""
    n = len(X)
    if n < 10: return None
    mx = sum(X)/n
    my = sum(y)/n
    sxx = sum((xi-mx)**2 for xi in X)
    if sxx == 0: return None
    sxy = sum((xi-mx)*(yi-my) for xi, yi in zip(X, y))
    b = sxy/sxx
    a = my - b*mx
    return a, b, n

def clustered_se(X, y, a, b, clusters):
    """Week-clustered SE for slope b in y ~ a + b*X."""
    n = len(X)
    mx = sum(X)/n
    sxx = sum((xi-mx)**2 for xi in X)
    if sxx == 0: return None
    
    # Clustered variance: sum over clusters of (sum of scores)^2
    # Score for obs i: x_tilde_i * e_i, where x_tilde = X - mean(X), e = residual
    by_cluster = {}
    for xi, yi, c in zip(X, y, clusters):
        e = yi - (a + b*xi)
        s = (xi - mx) * e
        by_cluster.setdefault(c, []).append(s)
    
    meat = sum(sum(scores)**2 for scores in by_cluster.values())
    # Degrees of freedom adjustment: G/(G-1) where G = n_clusters
    G = len(by_cluster)
    if G < 2: return None
    var_b = (G/(G-1)) * meat / (sxx**2)
    return (var_b)**0.5 if var_b > 0 else 0.0

def run_oos(rows, target="spread"):
    """Rolling week-level OOS. rows: list of dicts with season, week,
    elo_edge, residual. Returns OOS R², coefficient stability."""
    oos_preds = []  # (y_true, y_pred, season, week)
    coefs = []  # (season, b, se_b)
    fold_r2 = []
    
    for season in sorted(set(r["season"] for r in rows)):
        weeks = sorted(set(r["week"] for r in rows if r["season"]==season))
        for k in weeks:
            if k < 4 or k > 12: continue
            train = [r for r in rows if r["season"]==season and r["week"] <= k]
            test = [r for r in rows if r["season"]==season and r["week"] in (k+1, k+2)]
            if len(train) < 20 or len(test) < 5: continue
            
            Xtr = [r["elo_edge"] for r in train]
            ytr = [r["residual"] for r in train]
            res = ols_single(Xtr, ytr)
            if res is None: continue
            a, b, _ = res
            
            # Clustered SE on training data
            ctr = [(r["season"], r["week"]) for r in train]
            se_b = clustered_se(Xtr, ytr, a, b, ctr)
            
            Xte = [r["elo_edge"] for r in test]
            yte = [r["residual"] for r in test]
            ypr = [a + b*xi for xi in Xte]
            
            # OOS R² vs null (predict 0 residual = market is right)
            ss_tot = sum(yi**2 for yi in yte)  # null predicts 0
            ss_res = sum((yi-yp)**2 for yi, yp in zip(yte, ypr))
            r2 = 1 - ss_res/ss_tot if ss_tot > 0 else 0.0
            fold_r2.append(r2)
            
            coefs.append((season, b, se_b))
            for yt, yp in zip(yte, ypr):
                oos_preds.append((yt, yp, season))
    
    # Pooled OOS R²
    if oos_preds:
        yt_all = [p[0] for p in oos_preds]
        yp_all = [p[1] for p in oos_preds]
        ss_tot = sum(yi**2 for yi in yt_all)
        ss_res = sum((yi-yp)**2 for yi, yp in zip(yt_all, yp_all))
        pooled_r2 = 1 - ss_res/ss_tot if ss_tot > 0 else 0.0
    else:
        pooled_r2 = None
    
    # By season
    by_season = {}
    for season in sorted(set(p[2] for p in oos_preds)):
        yt_s = [p[0] for p in oos_preds if p[2]==season]
        yp_s = [p[1] for p in oos_preds if p[2]==season]
        ss_tot = sum(yi**2 for yi in yt_s)
        ss_res = sum((yi-yp)**2 for yi, yp in zip(yt_s, yp_s))
        by_season[str(season)] = round(1 - ss_res/ss_tot, 4) if ss_tot > 0 else None
    
    # Coefficient stability by season (mean b per season)
    coef_by_season = {}
    for season in sorted(set(c[0] for c in coefs)):
        bs = [c[1] for c in coefs if c[0]==season]
        coef_by_season[str(season)] = round(statistics.mean(bs), 4) if bs else None
    
    return {
        "pooled_oos_r2": round(pooled_r2, 4) if pooled_r2 is not None else None,
        "mean_fold_r2": round(statistics.mean(fold_r2), 4) if fold_r2 else None,
        "n_folds": len(fold_r2),
        "n_oos": len(oos_preds),
        "by_season_r2": by_season,
        "coef_by_season": coef_by_season,
    }

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--games", required=True)  # 2022-24 games
    ap.add_argument("--burnin-games", required=True)  # 2019-21 games
    ap.add_argument("--lines", required=True)  # CFBD lines (sign oracle)
    ap.add_argument("--out", default="/tmp/h3_residual.json")
    ap.add_argument("--sensitivity", choices=["full", "zero_init", "burnin_2020_21"],
                    default="full")
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]

    # Load games
    games_2224 = load_games_list(args.games.split(","))
    burnin_games = load_games_list(args.burnin_games.split(","))
    
    # Build Elo based on sensitivity mode
    if args.sensitivity == "full":
        all_games = burnin_games + games_2224
        all_games.sort(key=lambda g: (g.get("startDate") or "", g.get("id", 0)))
        pregame = build_elo(all_games, [2019, 2020, 2021])
    elif args.sensitivity == "zero_init":
        pregame = build_elo(games_2224, [])
    elif args.sensitivity == "burnin_2020_21":
        b2020_21 = [g for g in burnin_games if g["season"] >= 2020]
        all_games = b2020_21 + games_2224
        all_games.sort(key=lambda g: (g.get("startDate") or "", g.get("id", 0)))
        pregame = build_elo(all_games, [2020, 2021])
    
    print(f"Elo pregame predictions: {len(pregame)}", file=sys.stderr)

    # Load CFBD lines (sign oracle)
    lines_by_id = {}
    for lp in args.lines.split(","):
        lines_by_id.update(mo.load_lines(lp.strip()))
    
    # Build game index for matching
    games_idx = {}
    for gp in args.games.split(","):
        for key, glist in mo.load_games(gp.strip()).items():
            games_idx.setdefault(key, []).extend(glist)

    # Get market lines from DB (late window preferred)
    plan = ms.build_plan(args.sport, seasons)
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
    events = {}
    for (eid, season, week, window, book, market), v in selected.items():
        events.setdefault((eid, season),
                          (v["home"], v["away"], mo.parse_ts(v["kickoff"])))
    matched, _ = mo.match_events(events, games_idx)

    # Per (eid, season, week, market): {window: [lines]}
    games = {}
    for (eid, season, week, window, book, market), v in selected.items():
        if season not in seasons: continue
        if market not in ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL"): continue
        games.setdefault((eid, season, week, market), {}).setdefault(
            window, []).append(v["line"])

    spread_rows = []
    total_rows = []
    n_no_sign = n_no_elo = n_not_fbs = 0
    
    for (eid, season, week, market), by_win in games.items():
        m = matched.get((eid, season))
        if not m: continue
        game = m["game"]
        if mo.matchup_class(game) != "FBSvFBS":
            n_not_fbs += 1
            continue
        
        # Use latest available window (late > mid > early)
        lines = None
        for w in ["late", "mid", "early"]:
            if w in by_win:
                lines = by_win[w]
                break
        if not lines: continue
        
        cons = statistics.median(lines)
        gid = game.get("id")
        
        # Elo prediction
        if gid not in pregame:
            n_no_elo += 1
            continue
        pred_margin, pred_total = pregame[gid]
        
        # Actual scores
        hs = game.get("homePoints")
        aws = game.get("awayPoints")
        if hs is None or aws is None: continue
        # Handle swapped orientation
        if m["swapped"]:
            hs, aws = aws, hs
        
        if market == "FULL_GAME_SPREAD":
            cfbd = lines_by_id.get(gid)
            if cfbd is None:
                n_no_sign += 1
                continue
            home_fav = (cfbd > 0) if m["swapped"] else (cfbd < 0)
            # Signed market line from home perspective
            mkt_signed = -abs(cons) if home_fav else abs(cons)
            # Elo edge: how much Elo disagrees with market
            elo_edge = pred_margin - (-mkt_signed)
            # Residual: actual minus market-implied
            actual_margin = hs - aws
            residual = actual_margin - (-mkt_signed)
            spread_rows.append({
                "season": season, "week": week,
                "elo_edge": elo_edge, "residual": residual,
            })
        else:
            elo_edge = pred_total - cons
            actual_total = hs + aws
            residual = actual_total - cons
            total_rows.append({
                "season": season, "week": week,
                "elo_edge": elo_edge, "residual": residual,
            })

    print(f"Spread rows: {len(spread_rows)}, Total rows: {len(total_rows)}",
          file=sys.stderr)
    
    spread_res = run_oos(spread_rows, "spread")
    total_res = run_oos(total_rows, "total")
    
    # Primary test: t-test on elo_edge coefficient (spread)
    # Use pooled coefficient from full-sample OLS with clustered SEs
    X = [r["elo_edge"] for r in spread_rows]
    y = [r["residual"] for r in spread_rows]
    clusters = [(r["season"], r["week"]) for r in spread_rows]
    res = ols_single(X, y)
    if res:
        a, b, n = res
        se_b = clustered_se(X, y, a, b, clusters)
        t_stat = b / se_b if se_b and se_b > 0 else 0
        # Two-sided p (normal approx)
        import math
        p_val = 2 * (1 - 0.5 * (1 + math.erf(abs(t_stat) / math.sqrt(2))))
    else:
        b, se_b, t_stat, p_val = None, None, None, None

    result = {
        "preregistration": "docs/preregistrations/ncaaf-h3-residual-model.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "sensitivity": args.sensitivity,
        "elo_mechanics": {"k": ELO_K, "hfa": ELO_HFA, "league_avg": ELO_LEAGUE_AVG,
                          "frozen": True},
        "scope": {"sport": args.sport, "seasons": seasons, "universe": "FBSvFBS"},
        "n_spread": len(spread_rows),
        "n_total": len(total_rows),
        "n_no_sign": n_no_sign,
        "n_no_elo": n_no_elo,
        "n_not_fbs": n_not_fbs,
        "spread_model": spread_res,
        "total_model": total_res,
        "primary_test": {
            "coefficient": round(b, 4) if b is not None else None,
            "clustered_se": round(se_b, 4) if se_b else None,
            "t_stat": round(t_stat, 3) if t_stat else None,
            "p_value": round(p_val, 4) if p_val else None,
            "n": n if res else 0,
        },
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {args.out}")
    print(f"Spread coef: {b:.4f} (se={se_b:.4f}, t={t_stat:.2f}, p={p_val:.4f})")
    print(f"Spread OOS R²: {spread_res['pooled_oos_r2']}")
    print(f"Total OOS R²: {total_res['pooled_oos_r2']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
