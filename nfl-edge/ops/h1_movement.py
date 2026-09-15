"""H1: Inter-Window Market-Movement Prediction (revised 2026-09-15).

Prereg: docs/preregistrations/ncaaf-h1-movement.md
- Primary: next-window consensus movement (early->mid, mid->late)
- Secondary: movement-to-close (early->late)
- Rolling week-level origins, week-clustered SEs, season stability.

Stage 1 only. Predicts movement, does not bet.
"""
import argparse, json, os, sys, statistics, random, math
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_outcomes as mo
import market_structure as ms

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"

def ols_fit(X, y):
    """OLS coefficients via normal equations. X includes intercept."""
    n, p = len(X), len(X[0])
    XtX = [[0.0]*p for _ in range(p)]
    Xty = [0.0]*p
    for i in range(n):
        for j in range(p):
            Xty[j] += X[i][j] * y[i]
            for k in range(p):
                XtX[j][k] += X[i][j] * X[i][k]
    # Invert via Gauss-Jordan
    aug = [XtX[i] + [1.0 if i==j else 0.0 for j in range(p)] for i in range(p)]
    for col in range(p):
        piv = max(range(col, p), key=lambda r: abs(aug[r][col]))
        aug[col], aug[piv] = aug[piv], aug[col]
        d = aug[col][col]
        if abs(d) < 1e-12:
            return None
        for j in range(2*p):
            aug[col][j] /= d
        for r in range(p):
            if r != col:
                f = aug[r][col]
                for j in range(2*p):
                    aug[r][j] -= f * aug[col][j]
    inv = [row[p:] for row in aug]
    return [sum(inv[j][k] * Xty[k] for k in range(p)) for j in range(p)]

def predict(X, beta):
    return [sum(xj*bj for xj, bj in zip(xi, beta)) for xi in X]

def r2_score(y, y_pred):
    n = len(y)
    if n == 0: return None
    ym = sum(y)/n
    ss_tot = sum((yi-ym)**2 for yi in y)
    if ss_tot == 0: return 0.0
    ss_res = sum((yi-yp)**2 for yi, yp in zip(y, y_pred))
    return 1 - ss_res/ss_tot

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--games", required=True)
    ap.add_argument("--lines", required=True)
    ap.add_argument("--out", default="/tmp/h1_movement.json")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n_perm", type=int, default=499)
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]
    random.seed(args.seed)

    games_idx = {}
    for gp in args.games.split(","):
        for key, glist in mo.load_games(gp.strip()).items():
            games_idx.setdefault(key, []).extend(glist)
    lines_by_id = {}
    for lp in args.lines.split(","):
        lines_by_id.update(mo.load_lines(lp.strip()))

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

    # Per (eid, season, week, market): {window: [(book, line)]}
    games = {}
    for (eid, season, week, window, book, market), v in selected.items():
        if season not in seasons: continue
        if market not in ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL"): continue
        games.setdefault((eid, season, week, market), {}).setdefault(
            window, []).append((book, v["line"]))

    FEATURES = ["line_t", "dispersion_t", "range_t", "pinnacle_dev_t", "n_books_t"]
    
    rows = []  # each: (season, week, market, transition, features, movement)
    n_no_sign = n_not_fbs = n_outlier = 0
    
    for (eid, season, week, market), by_win in games.items():
        m = matched.get((eid, season))
        if not m: continue
        game = m["game"]
        if mo.matchup_class(game) != "FBSvFBS":
            n_not_fbs += 1
            continue
        
        # Transitions: (early->mid), (mid->late) for primary; (early->late) secondary
        for t_from, t_to, trans in [("early","mid","next"), ("mid","late","next"),
                                    ("early","late","to_close")]:
            if t_from not in by_win or t_to not in by_win:
                continue
            bl_from = by_win[t_from]
            bl_to = by_win[t_to]
            
            lines_from = [ln for _, ln in bl_from]
            lines_to = [ln for _, ln in bl_to]
            cons_from = statistics.median(lines_from)
            cons_to = statistics.median(lines_to)
            
            disp = statistics.stdev(lines_from) if len(lines_from) > 1 else 0.0
            rng = max(lines_from) - min(lines_from)
            n_books = len(bl_from)
            pinn = [ln for b, ln in bl_from if b == "pinnacle"]
            pinn_dev = (pinn[0] - cons_from) if pinn else 0.0
            
            # Outlier cleaning (F1 rule)
            if market == "FULL_GAME_SPREAD" and rng > 10:
                n_outlier += 1
                continue
            if market == "FULL_GAME_TOTAL" and rng > 15:
                n_outlier += 1
                continue
            
            # Signed movement
            if market == "FULL_GAME_SPREAD":
                gid = game.get("id")
                cfbd = lines_by_id.get(gid)
                if cfbd is None:
                    n_no_sign += 1
                    continue
                home_fav = (cfbd > 0) if m["swapped"] else (cfbd < 0)
                # Signed lines from home perspective
                s_from = -abs(cons_from) if home_fav else abs(cons_from)
                s_to = -abs(cons_to) if home_fav else abs(cons_to)
                move = s_to - s_from
                line_t = s_from
            else:
                move = cons_to - cons_from
                line_t = cons_from
            
            rows.append({
                "season": season, "week": week, "market": market,
                "transition": trans,
                "line_t": line_t, "dispersion_t": disp, "range_t": rng,
                "pinnacle_dev_t": pinn_dev, "n_books_t": n_books,
                "movement": move,
            })

    # --- Rolling week-level OOS ---
    # Train weeks 1..k, predict k+1..k+2, for k=4..12. Pool across seasons
    # but track season separately for stability.
    def run_oos(rows, features, target="movement"):
        # Group by (season, week)
        by_sw = {}
        for r in rows:
            by_sw.setdefault((r["season"], r["week"]), []).append(r)
        
        oos_preds = []  # (y_true, y_pred, season, week)
        fold_r2 = []
        
        # Rolling origins: for each season, k=4..12
        for season in sorted(set(r["season"] for r in rows)):
            weeks = sorted(set(r["week"] for r in rows if r["season"]==season))
            for k in weeks:
                if k < 4 or k > 12: continue
                train = [r for r in rows if r["season"]==season and r["week"] <= k]
                test = [r for r in rows if r["season"]==season and r["week"] in (k+1, k+2)]
                if len(train) < 20 or len(test) < 5: continue
                
                Xtr = [[1.0]+[r[f] for f in features] for r in train]
                ytr = [r[target] for r in train]
                beta = ols_fit(Xtr, ytr)
                if beta is None: continue
                
                Xte = [[1.0]+[r[f] for f in features] for r in test]
                yte = [r[target] for r in test]
                ypr = predict(Xte, beta)
                
                r2 = r2_score(yte, ypr)
                if r2 is not None:
                    fold_r2.append(r2)
                    for yt, yp in zip(yte, ypr):
                        oos_preds.append((yt, yp, season, k+1))
        
        # Pooled OOS R²
        if oos_preds:
            yt_all = [p[0] for p in oos_preds]
            yp_all = [p[1] for p in oos_preds]
            pooled_r2 = r2_score(yt_all, yp_all)
        else:
            pooled_r2 = None
        
        # Season stability: OOS R² by season
        by_season = {}
        for season in sorted(set(p[2] for p in oos_preds)):
            yt_s = [p[0] for p in oos_preds if p[2]==season]
            yp_s = [p[1] for p in oos_preds if p[2]==season]
            by_season[str(season)] = round(r2_score(yt_s, yp_s), 4) if yt_s else None
        
        # Directional accuracy (sign agreement)
        if oos_preds:
            dir_acc = sum(1 for yt, yp, _, _ in oos_preds 
                         if (yt > 0) == (yp > 0) and yt != 0) / len(oos_preds)
        else:
            dir_acc = None
        
        return {
            "pooled_oos_r2": round(pooled_r2, 4) if pooled_r2 is not None else None,
            "mean_fold_r2": round(statistics.mean(fold_r2), 4) if fold_r2 else None,
            "n_folds": len(fold_r2),
            "n_oos": len(oos_preds),
            "by_season": by_season,
            "directional_accuracy": round(dir_acc, 4) if dir_acc else None,
        }

    # Primary: next-window movement. Secondary: to_close.
    spread_next = [r for r in rows if r["market"]=="FULL_GAME_SPREAD" and r["transition"]=="next"]
    total_next = [r for r in rows if r["market"]=="FULL_GAME_TOTAL" and r["transition"]=="next"]
    spread_close = [r for r in rows if r["market"]=="FULL_GAME_SPREAD" and r["transition"]=="to_close"]
    total_close = [r for r in rows if r["market"]=="FULL_GAME_TOTAL" and r["transition"]=="to_close"]
    
    res = {
        "spread_next": run_oos(spread_next, FEATURES),
        "total_next": run_oos(total_next, FEATURES),
        "spread_to_close": run_oos(spread_close, FEATURES),
        "total_to_close": run_oos(total_close, FEATURES),
    }
    
    # Permutation test on primary (spread_next pooled OOS R²)
    # Shuffle movement within (season, week), recompute OOS R²
    obs_r2 = res["spread_next"]["pooled_oos_r2"] or 0
    perm_r2s = []
    for _ in range(args.n_perm):
        perm_rows = []
        # Shuffle within (season, week) strata
        by_sw = {}
        for r in spread_next:
            by_sw.setdefault((r["season"], r["week"]), []).append(r)
        for key, grp in by_sw.items():
            moves = [r["movement"] for r in grp]
            random.shuffle(moves)
            for r, pm in zip(grp, moves):
                perm_rows.append(dict(r, movement=pm))
        pr = run_oos(perm_rows, FEATURES)
        if pr["pooled_oos_r2"] is not None:
            perm_r2s.append(pr["pooled_oos_r2"])
    p_val = (sum(1 for pr in perm_r2s if pr >= obs_r2) + 1) / (len(perm_r2s) + 1)

    result = {
        "preregistration": "docs/preregistrations/ncaaf-h1-movement.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "scope": {"sport": args.sport, "seasons": seasons, "universe": "FBSvFBS"},
        "n_rows": len(rows),
        "n_no_sign": n_no_sign,
        "n_not_fbs": n_not_fbs,
        "n_outlier": n_outlier,
        "primary": {
            "spread_next_window": res["spread_next"],
            "total_next_window": res["total_next"],
        },
        "secondary": {
            "spread_to_close": res["spread_to_close"],
            "total_to_close": res["total_to_close"],
        },
        "permutation_test": {
            "target": "spread next-window pooled OOS R²",
            "observed_r2": round(obs_r2, 4),
            "p_value": round(p_val, 4),
            "n_perm": args.n_perm,
        },
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {args.out}")
    print(f"Rows: {len(rows)}")
    print(f"Spread next-window OOS R²: {res['spread_next']['pooled_oos_r2']}, p={p_val:.4f}")
    print(f"Total next-window OOS R²: {res['total_next']['pooled_oos_r2']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
