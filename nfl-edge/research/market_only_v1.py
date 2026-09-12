"""Market-only v1: predict the closing consensus from early-week prices.

SHADOW research only. Preregistered in
docs/preregistrations/market-only-v1.md (committed before real-data
execution). A positive result selects the model as a FORWARD-SHADOW
candidate only; it never promotes anything.

Design (see preregistration for the full contract):
- Unit: (canonical game, market). Snapshot slots per game from kickoff K:
  close = latest snapshot < K (require K - close <= 72h);
  sat   = latest snapshot <= close - 24h;
  wed   = latest snapshot <= close - 96h. All features strictly pre-close.
- Analysis line: modal line across books at close (deterministic tie-break).
  Consensus at each slot: median multiplicative no-vig fair prob across
  books quoting exactly that line (>= 2 books, complete-case).
- Model: Ridge(alpha=1.0, fixed) on standardized features, leave-one-
  season-out over {2022, 2023, 2024}.
- Primary: one-sided paired test (Diebold-Mariano form) of
  SE_model - SE_carry_sat < 0, alpha = 0.05.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.devig_tournament import canonical_game_keys  # noqa: E402
from research.market_alpha import (  # noqa: E402
    _as_utc,
    _load_quotes_csv,
    build_game_series,
    DATASET_FINGERPRINT,
    EXPECTED_QUOTES,
    EXPECTED_SNAPSHOTS,
    MARKETS,
    PINNACLE,
    SPREAD,
    TOTAL,
)

try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None

PREREG_PATH = "docs/preregistrations/market-only-v1.md"
ALPHA = 0.05
RIDGE_ALPHA = 1.0
MIN_UNITS = 200
PRACTICAL_RMSE_GAIN = 0.003

FEATURES = ["f_wed", "f_sat", "move", "pinn_dev", "disp_wed", "n_books_wed",
            "is_total"]


# ---------------------------------------------------------------------------
# Unit construction
# ---------------------------------------------------------------------------

def _season_of(kickoff) -> int:
    return kickoff.year - 1 if kickoff.month == 1 else kickoff.year


def _modal_line(lines) -> float:
    counts = collections.Counter(lines)
    best = max(counts.values())
    cands = [ln for ln, c in counts.items() if c == best]
    if len(cands) == 1:
        return cands[0]
    med = statistics.median(lines)
    return min(cands, key=lambda ln: (abs(ln - med), ln))


def _consensus(book_pairs, line):
    """Median fair prob across books quoting exactly `line`; None if <2."""
    vals = [f for _b, pairs in book_pairs.items()
            for ln, f in pairs if ln == line]
    books = {_b for _b, pairs in book_pairs.items()
             for ln, _f in pairs if ln == line}
    if len(books) < 2:
        return None
    return statistics.median(vals)


def _dispersion(book_pairs, line):
    vals = [f for _b, pairs in book_pairs.items()
            for ln, f in pairs if ln == line]
    if len(vals) < 2:
        return 0.0
    return statistics.pstdev(vals)


def build_units(quotes):
    """Return list of analysis units (dicts). Point-in-time safe by slot rule."""
    normed = []
    for q in quotes:
        nq = dict(q)
        nq["observed_at"] = _as_utc(q.get("observed_at"))
        nq["kickoff"] = _as_utc(q.get("kickoff"))
        normed.append(nq)
    key_of = canonical_game_keys(normed)
    kickoffs = collections.defaultdict(list)
    for q in normed:
        k = key_of.get(id(q))
        if k is not None and q.get("kickoff") is not None:
            kickoffs[k].append(q["kickoff"])

    gs = build_game_series(quotes)
    units = []
    for gkey, g in gs.items():
        ko_list = kickoffs.get(gkey)
        if not ko_list:
            continue
        kickoff = collections.Counter(ko_list).most_common(1)[0][0]
        snaps = sorted({s for m in MARKETS for s in g["line_pairs"].get(m, {})})
        pre = [s for s in snaps if s < kickoff]
        if not pre:
            continue
        close = max(pre)
        if (kickoff - close).total_seconds() > 72 * 3600:
            continue
        sat_c = [s for s in pre if s <= close - dt.timedelta(hours=24)]
        wed_c = [s for s in pre if s <= close - dt.timedelta(hours=96)]
        if not sat_c or not wed_c:
            continue
        sat, wed = max(sat_c), max(wed_c)
        season = _season_of(kickoff)
        for market in MARKETS:
            lp = g["line_pairs"].get(market, {})
            if not all(s in lp for s in (wed, sat, close)):
                continue
            close_lines = [ln for pairs in lp[close].values()
                           for ln, _f in pairs]
            if not close_lines:
                continue
            line = _modal_line(close_lines)
            f_wed = _consensus(lp[wed], line)
            f_sat = _consensus(lp[sat], line)
            f_close = _consensus(lp[close], line)
            if f_wed is None or f_sat is None or f_close is None:
                continue
            pinn_wed = None
            for ln, f in lp[wed].get(PINNACLE, []):
                if ln == line:
                    pinn_wed = f
                    break
            units.append({
                "game": gkey, "market": market, "season": season,
                "line": line,
                "f_wed": f_wed, "f_sat": f_sat, "f_close": f_close,
                "move": f_sat - f_wed,
                "pinn_dev": (pinn_wed - f_wed) if pinn_wed is not None else 0.0,
                "disp_wed": _dispersion(lp[wed], line),
                "n_books_wed": len({b for b, pairs in lp[wed].items()
                                    for ln, _f in pairs if ln == line}),
                "is_total": 1.0 if market == TOTAL else 0.0,
            })
    return units


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _feat_matrix(units):
    X = np.array([[u[f] for f in FEATURES] for u in units], dtype=float)
    y = np.array([u["f_close"] for u in units], dtype=float)
    return X, y


def ridge_fit_predict(X_train, y_train, X_test, alpha=RIDGE_ALPHA):
    """Ridge on standardized features; y centered. Returns predictions."""
    mu = X_train.mean(axis=0)
    sd = X_train.std(axis=0)
    sd[sd == 0.0] = 1.0
    Xs = (X_train - mu) / sd
    Xt = (X_test - mu) / sd
    yc = y_train - y_train.mean()
    p = Xs.shape[1]
    A = Xs.T @ Xs + alpha * np.eye(p)
    w = np.linalg.solve(A, Xs.T @ yc)
    return Xt @ w + y_train.mean()


def loso_predict(units):
    """Leave-one-season-out predictions. Returns {idx: pred} plus per-season
    RMSE for model and baselines."""
    if np is None:
        raise RuntimeError("numpy is required")
    X, y = _feat_matrix(units)
    seasons = sorted({u["season"] for u in units})
    preds, per_season = {}, {}
    for s in seasons:
        te = np.array([i for i, u in enumerate(units) if u["season"] == s])
        tr = np.array([i for i, u in enumerate(units) if u["season"] != s])
        if len(te) == 0 or len(tr) == 0:
            continue
        p = ridge_fit_predict(X[tr], y[tr], X[te])
        for i, v in zip(te, p):
            preds[int(i)] = float(v)
        yt = y[te]
        yw = np.array([units[i]["f_wed"] for i in te])
        ys = np.array([units[i]["f_sat"] for i in te])
        per_season[s] = {
            "n": int(len(te)),
            "rmse_model": float(np.sqrt(np.mean((p - yt) ** 2))),
            "rmse_bwed": float(np.sqrt(np.mean((yw - yt) ** 2))),
            "rmse_bsat": float(np.sqrt(np.mean((ys - yt) ** 2))),
        }
    return preds, per_season


# ---------------------------------------------------------------------------
# Inference (repo convention: normal approximation, cf. market_alpha)
# ---------------------------------------------------------------------------

def one_sided_paired_greater_neg(diffs):
    """One-sided test of mean(diffs) < 0. Returns (mean, n, z, p)."""
    n = len(diffs)
    mean = statistics.fmean(diffs)
    sd = statistics.pstdev(diffs)
    if n < 2 or sd == 0:
        return (mean, n, 0.0, 1.0 if mean >= 0 else 0.0)
    z = mean / (sd / math.sqrt(n))
    # P(mean < 0): p = Phi(z) for z negative large -> small p
    p = 0.5 * math.erfc(-z / math.sqrt(2.0))
    return (mean, n, z, p)


def decide(rmse_model, rmse_bsat, p_value, per_season):
    """Preregistered decision rule."""
    cond_a = p_value < ALPHA
    cond_b = rmse_model <= rmse_bsat - PRACTICAL_RMSE_GAIN
    wins = sum(1 for s, r in per_season.items()
               if r["rmse_model"] < r["rmse_bsat"])
    cond_c = wins >= 2
    if cond_a and cond_b and cond_c:
        return "advance_to_forward_shadow_candidacy"
    return "no_edge"


def run_market_only(units):
    if len(units) < MIN_UNITS:
        return {"verdict": "infeasible", "n_units": len(units)}
    preds, per_season = loso_predict(units)
    idx = sorted(preds)
    se_m, se_w, se_s = [], [], []
    for i in idx:
        u = units[i]
        se_m.append((preds[i] - u["f_close"]) ** 2)
        se_w.append((u["f_wed"] - u["f_close"]) ** 2)
        se_s.append((u["f_sat"] - u["f_close"]) ** 2)
    n = len(idx)
    rmse_m = math.sqrt(sum(se_m) / n)
    rmse_w = math.sqrt(sum(se_w) / n)
    rmse_s = math.sqrt(sum(se_s) / n)
    diffs = [a - b for a, b in zip(se_m, se_s)]
    mean_d, _n, z, p = one_sided_paired_greater_neg(diffs)
    verdict = decide(rmse_m, rmse_s, p, per_season)
    # Exploratory CLV sketch (non-decisive): at sat, lean toward predicted
    # close when |pred - f_sat| >= 0.01; score vs actual close.
    clv = []
    for i in idx:
        u = units[i]
        gap = preds[i] - u["f_sat"]
        if abs(gap) >= 0.01:
            clv.append(math.copysign(1.0, gap) * (u["f_close"] - u["f_sat"]))
    out = {
        "role": "SHADOW",
        "preregistration": PREREG_PATH,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "n_units": n,
        "seasons": sorted({u["season"] for u in units}),
        "rmse_model": rmse_m, "rmse_bwed": rmse_w, "rmse_bsat": rmse_s,
        "primary_test": {"mean_se_diff": mean_d, "n": n, "z": z,
                         "p_one_sided": p, "alpha": ALPHA},
        "per_season": per_season,
        "exploratory_clv_sketch": {
            "n_positions": len(clv),
            "mean_clv_pp": statistics.fmean(clv) if clv else 0.0,
        },
        "verdict": verdict,
    }
    return out


# ---------------------------------------------------------------------------
# Null simulation (pre-results calibration check, Amendment A1 template)
# ---------------------------------------------------------------------------

def null_simulate(n_games=400, n_seeds=200, sigma=0.02, seed0=0):
    """Martingale fair-prob evolution: f_sat is the optimal predictor, so a
    calibrated test must reject at ~nominal rate. Returns rejection rate."""
    rejs = 0
    for s in range(n_seeds):
        rng = np.random.default_rng(seed0 + s)
        units = []
        for g in range(n_games):
            f_wed = float(rng.uniform(0.3, 0.7))
            f_sat = min(0.99, max(0.01, f_wed + float(rng.normal(0, sigma))))
            f_close = min(0.99, max(0.01, f_sat + float(rng.normal(0, sigma))))
            units.append({
                "game": "g%d" % g, "market": SPREAD,
                "season": 2022 + (g % 3), "line": -3.0,
                "f_wed": f_wed, "f_sat": f_sat, "f_close": f_close,
                "move": f_sat - f_wed,
                "pinn_dev": float(rng.normal(0, 0.01)),
                "disp_wed": 0.01, "n_books_wed": 4,
                "is_total": 0.0,
            })
        res = run_market_only(units)
        if res["verdict"] == "infeasible":
            continue
        if res["primary_test"]["p_one_sided"] < ALPHA:
            rejs += 1
    return rejs / n_seeds


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--out", required=True)
    ap.add_argument("--null-sim", action="store_true",
                    help="run the pre-results null simulation instead of real data")
    args = ap.parse_args()
    if np is None:
        raise SystemExit("error: numpy is required")
    if args.null_sim:
        rate = null_simulate()
        print("null-simulation rejection rate at alpha=%.2f: %.3f" % (ALPHA, rate))
        print("CALIBRATED" if rate <= 0.075 else "MISCALIBRATED - AMEND BEFORE REAL DATA")
        return
    if args.csv:
        quotes = _load_quotes_csv(args.csv)
        n_quotes, n_snaps = len(quotes), len({q["observed_at"] for q in quotes})
    else:
        if not args.database_url:
            raise SystemExit("error: --database-url or NFL_EDGE_DATABASE_URL or --csv required")
        import psycopg
        conn = psycopg.connect(args.database_url, connect_timeout=15)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM nfl_edge_historical_quotes")
                n_quotes = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT observed_at) FROM nfl_edge_historical_quotes")
                n_snaps = cur.fetchone()[0]
                cur.execute("SELECT quote_id, provider_event_id, home_team, away_team, "
                            "kickoff, book_key, market, selection, line, american_odds, "
                            "observed_at FROM nfl_edge_historical_quotes")
                cols = [d[0] for d in cur.description]
                quotes = [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    print("freeze check: quotes=%d (expected %d), snapshots=%d (expected %d)"
          % (n_quotes, EXPECTED_QUOTES, n_snaps, EXPECTED_SNAPSHOTS), flush=True)
    if n_quotes != EXPECTED_QUOTES or n_snaps != EXPECTED_SNAPSHOTS:
        raise SystemExit("error: freeze check failed")
    units = build_units(quotes)
    print("built %d analysis units" % len(units), flush=True)
    results = run_market_only(units)
    results["freeze_check"] = {"n_quotes": n_quotes, "n_snapshots": n_snaps}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, sort_keys=True)
    print("wrote %s (verdict=%s)" % (args.out, results["verdict"]))


if __name__ == "__main__":
    main()
