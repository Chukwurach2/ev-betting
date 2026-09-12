"""residual-v1: does challenger v1's disagreement with the market predict the
closing-line move?  SHADOW research only.

Design: see docs/preregistrations/residual-v1.md (committed before any
real-data execution).

Challenger point-in-time audit: artifact v1 has train_end=2021 (6,137
nflverse games 1999-2021), static Elo ratings, fixed hyperparameters.
2022-2024 is a valid evaluation window (challenger never saw those scores),
but it was also the challenger's gate-validation window: this is a
mechanism test on a shared window, not independent evidence.

Unit: canonical (game, market) with sat/close snapshots (reuses the
market-only-v1 unit builder), plus challenger fair prob at the analysis
line L.  Primary test: OLS move ~ r, one-sided t-test slope > 0.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from research.market_only_v1 import build_units  # noqa: E402
from research.market_alpha import (  # noqa: E402
    _load_quotes_csv, DATASET_FINGERPRINT, EXPECTED_QUOTES,
    EXPECTED_SNAPSHOTS,
)
from research.devig_tournament import canonical_game_keys  # noqa: E402
from model.challenger.infer import load_challenger  # noqa: E402

# Team full-name -> abbreviation.  Keep in sync with ops/picks.py
# FULL_TO_ABBR (cross-reference; duplicated here so research scripts do not
# import the live picks module).
FULL_TO_ABBR = {
    'Arizona Cardinals': 'AZ', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
    'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS',
}

ALPHA = 0.05


def challenger_fair(chal, unit) -> float | None:
    """No-vig fair prob of the reference selection at the analysis line.

    Spreads: home-perspective nflverse line = +L when the close market has
    the home team favored (f_close >= 0.5), else -L.  The favorite is
    inferred from market prices only -- no scores, no leakage.
    """
    try:
        habbr = FULL_TO_ABBR[unit["home"]]
        aabbr = FULL_TO_ABBR[unit["away"]]
    except KeyError:
        return None
    line = unit["line"]
    try:
        if unit["market"] == "h2h_spreads":
            home_line = line if unit["f_close"] >= 0.5 else -line
            p = chal.cover_probability(habbr, aabbr, "spreads", "home", home_line)
        else:
            p = chal.cover_probability(habbr, aabbr, "totals", "over", line)
    except Exception:
        return None
    if p is None or not (0.0 < p < 1.0):
        return None
    return float(p)


def build_residual_units(units, chal) -> list[dict]:
    """Attach challenger fair prob; keep units where it is computable."""
    out = []
    for u in units:
        cf = challenger_fair(chal, u)
        if cf is None:
            continue
        r = cf - u["f_sat"]
        move = u["f_close"] - u["f_sat"]
        out.append({**u, "chal_fair": cf, "r": r, "move": move})
    return out


def slope_t_test(r: np.ndarray, move: np.ndarray) -> dict:
    """OLS move = a + b*r; one-sided t-test on b > 0."""
    n = len(r)
    X = np.column_stack([np.ones(n), r])
    coef, *_ = np.linalg.lstsq(X, move, rcond=None)
    resid = move - X @ coef
    dof = n - 2
    s2 = float(resid @ resid / dof)
    XtX_inv = np.linalg.inv(X.T @ X)
    se_b = math.sqrt(s2 * XtX_inv[1, 1])
    b = float(coef[1])
    t = b / se_b if se_b > 0 else 0.0
    # one-sided p via normal approximation (n is large)
    p = 0.5 * math.erfc(t / math.sqrt(2))
    return {"n": n, "slope": b, "se": se_b, "t": t,
            "p_one_sided": p, "intercept": float(coef[0])}


def per_season_slopes(rows: list[dict]) -> dict:
    out = {}
    for season in sorted({r["season"] for r in rows}):
        sub = [r for r in rows if r["season"] == season]
        if len(sub) >= 10:
            t = slope_t_test(np.array([r["r"] for r in sub]),
                             np.array([r["move"] for r in sub]))
            out[str(season)] = t
    return out


def decide(primary: dict, season_slopes: dict) -> str:
    """Locked decision rule: signal iff (a) p<0.05, (b) slope>=0.2,
    (c) slope>0 in >=2 of 3 seasons."""
    pos = sum(1 for s in season_slopes.values() if s["slope"] > 0)
    if (primary["p_one_sided"] < ALPHA and primary["slope"] >= 0.2
            and pos >= 2):
        return "signal"
    return "no_edge"


def attach_teams(units, quotes) -> list[dict]:
    """Map each unit's canonical game key back to (home, away) team names."""
    key_of = canonical_game_keys(quotes)
    teams: dict[str, tuple] = {}
    for q in quotes:
        k = key_of.get(id(q))
        if k is not None:
            teams[k] = (q.get("home_team"), q.get("away_team"))
    out = []
    for u in units:
        t = teams.get(u["game"])
        if t and t[0] and t[1]:
            out.append({**u, "home": t[0], "away": t[1]})
    return out


def run(units, quotes) -> dict:
    chal = load_challenger("v1")
    units = attach_teams(units, quotes)
    rows = build_residual_units(units, chal)
    n = len(rows)
    result = {
        "experiment": "residual-v1",
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "n_units": n,
        "feasible": n >= 200,
        "seasons": sorted({r["season"] for r in rows}),
    }
    if n < 200:
        result["verdict"] = "infeasible"
        return result
    r = np.array([x["r"] for x in rows])
    move = np.array([x["move"] for x in rows])
    primary = slope_t_test(r, move)
    season_slopes = per_season_slopes(rows)
    verdict = decide(primary, season_slopes)
    result.update({
        "primary_test": primary,
        "per_season_slopes": season_slopes,
        "mean_abs_r": float(np.mean(np.abs(r))),
        "verdict": verdict,
    })
    return result


def null_simulation(n_seeds: int = 200, n: int = 700,
                    rng_seed: int = 20260912) -> dict:
    """Martingale null: r and move independent normals -> nominal size."""
    rng = np.random.default_rng(rng_seed)
    rejections = 0
    for _ in range(n_seeds):
        r = rng.normal(0, 0.05, n)
        move = rng.normal(0, 0.01, n)
        t = slope_t_test(r, move)
        if t["p_one_sided"] < ALPHA:
            rejections += 1
    rate = rejections / n_seeds
    return {"n_seeds": n_seeds, "rejection_rate": rate,
            "alpha": ALPHA, "verdict": "CALIBRATED" if rate <= 0.15
            else "MISCALIBRATED"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--csv", default=None)
    ap.add_argument("--out", default="/tmp/residual-v1.json")
    ap.add_argument("--null-sim", action="store_true")
    args = ap.parse_args()

    if args.null_sim:
        res = null_simulation()
        json.dump(res, open(args.out, "w"), indent=2)
        print("null-simulation rejection rate at alpha=%.2f: %.3f"
              % (ALPHA, res["rejection_rate"]))
        print(res["verdict"])
        return 0 if res["verdict"] == "CALIBRATED" else 1

    if args.csv:
        quotes = _load_quotes_csv(args.csv)
        n_quotes = len(quotes)
        n_snaps = len({q["observed_at"] for q in quotes})
    elif args.database_url:
        import psycopg
        conn = psycopg.connect(args.database_url, connect_timeout=15)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM nfl_edge_historical_quotes")
                n_quotes = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT observed_at) "
                            "FROM nfl_edge_historical_quotes")
                n_snaps = cur.fetchone()[0]
                cur.execute("SELECT quote_id, provider_event_id, home_team, "
                            "away_team, kickoff, book_key, market, selection, "
                            "line, american_odds, observed_at "
                            "FROM nfl_edge_historical_quotes")
                cols = [d[0] for d in cur.description]
                quotes = [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    else:
        print("need --database-url or --csv", file=sys.stderr)
        return 2

    print("freeze check: quotes=%d (expected %d), snapshots=%d (expected %d)"
          % (n_quotes, EXPECTED_QUOTES, n_snaps, EXPECTED_SNAPSHOTS),
          flush=True)
    if n_quotes != EXPECTED_QUOTES or n_snaps != EXPECTED_SNAPSHOTS:
        print("error: freeze check failed", file=sys.stderr)
        return 1

    units = build_units(quotes)
    print("built %d analysis units" % len(units), flush=True)
    res = run(units, quotes)
    res["freeze_check"] = {"n_quotes": n_quotes, "n_snapshots": n_snaps}
    json.dump(res, open(args.out, "w"), indent=2, sort_keys=True)
    print("verdict:", res["verdict"], "| n_units:", res["n_units"])
    if res.get("primary_test"):
        pt = res["primary_test"]
        print("slope=%.4f se=%.4f t=%+.3f p_one_sided=%.4g"
              % (pt["slope"], pt["se"], pt["t"], pt["p_one_sided"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
