"""Execution-alpha v1: pure line-shopping CLV vs closing LOBO consensus.

Preregistered: docs/preregistrations/execution-alpha-v1.md (written before
any execution). Frozen dataset: 2022-2024 weeks 1-18, fingerprint
43f853a44bf93937d85149ca5fd7241b (docs/dataset-freeze.md).

Hypothesis: pure execution -- always taking the best available price across
books at each snapshot, with zero prediction -- beats the closing LOBO
consensus (positive CLV).

The Amendment A1 lesson is baked in: under the noise null, best-price
selection has POSITIVE expected edge (mechanical shopping bias), so the
confirmatory test is observed-vs-simulated-null, NOT mean-vs-zero. The null
DGP gives every book the observed same-snapshot consensus plus iid noise
with the observed cross-book std; beating it means beating mere
noise-shopping.

Game identity reuses canonical_game_keys ((home, away) + kickoff-proximity
clustering). Exact-line comparability throughout. Target book never enters
its own closing benchmark. Closing snapshots are ex-post benchmarks only.
Multiplicative de-vig. Read-only; zero API credits.

Also reused for moneyline-pilot-v1 Phase 3 (markets=("FULL_GAME_MONEYLINE",)).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import random
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.market_alpha import (  # noqa: E402
    _as_utc,
    _jsonable,
    build_game_series,
    one_sample_one_sided,
)

PREREG_PATH = "docs/preregistrations/execution-alpha-v1.md"
DATASET_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
EXPECTED_QUOTES = 291586
EXPECTED_SNAPSHOTS = 162
ALPHA = 0.05

SPREAD = "FULL_GAME_SPREAD"
TOTAL = "FULL_GAME_TOTAL"
MONEYLINE = "FULL_GAME_MONEYLINE"
DEFAULT_MARKETS = (SPREAD, TOTAL)

PRACTICAL_EDGE = 0.01  # min mean game-level edge (prob. points) to matter
MIN_GAMES = 30  # games with >= 1 cell required, else infeasible
N_NULL = 500  # null replicates for the observed-vs-null test


def _unbiased_sigma(fs):
    """Unbiased cross-book std with the c4(n) small-sample correction.

    The naive pstdev/stdev underestimates sigma for the 2-6 books in a
    cell; the null DGP then sits systematically below the observed
    statistic (caught by the A1-template calibration test: 59% false
    rejections with pstdev). c4(n) = sqrt(2/(n-1)) * Gamma(n/2)/Gamma((n-1)/2).
    """
    n = len(fs)
    if n < 2:
        return 0.0
    s = statistics.stdev(fs)  # ddof=1
    c4 = math.sqrt(2.0 / (n - 1)) * math.gamma(n / 2.0) / math.gamma((n - 1) / 2.0)
    return s / c4 if c4 > 0 else 0.0


def _line_map(lp_market, snap):
    """{line: {book: f_ref}} for one snapshot's line_pairs."""
    out = {}
    for b, pairs in (lp_market.get(snap) or {}).items():
        for line, f in pairs:
            out.setdefault(line, {})[b] = f
    return out


def execution_cells(game_series, markets=DEFAULT_MARKETS):
    """Build best-price-vs-close cells.

    Each cell: one (game, market, snapshot s < close, exact line L) with
    >= 2 books at L. Records the best book's fair prob, the strict-LOBO
    closing benchmark (evaluated book excluded), and the null-DGP
    parameters (same-snapshot consensus + cross-book std; closing
    consensus + std over all close books).
    """
    cells = []
    for gkey, g in game_series.items():
        ko = g.get("kickoff")
        for market in markets:
            series = g.get("series", {}).get(market, {})
            lp = g.get("line_pairs", {}).get(market, {})
            if not series:
                continue
            snaps = sorted(series)
            pre = [s for s in snaps if ko is None or s < ko]
            if not pre:
                continue
            close_snap = pre[-1]
            close_map = _line_map(lp, close_snap)
            for s in snaps:
                if s >= close_snap:
                    continue
                snap_map = _line_map(lp, s)
                for line, books in snap_map.items():
                    if len(books) < 2:
                        continue
                    best_book = max(sorted(books), key=lambda b: books[b])
                    f_best = books[best_book]
                    close_books = {b: f for b, f in
                                   close_map.get(line, {}).items()
                                   if b != best_book}
                    if len(close_books) < 2:
                        continue
                    close_med = statistics.median(close_books.values())
                    snap_fs = list(books.values())
                    all_close_fs = list(close_map.get(line, {}).values())
                    cells.append({
                        "game": gkey,
                        "market": market,
                        "snap": s,
                        "line": line,
                        "best_book": best_book,
                        "f_best": f_best,
                        "edge": f_best - close_med,
                        "snap_books": dict(books),
                        "snap_median": statistics.median(snap_fs),
                        "snap_sigma": _unbiased_sigma(snap_fs),
                        "close_books_all": dict(close_map.get(line, {})),
                        "close_median_all": statistics.median(all_close_fs),
                        "close_sigma": _unbiased_sigma(all_close_fs),
                    })
    return cells


def analyze_execution(cells):
    """Aggregate cells to game level, then overall. Returns stats dict."""
    by_game = {}
    for c in cells:
        by_game.setdefault(c["game"], []).append(c["edge"])
    game_edges = {g: sum(v) / len(v) for g, v in by_game.items()}
    vals = list(game_edges.values())
    n = len(vals)
    out = {
        "n_cells": len(cells),
        "n_games": n,
        "game_edges": game_edges,
        "feasible": n >= MIN_GAMES,
    }
    if n:
        mean = sum(vals) / n
        out["mean"] = mean
        if n > 1:
            sd = statistics.stdev(vals)
            out["stdev"] = sd
            # 95% CI via normal approx (n is large in practice)
            se = sd / math.sqrt(n)
            out["ci95"] = (mean - 1.96 * se, mean + 1.96 * se)
        # best-book concentration (exploratory)
        conc = {}
        for c in cells:
            conc[c["best_book"]] = conc.get(c["best_book"], 0) + 1
        out["best_book_counts"] = conc
    return out


def null_replicate_means(cells, rng, n_rep=N_NULL):
    """Null DGP: books = same-snapshot consensus + iid N(0, sigma).

    Returns the null distribution of the mean game-level edge. Under this
    null there is no staleness and no signal -- only the mechanical
    shopping bias -- so the observed mean must beat THIS, not zero.
    """
    null_means = []
    for _ in range(n_rep):
        by_game = {}
        for c in cells:
            draws = {b: rng.gauss(c["snap_median"], c["snap_sigma"])
                     for b in c["snap_books"]}
            best = max(sorted(draws), key=lambda b: draws[b])
            f_best = draws[best]
            close_draws = {
                b: rng.gauss(c["close_median_all"], c["close_sigma"])
                for b in c["close_books_all"]}
            others = [f for b, f in close_draws.items() if b != best]
            if not others:
                continue
            edge = f_best - statistics.median(others)
            by_game.setdefault(c["game"], []).append(edge)
        per_game = [sum(v) / len(v) for v in by_game.values() if v]
        null_means.append(sum(per_game) / len(per_game) if per_game else 0.0)
    return null_means


def decide_execution(analysis, null_means, alpha=ALPHA,
                     practical_edge=PRACTICAL_EDGE):
    """Verdict from the observed-vs-null comparison."""
    if not analysis["feasible"]:
        return "infeasible", {"reason": "fewer than %d games" % MIN_GAMES}
    obs = analysis["mean"]
    n_ge = sum(1 for m in null_means if m >= obs)
    p = (n_ge + 1) / (len(null_means) + 1)
    detail = {
        "observed_mean": obs,
        "null_mean": sum(null_means) / len(null_means),
        "null_p95": sorted(null_means)[int(0.95 * len(null_means))],
        "p_value": p,
        "n_null": len(null_means),
        "practical_bar": practical_edge,
    }
    if p < alpha and obs >= practical_edge:
        return "execution_edge", detail
    return "no_edge", detail


def moneyline_quality_gate(game_series):
    """Pilot Phase 2 quality gate: >=50% of games have >=3 books with
    moneyline pairs in their closing snapshot. Returns (passed, frac, n)."""
    n_games = 0
    n_ok = 0
    for gkey, g in game_series.items():
        ko = g.get("kickoff")
        series = g.get("series", {}).get(MONEYLINE, {})
        lp = g.get("line_pairs", {}).get(MONEYLINE, {})
        if not series:
            continue
        snaps = sorted(series)
        pre = [s for s in snaps if ko is None or s < ko]
        if not pre:
            continue
        close_snap = pre[-1]
        n_books = len(lp.get(close_snap) or {})
        n_games += 1
        if n_books >= 3:
            n_ok += 1
    frac = (n_ok / n_games) if n_games else 0.0
    return frac >= 0.5, frac, n_games


def run_execution_alpha(quotes, markets=DEFAULT_MARKETS, n_null=N_NULL,
                        seed=20260912, prereg_path=PREREG_PATH):
    """Full pipeline on quote dicts. Returns JSON-serializable results."""
    game_series = build_game_series(quotes, markets=markets)
    if markets == (MONEYLINE,):
        passed, frac, n = moneyline_quality_gate(game_series)
        if not passed:
            return {
                "preregistration": prereg_path,
                "dataset_fingerprint": DATASET_FINGERPRINT,
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "role": "SHADOW",
                "markets": list(markets),
                "n_games": len(game_series),
                "verdict": "infeasible_for_analysis",
                "verdict_detail": {
                    "reason": "pilot quality gate failed",
                    "frac_games_close_ge3books": frac,
                    "n_games": n,
                },
            }, game_series
    cells = execution_cells(game_series, markets=markets)
    analysis = analyze_execution(cells)
    rng = random.Random(seed)
    null_means = (null_replicate_means(cells, rng, n_rep=n_null)
                  if analysis["feasible"] else [])
    verdict, detail = decide_execution(analysis, null_means)
    # Executability diagnostics (exploratory, pre-declared)
    exec_diag = {}
    if cells:
        stale_like = 0
        for c in cells:
            dev = abs(c["f_best"] - c["snap_median"])
            if dev >= 0.02:
                stale_like += 1
        exec_diag["frac_best_potentially_stale"] = stale_like / len(cells)
        ge = sorted(analysis["game_edges"].values(), reverse=True)
        top5 = ge[: max(1, len(ge) // 20)]
        exec_diag["share_edge_top5pct_games"] = (
            sum(top5) / sum(ge) if sum(ge) else None)
    return {
        "preregistration": prereg_path,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "SHADOW",
        "markets": list(markets),
        "n_games": len(game_series),
        "n_cells": len(cells),
        "analysis": _jsonable(analysis),
        "verdict": verdict,
        "verdict_detail": _jsonable(detail),
        "executability": _jsonable(exec_diag),
    }, game_series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--csv", default=None)
    ap.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
    ap.add_argument("--prereg", default=PREREG_PATH)
    ap.add_argument("--n-null", type=int, default=N_NULL)
    ap.add_argument("--seed", type=int, default=20260912)
    ap.add_argument("--skip-freeze-check", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    from research.market_alpha import _load_quotes_csv
    if args.csv:
        quotes = _load_quotes_csv(args.csv)
        n_quotes, n_snaps = len(quotes), len({q["observed_at"] for q in quotes})
    else:
        if not args.database_url:
            raise SystemExit("error: --database-url or NFL_EDGE_DATABASE_URL or --csv required")
        import psycopg
        markets = tuple(args.markets.split(","))
        conn = psycopg.connect(args.database_url, connect_timeout=15)
        try:
            with conn.cursor() as cur:
                # Freeze check covers the frozen spread/total dataset only;
                # moneyline pilot rows (if present) do not affect it.
                cur.execute("SELECT COUNT(*) FROM nfl_edge_historical_quotes "
                            "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')")
                n_quotes = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT observed_at) "
                            "FROM nfl_edge_historical_quotes "
                            "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')")
                n_snaps = cur.fetchone()[0]
                cur.execute(
                    "SELECT quote_id, provider_event_id, home_team, away_team, "
                    "kickoff, book_key, market, selection, line, american_odds, "
                    "observed_at FROM nfl_edge_historical_quotes "
                    "WHERE market = ANY(%s)",
                    (list(markets),))
                cols = [d[0] for d in cur.description]
                quotes = [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    print("freeze check: quotes=%d (expected %d), snapshots=%d (expected %d)"
          % (n_quotes, EXPECTED_QUOTES, n_snaps, EXPECTED_SNAPSHOTS), flush=True)
    if not args.skip_freeze_check and (
            n_quotes != EXPECTED_QUOTES or n_snaps != EXPECTED_SNAPSHOTS):
        raise SystemExit("error: freeze check failed")
    markets = tuple(args.markets.split(","))
    results, _ = run_execution_alpha(quotes, markets=markets,
                                     n_null=args.n_null, seed=args.seed,
                                     prereg_path=args.prereg)
    with open(args.out, "w") as f:
        json.dump(results, f, default=str)
    print("wrote %s (verdict=%s)" % (args.out, results["verdict"]), flush=True)


if __name__ == "__main__":
    main()
