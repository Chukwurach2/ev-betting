"""Pinnacle-early-week v1: does Pinnacle's early-week price predict the close?

Preregistered: docs/preregistrations/pinnacle-early-week-v1.md (written
before any execution). Frozen dataset: 2022-2024 weeks 1-18, fingerprint
43f853a44bf93937d85149ca5fd7241b (docs/dataset-freeze.md).

Hypothesis: Pinnacle's early-week (Wednesday 12:00 UTC) price predicts the
closing consensus direction better than the early-week cross-book consensus
does. Different mechanism from market-alpha-v1 Track A (which tested
follower copying in subsequent snapshots): the benchmark here is the CLOSE.

Per game/market: signal s = sign(f_pin_early - C_wed) (|signal| >= 0.005
required); outcome o = sign(C_close - C_wed). Tests: (1) one-sided binomial
agreement > 0.5; (2) one-sided t-test of the consensus-priced Wednesday
Pinnacle-lean strategy's CLV > 0; Holm across the 2 tests; practical bar
mean CLV >= 0.01. Exploratory falsification: does Pinnacle itself converge
toward the early consensus by the close?

Point-in-time safe: the early snapshot uses only information at its
observed_at; the close is an ex-post benchmark. Pinnacle never enters its
own consensus. Multiplicative de-vig. Read-only; zero API credits.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.devig_tournament import _holm_bonferroni  # noqa: E402
from research.market_alpha import (  # noqa: E402
    _jsonable,
    binomial_one_sided_ge,
    build_game_series,
    one_sample_one_sided,
)

PREREG_PATH = "docs/preregistrations/pinnacle-early-week-v1.md"
DATASET_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
EXPECTED_QUOTES = 291586
EXPECTED_SNAPSHOTS = 162
ALPHA = 0.05

SPREAD = "FULL_GAME_SPREAD"
TOTAL = "FULL_GAME_TOTAL"
DEFAULT_MARKETS = (SPREAD, TOTAL)
PINNACLE = "pinnacle"

SIGNAL_MIN = 0.005  # min |f_pin - C_wed| to count as a signal
MIN_CONSENSUS_BOOKS = 3  # other books required at the early snapshot
MIN_CLOSE_BOOKS = 2  # other books required at the close
MIN_UNITS = 30  # signal games required, else infeasible
PRACTICAL_CLV = 0.01  # min mean per-game CLV to matter
EARLY_WINDOW_DAYS = 9  # earliest early snapshot: kickoff - 9 days
EARLY_MIN_HOURS = 48  # latest early snapshot: kickoff - 48h


def _pairs_at_line(lp_market, snap, book, line):
    for ln, f in (lp_market.get(snap) or {}).get(book, []):
        if ln == line:
            return f
    return None


def early_week_units(game_series, markets=DEFAULT_MARKETS):
    """Build one unit per (game, market) with a valid early signal.

    Returns (units, diagnostics). Each unit: game, market, early snap,
    line, signal sign, agreement (bool), clv (consensus-priced strategy),
    plus falsification fields for Pinnacle's own convergence.
    """
    units = []
    diag = {"n_no_signal": 0, "n_no_early": 0, "n_no_close": 0}
    for gkey, g in game_series.items():
        ko = g.get("kickoff")
        if ko is None:
            continue
        for market in markets:
            series = g.get("series", {}).get(market, {})
            lp = g.get("line_pairs", {}).get(market, {})
            if not series:
                continue
            snaps = sorted(series)
            pre = [s for s in snaps if s < ko]
            if not pre:
                continue
            close_snap = pre[-1]
            lo = ko - dt.timedelta(days=EARLY_WINDOW_DAYS)
            hi = ko - dt.timedelta(hours=EARLY_MIN_HOURS)
            # Wednesday snapshots of the cadence. NOTE: the provider returns
            # snapshots at ~11:55 UTC, stored as-is; matching on weekday only
            # (an earlier hour==12 filter silently matched nothing — fixed
            # 2026-09-12 after the first run returned zero units).
            cands = [s for s in pre if s.weekday() == 2 and lo <= s <= hi]
            chosen = None
            for s in sorted(cands):  # earliest first
                pin = series[s].get(PINNACLE)
                if pin is None:
                    continue
                line = pin[0]
                others = {}
                for b, pairs in (lp.get(s) or {}).items():
                    if b == PINNACLE:
                        continue
                    for ln, f in pairs:
                        if ln == line:
                            others[b] = f
                if len(others) >= MIN_CONSENSUS_BOOKS:
                    chosen = (s, line, pin[1], others)
                    break
            if chosen is None:
                diag["n_no_early"] += 1
                continue
            s, line, f_pin, others = chosen
            c_wed = statistics.median(others.values())
            sig = f_pin - c_wed
            if abs(sig) < SIGNAL_MIN:
                diag["n_no_signal"] += 1
                continue
            s_sign = 1 if sig > 0 else -1
            close_map = {}
            for b, pairs in (lp.get(close_snap) or {}).items():
                if b == PINNACLE:
                    continue
                for ln, f in pairs:
                    if ln == line:
                        close_map[b] = f
            if len(close_map) < MIN_CLOSE_BOOKS:
                diag["n_no_close"] += 1
                continue
            c_close = statistics.median(close_map.values())
            move = c_close - c_wed
            o_sign = 1 if move > 0 else (-1 if move < 0 else 0)
            agree = o_sign != 0 and s_sign == o_sign
            # Consensus-priced strategy CLV: bet at C_wed, score vs C_close.
            # Other-selection fair prob = 1 - q_ref (multiplicative de-vig
            # normalizes), so CLV = s_sign * (C_close - C_wed).
            clv = s_sign * move
            # Falsification: does Pinnacle itself converge to the early
            # consensus? (exploratory, complete-case)
            f_pin_close = _pairs_at_line(lp, close_snap, PINNACLE, line)
            conv = None
            if f_pin_close is not None and abs(f_pin_close - f_pin) >= 0.002:
                conv = ((1 if (c_wed - f_pin) > 0 else -1)
                        == (1 if (f_pin_close - f_pin) > 0 else -1))
            units.append({
                "game": gkey, "market": market, "snap": s, "line": line,
                "f_pin": f_pin, "c_wed": c_wed, "c_close": c_close,
                "signal": s_sign, "agree": agree, "clv": clv,
                "pinnacle_converges": conv,
            })
    return units, diag


def analyze_early_week(units):
    """Confirmatory tests: binomial on agreement, t-test on CLV."""
    by_game_agree = {}
    by_game_clv = {}
    for u in units:
        by_game_agree.setdefault(u["game"], []).append(1 if u["agree"] else 0)
        by_game_clv.setdefault(u["game"], []).append(u["clv"])
    # Game-level: agreement = fraction of the game's units; CLV = mean.
    agree_vals = [sum(v) / len(v) for v in by_game_agree.values()]
    clv_vals = [sum(v) / len(v) for v in by_game_clv.values()]
    n = len(agree_vals)
    out = {"n_units": len(units), "n_games": n, "feasible": n >= MIN_UNITS}
    if not n:
        return out
    # Direction test: count game-level majority agreements as successes.
    # (Pre-declared unit is the game; a game agrees if mean agreement > 0.5.)
    k = sum(1 for a in agree_vals if a > 0.5)
    ties = sum(1 for a in agree_vals if a == 0.5)
    p_dir = binomial_one_sided_ge(k, n - ties) if n - ties else None
    out["direction"] = {
        "n": n, "n_success": k, "n_ties_excluded": ties,
        "agree_rate": k / (n - ties) if n - ties else None,
        "p_raw": p_dir,
    }
    res = one_sample_one_sided(clv_vals)
    t_clv = res[3] if res else None
    p_clv = res[4] if res else None
    mean_clv = sum(clv_vals) / n
    out["clv"] = {
        "n": n, "mean": mean_clv,
        "t": t_clv, "p_raw": p_clv,
    }
    holm = _holm_bonferroni(
        [("direction", p_dir if p_dir is not None else 1.0),
         ("clv", p_clv if p_clv is not None else 1.0)])
    out["direction"]["p_holm"] = holm["direction"]
    out["direction"]["significant"] = holm["direction"] < ALPHA
    out["clv"]["p_holm"] = holm["clv"]
    out["clv"]["significant"] = holm["clv"] < ALPHA
    # Falsification (exploratory)
    conv = [u["pinnacle_converges"] for u in units
            if u["pinnacle_converges"] is not None]
    out["falsification"] = {
        "n": len(conv),
        "pinnacle_converge_rate": (sum(conv) / len(conv)) if conv else None,
    }
    return out


def decide_early_week(analysis, alpha=ALPHA, practical_clv=PRACTICAL_CLV):
    if not analysis["feasible"]:
        return "infeasible", {"reason": "fewer than %d signal games" % MIN_UNITS}
    d_sig = analysis["direction"]["significant"]
    c_sig = analysis["clv"]["significant"]
    mean_clv = analysis["clv"]["mean"]
    detail = {
        "direction_significant": d_sig,
        "clv_significant": c_sig,
        "mean_clv": mean_clv,
        "practical_bar": practical_clv,
    }
    if d_sig and c_sig and mean_clv >= practical_clv:
        return "pinnacle_early_signal", detail
    if d_sig and c_sig:
        return "significant_but_negligible", detail
    return "no_signal", detail


def run_pinnacle_early_week(quotes, markets=DEFAULT_MARKETS):
    game_series = build_game_series(quotes, markets=markets)
    # Loud guard: the Wednesday cadence must exist in the data at all.
    wedges = [s for g in game_series.values()
              for m in g.get("series", {}).values() for s in m
              if s.weekday() == 2]
    if not wedges:
        raise SystemExit("error: no Wednesday snapshots in dataset")
    units, diag = early_week_units(game_series, markets=markets)
    analysis = analyze_early_week(units)
    verdict, detail = decide_early_week(analysis)
    return {
        "preregistration": PREREG_PATH,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "SHADOW",
        "markets": list(markets),
        "n_games": len(game_series),
        "diagnostics": diag,
        "analysis": _jsonable(analysis),
        "verdict": verdict,
        "verdict_detail": _jsonable(detail),
    }, game_series


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--csv", default=None)
    ap.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
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
        conn = psycopg.connect(args.database_url, connect_timeout=15)
        try:
            with conn.cursor() as cur:
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
                    "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')")
                cols = [d[0] for d in cur.description]
                quotes = [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    print("freeze check: quotes=%d (expected %d), snapshots=%d (expected %d)"
          % (n_quotes, EXPECTED_QUOTES, n_snaps, EXPECTED_SNAPSHOTS), flush=True)
    if n_quotes != EXPECTED_QUOTES or n_snaps != EXPECTED_SNAPSHOTS:
        raise SystemExit("error: freeze check failed")
    results, _ = run_pinnacle_early_week(quotes, markets=tuple(args.markets.split(",")))
    with open(args.out, "w") as f:
        json.dump(results, f, default=str)
    print("wrote %s (verdict=%s)" % (args.out, results["verdict"]), flush=True)


if __name__ == "__main__":
    main()
