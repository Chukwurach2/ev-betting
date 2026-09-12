"""Steam v1: coordinated multi-book moves predicting the close.

Preregistered: docs/preregistrations/steam-v1.md (written before any
execution). Frozen dataset: 2022-2024 weeks 1-18, fingerprint
43f853a44bf93937d85149ca5fd7241b (docs/dataset-freeze.md).

Hypothesis: coordinated multi-book line moves ("steam") -- arising from
news, injury information, or correlated sharp action -- predict
continuation to the close. This is distinct from market-alpha-v1 Track A
(single-book leadership): a market can have no persistent leader yet
exhibit predictable coordinated moves.

Point-in-time safe: the steam move is measured on (s1 -> s2); the outcome
is the close move from s2 to the closing snapshot. Exact-line
comparability throughout. Multiplicative de-vig. Read-only; zero credits.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.market_alpha import (  # noqa: E402
    _as_utc,
    _jsonable,
    binomial_one_sided_ge,
    build_game_series,
    one_sample_one_sided,
)

PREREG_PATH = "docs/preregistrations/steam-v1.md"
DATASET_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
EXPECTED_QUOTES = 291586
EXPECTED_SNAPSHOTS = 162
ALPHA = 0.05

SPREAD = "FULL_GAME_SPREAD"
TOTAL = "FULL_GAME_TOTAL"
DEFAULT_MARKETS = (SPREAD, TOTAL)

MIN_BOOKS = 5          # books moving the same direction
MOVE_THRESH = 0.01     # per-book |delta| to count as moving
MEDIAN_THRESH = 0.005  # median |delta| across all books at the line
PRACTICAL_CLV = 0.01
MIN_GAMES = 30


def _holm_bonferroni(items):
    """items: [(name, p)]. Returns {name: holm-adjusted p}."""
    ordered = sorted(items, key=lambda kv: kv[1])
    m = len(ordered)
    adj = {}
    running = 0.0
    for rank, (name, p) in enumerate(ordered):
        cur = min(1.0, (m - rank) * p)
        running = max(running, cur)
        adj[name] = running
    return adj


def steam_events(game_series, markets=DEFAULT_MARKETS,
                 min_books=MIN_BOOKS, move_thresh=MOVE_THRESH,
                 median_thresh=MEDIAN_THRESH):
    """Detect steam events.

    For each game/market and each consecutive pre-close snapshot pair
    (s1 -> s2) with a later close available: measure each book's fair-prob
    move at the modal s2 line (exact line, both snapshots). A steam event
    needs >= min_books moving the same direction by >= move_thresh each,
    with median |delta| >= median_thresh.

    Returns events: {game, market, s2, line, direction, n_books, clv,
                     agree} where clv/agree score the s2->close consensus
    move (bet the steam direction at the s2 consensus price).
    """
    events = []
    for gkey, g in game_series.items():
        ko = g.get("kickoff")
        for market in markets:
            lp = g.get("line_pairs", {}).get(market, {})
            series = g.get("series", {}).get(market, {})
            if not series:
                continue
            snaps = sorted(series)
            pre = [s for s in snaps if ko is None or s < ko]
            if len(pre) < 3:
                continue
            close_snap = pre[-1]
            for i in range(1, len(pre) - 1):
                s1, s2 = pre[i - 1], pre[i]
                d1 = lp.get(s1) or {}
                d2 = lp.get(s2) or {}
                dc = lp.get(close_snap) or {}
                if not d1 or not d2 or not dc:
                    continue
                # Modal line at s2 for exact-line comparability.
                line_counts = {}
                for b, pairs in d2.items():
                    for ln, _ in pairs:
                        line_counts[ln] = line_counts.get(ln, 0) + 1
                if not line_counts:
                    continue
                line = max(sorted(line_counts), key=lambda ln: line_counts[ln])
                # Per-book moves at the exact line.
                deltas = {}
                for b, pairs in d2.items():
                    f2 = next((f for ln, f in pairs if ln == line), None)
                    f1 = next((f for ln, f in (d1.get(b) or [])
                               if ln == line), None)
                    if f1 is None or f2 is None:
                        continue
                    deltas[b] = f2 - f1
                if len(deltas) < min_books:
                    continue
                med_abs = statistics.median(abs(v) for v in deltas.values())
                if med_abs < median_thresh:
                    continue
                up = [b for b, v in deltas.items() if v >= move_thresh]
                down = [b for b, v in deltas.items() if v <= -move_thresh]
                if len(up) >= min_books:
                    direction = 1
                elif len(down) >= min_books:
                    direction = -1
                else:
                    continue
                # Outcome: s2 -> close consensus move at the exact line.
                f_s2 = [f for b, pairs in d2.items()
                        for ln, f in pairs if ln == line]
                f_close = [f for b, pairs in dc.items()
                           for ln, f in pairs if ln == line]
                if len(f_s2) < 2 or len(f_close) < 2:
                    continue
                c_s2 = statistics.median(f_s2)
                c_close = statistics.median(f_close)
                move = c_close - c_s2
                o_sign = 1 if move > 0 else (-1 if move < 0 else 0)
                agree = o_sign != 0 and o_sign == direction
                clv = direction * move
                events.append({
                    "game": gkey, "market": market,
                    "s2": s2.isoformat(), "line": line,
                    "direction": direction,
                    "n_books_moved": len(up) if direction == 1 else len(down),
                    "n_books_total": len(deltas),
                    "agree": agree, "clv": clv,
                })
    return events


def analyze_steam(events):
    """Confirmatory tests: binomial on direction, t-test on CLV."""
    by_game_agree = {}
    by_game_clv = {}
    for e in events:
        by_game_agree.setdefault(e["game"], []).append(
            1 if e["agree"] else 0)
        by_game_clv.setdefault(e["game"], []).append(e["clv"])
    agree_vals = [sum(v) / len(v) for v in by_game_agree.values()]
    clv_vals = [sum(v) / len(v) for v in by_game_clv.values()]
    n = len(agree_vals)
    out = {"n_events": len(events), "n_games": n,
           "feasible": n >= MIN_GAMES}
    if not n:
        return out
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
        "n": n, "mean": mean_clv, "t": t_clv, "p_raw": p_clv,
    }
    holm = _holm_bonferroni(
        [("direction", p_dir if p_dir is not None else 1.0),
         ("clv", p_clv if p_clv is not None else 1.0)])
    out["direction"]["p_holm"] = holm["direction"]
    out["direction"]["significant"] = holm["direction"] < ALPHA
    out["clv"]["p_holm"] = holm["clv"]
    out["clv"]["significant"] = holm["clv"] < ALPHA
    return out


def decide_steam(analysis, alpha=ALPHA, practical_clv=PRACTICAL_CLV):
    if not analysis["feasible"]:
        return "infeasible", {"reason": "fewer than %d games" % MIN_GAMES}
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
        return "steam_edge", detail
    return "no_edge", detail


def run_steam(quotes, markets=DEFAULT_MARKETS):
    """Full pipeline on quote dicts. Returns JSON-serializable results."""
    game_series = build_game_series(quotes, markets=markets)
    events = steam_events(game_series, markets=markets)
    analysis = analyze_steam(events)
    verdict, detail = decide_steam(analysis)
    return {
        "preregistration": PREREG_PATH,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "SHADOW",
        "markets": list(markets),
        "n_games": len(game_series),
        "n_events": len(events),
        "analysis": _jsonable(analysis),
        "verdict": verdict,
        "verdict_detail": _jsonable(detail),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--csv", default=None)
    ap.add_argument("--markets", default=",".join(DEFAULT_MARKETS))
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
    results = run_steam(quotes, markets=tuple(args.markets.split(",")))
    with open(args.out, "w") as f:
        json.dump(results, f, default=str)
    print("wrote %s (verdict=%s)" % (args.out, results["verdict"]), flush=True)


if __name__ == "__main__":
    main()
