#!/usr/bin/env python3
"""Phase F: market-structure map of the frozen NCAAF dataset (2022-24 only).

Preregistration: docs/preregistrations/ncaaf-market-structure-f1.md
Read-only SELECTs against ncaaf_edge_historical_quotes. Zero API credits.

Usage:
    NFL_EDGE_DATABASE_URL=... python nfl-edge/ops/market_structure.py \
        --sport ncaaf --seasons 2022,2023,2024 --out /tmp/market_structure.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"
MATCH_TOL_S = 1800

# (weekday, hour) -> window label, from the historical cadence.
WINDOW_LABELS = {(2, 12): "early", (4, 20): "mid", (5, 15): "late"}

SPREAD_BUCKETS = [(0, 3), (3, 7), (7, 14), (14, float("inf"))]
TOTAL_BUCKETS = [(float("-inf"), 45), (45, 55), (55, 65), (65, float("inf"))]


def _pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _dist(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "mean": statistics.fmean(xs),
            "p50": _pct(xs, 50), "p90": _pct(xs, 90),
            "min": min(xs), "max": max(xs)}


def _bucket(v, buckets):
    for lo, hi in buckets:
        if lo <= v < hi:
            return f"[{lo},{hi})"
    return "other"


def build_plan(sport, seasons):
    """requested_at -> (season, week, window_label)."""
    plan = {}
    for s in seasons:
        for w in range(0, 15):
            for t in sports.snapshot_times(sport, s, w):
                label = WINDOW_LABELS.get((t.weekday(), t.hour),
                                          f"w{t.weekday()}h{t.hour}")
                plan[t] = (s, w, label)
    return plan


def match_window(observed_at, plan, sorted_times=None):
    """Nearest planned instant within MATCH_TOL_S, else None.

    sorted_times: precomputed sorted list of (epoch, instant) tuples.
    """
    import bisect
    if sorted_times is None:
        sorted_times = sorted((t.timestamp(), t) for t in plan)
    epochs = [e for e, _ in sorted_times]
    ts = observed_at.timestamp()
    i = bisect.bisect_left(epochs, ts)
    best, best_dt = None, None
    for j in (i - 1, i):
        if 0 <= j < len(sorted_times):
            e, t = sorted_times[j]
            d = abs(e - ts)
            if d < MATCH_TOL_S and (best_dt is None or d < best_dt):
                best, best_dt = t, d
    return best


def kickoff_season(kickoff):
    """Season a game belongs to: kickoff in Aug-Dec -> that year,
    Jan-Jul -> prior year (bowls)."""
    return kickoff.year if kickoff.month >= 8 else kickoff.year - 1


def select_pairs(rows, plan, seasons):
    """Group quotes -> one selected pair per (event, window, book, market).

    Rows whose kickoff-derived season is outside `seasons` are excluded
    before window matching (2025 never enters the analysis).

    Returns (selected, integrity, matched_instants) where selected maps
    (event_id, season, week, window, book, market) ->
    {"line": ref_line, "fair_prob": ref_fair_prob,
     "home": home, "away": away, "kickoff": kickoff}.
    """
    integrity = {"unmatched_quotes": 0, "identity_violations": 0,
                 "events_dropped": 0, "quotes_read": len(rows),
                 "excluded_by_season": 0}
    rows = [r for r in rows if kickoff_season(r["kickoff"]) in seasons]
    integrity["excluded_by_season"] = integrity["quotes_read"] - len(rows)

    # match windows first; identity is validated per (event_id, plan season)
    matched = []
    sorted_times = sorted((t.timestamp(), t) for t in plan)
    for r in rows:
        t = match_window(r["observed_at"], plan, sorted_times)
        if t is None:
            integrity["unmatched_quotes"] += 1
            continue
        season, week, window = plan[t]
        matched.append((r, season, week, window, t))

    # event identity validation, scoped per (event_id, season)
    ident = {}
    bad = set()
    for r, season, _, _, _ in matched:
        eid = r["provider_event_id"]
        key = (r["home_team"], r["away_team"], str(r["kickoff"]))
        skey = (eid, season)
        if skey in ident and ident[skey] != key:
            bad.add(skey)
        ident[skey] = key
    integrity["identity_violations"] = len(bad)
    integrity["events_dropped"] = len(bad)

    grouped = {}
    matched_instants = set()
    for r, season, week, window, t in matched:
        if (r["provider_event_id"], season) in bad:
            continue
        matched_instants.add(t)
        key = (r["provider_event_id"], season, week, window,
               r["book_key"], r["market"])
        grouped.setdefault(key, []).append(r)

    selected = {}
    for key, rs in grouped.items():
        eid, season, week, window, book, market = key
        # cross-book median line at this (event, window, market), for ties
        # (computed in a second pass below; first pass collects lines)
        by_line = {}
        for r in rs:
            by_line.setdefault(round(float(r["line"]), 3), []).append(r)
        # modal line = most rows; tie -> closest to cross-book median
        # (median computed from all books' modal candidates; approximate
        #  with median of per-book modal lines here for determinism)
        counts = sorted(by_line.items(), key=lambda kv: -len(kv[1]))
        top_n = len(counts[0][1])
        candidates = [ln for ln, lst in counts if len(lst) == top_n]
        if len(candidates) == 1:
            chosen_line = candidates[0]
        else:
            # cross-book median of each book's own modal line
            med = statistics.median(candidates)
            chosen_line = min(candidates, key=lambda ln: (abs(ln - med), ln))
        cand_rows = by_line[chosen_line]
        # need both selections of a pair
        if market == "FULL_GAME_TOTAL":
            want = {"Over", "Under"}
            ref_sel = "Over"
        else:
            home = rs[0]["home_team"]
            away = rs[0]["away_team"]
            want = {home, away}
            ref_sel = home
        have = {}
        for r in cand_rows:
            if r["selection"] in want:
                # latest observed_at wins on duplicates
                if (r["selection"] not in have or
                        r["observed_at"] > have[r["selection"]]["observed_at"]):
                    have[r["selection"]] = r
        if set(have) != want:
            continue
        ref = have[ref_sel]
        selected[key] = {
            "line": float(ref["line"]),
            "fair_prob": float(ref["fair_probability"]),
            "home": ref["home_team"], "away": ref["away_team"],
            "kickoff": str(ref["kickoff"]),
        }
    integrity["quotes_used"] = sum(1 for _ in selected)
    return selected, integrity, matched_instants


def analyze(selected, integrity, seasons):
    out = {"integrity": integrity}
    # ---- coverage ----
    # (event, window) -> n books; per (season, market)
    ew_books = {}
    book_windows = {}
    for (eid, season, week, window, book, market) in selected:
        ew_books.setdefault((eid, season, week, window, market), set()).add(book)
        book_windows.setdefault((book, season, window), set()).add(eid)
    # simpler: coverage by (season, market): share of event-windows with >=k books
    cov_cells = {}
    for (eid, season, week, window, market), books in ew_books.items():
        cov_cells.setdefault((season, market), []).append(len(books))
    coverage = {}
    for (season, market), ns in sorted(cov_cells.items()):
        coverage[f"{season}/{market}"] = {
            "event_windows": len(ns),
            "books_ge1": sum(1 for n in ns if n >= 1) / len(ns),
            "books_ge5": sum(1 for n in ns if n >= 5) / len(ns),
            "books_ge10": sum(1 for n in ns if n >= 10) / len(ns),
            "mean_books": statistics.fmean(ns),
        }
    out["coverage"] = coverage
    out["book_window_presence"] = {
        f"{b}/{s}/{w}": len(evs)
        for (b, s, w), evs in sorted(book_windows.items())}

    # ---- disagreement ----
    # per (game, window, market): std of ref fair prob, range of lines
    gwm = {}
    for (eid, season, week, window, book, market), v in selected.items():
        gwm.setdefault((eid, season, week, window, market), []).append(
            (book, v["line"], v["fair_prob"]))
    dis_cells = {}
    for (eid, season, week, window, market), lst in gwm.items():
        if len(lst) < 2:
            continue
        lines = [l for _, l, _ in lst]
        fps = [f for _, _, f in lst]
        med = statistics.median(lines)
        buckets = SPREAD_BUCKETS if market == "FULL_GAME_SPREAD" else TOTAL_BUCKETS
        cell = (market, window, season, _bucket(abs(med), buckets)
                if market == "FULL_GAME_SPREAD"
                else _bucket(med, buckets))
        dis_cells.setdefault(cell, {"std_fp": [], "range_line": []})
        dis_cells[cell]["std_fp"].append(statistics.pstdev(fps))
        dis_cells[cell]["range_line"].append(max(lines) - min(lines))
    out["disagreement"] = {
        f"{m}/{w}/s{s}/{b}": {"std_fair_prob": _dist(v["std_fp"]),
                              "range_line": _dist(v["range_line"])}
        for (m, w, s, b), v in sorted(dis_cells.items())}

    # ---- line movement ----
    # per (event, market, book) with early+mid+late
    emb = {}
    for (eid, season, week, window, book, market), v in selected.items():
        emb.setdefault((eid, season, week, market, book), {})[window] = v["line"]
    move = {"FULL_GAME_SPREAD": [], "FULL_GAME_TOTAL": []}
    cons_move = {"FULL_GAME_SPREAD": [], "FULL_GAME_TOTAL": []}
    for (eid, season, week, market, book), wv in emb.items():
        if set(wv) >= {"early", "mid", "late"}:
            move[market].append(abs(wv["late"] - wv["early"]))
    # consensus movement
    cons = {}
    for (eid, season, week, window, market), lst in gwm.items():
        cons.setdefault((eid, season, week, market), {})[window] = \
            statistics.median([l for _, l, _ in lst])
    for (eid, season, week, market), wv in cons.items():
        if set(wv) >= {"early", "mid", "late"}:
            cons_move[market].append(abs(wv["late"] - wv["early"]))
    out["movement"] = {
        m: {"book_level_abs_late_minus_early": _dist(v),
            "consensus_abs_late_minus_early": _dist(cons_move[m])}
        for m, v in move.items()}

    # ---- pinnacle vs consensus ----
    pin_diff, other_diff = [], []
    for (eid, season, week, window, market), lst in gwm.items():
        pin = [l for b, l, _ in lst if b == "pinnacle"]
        if not pin:
            continue
        med = statistics.median([l for _, l, _ in lst])
        pin_diff.append(abs(pin[0] - med))
        others = [abs(l - med) for b, l, _ in lst if b != "pinnacle"]
        if others:
            other_diff.append(statistics.fmean(others))
    paired = [p - o for p, o in zip(pin_diff, other_diff)]
    out["pinnacle_vs_consensus"] = {
        "n_event_windows": len(pin_diff),
        "abs_pinnacle_deviation": _dist(pin_diff),
        "mean_abs_other_deviation": _dist(other_diff),
        "paired_diff_pinnacle_minus_other": _dist(paired),
    }
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/market_structure.json")
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]

    plan = build_plan(args.sport, seasons)
    expected_snapshots = len(plan)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    hq = sports.historical_quotes_table(args.sport)
    cols = ("provider_event_id, home_team, away_team, kickoff, book_key,"
            " market, selection, line, american_odds, fair_probability,"
            " observed_at")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "home_team", "away_team", "kickoff",
         "book_key", "market", "selection", "line", "american_odds",
         "fair_probability", "observed_at"], r)) for r in rows]

    selected, integrity, matched = select_pairs(recs, plan, seasons)
    freeze_ok = len(matched) == expected_snapshots

    result = {
        "preregistration": "docs/preregistrations/ncaaf-market-structure-f1.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "scope": {"sport": args.sport, "seasons": seasons,
                  "markets": ["FULL_GAME_SPREAD", "FULL_GAME_TOTAL"],
                  "windows": ["early", "mid", "late"]},
        "freeze_check": {"matched_snapshots": len(matched),
                         "expected_snapshots": expected_snapshots,
                         "pass": freeze_ok},
        **analyze(selected, integrity, seasons),
    }
    if not freeze_ok:
        print(f"FREEZE CHECK FAILED: {len(matched)} != {expected_snapshots}",
              file=sys.stderr)
        return 1
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"wrote {args.out}: "
          f"{integrity['quotes_read']} quotes read, "
          f"{len(selected)} selected pairs, "
          f"{len(matched)}/{expected_snapshots} snapshots matched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
