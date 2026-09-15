#!/usr/bin/env python3
"""Price-pressure descriptive on burned 2022-24 NCAAF data.

Preregistered purpose (investment card 2026-09-15):
- Q1 (same-line convergence): does greater cross-book de-vig fair-probability
  dispersion at window t predict greater convergence at t+1?
- Q2 (directional repricing): does the direction of price pressure at t predict
  subsequent market movement? Totals first (sign-safe). Spreads magnitude-only.

No threshold optimization. No ROI. No best-book/window/season selection.
Report Wed->Fri, Fri->Sat, pooled, by season for stability.

Read-only. Zero API credits. 2025 sealed.
"""
import os, sys, json, argparse, math, statistics
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402
from market_structure import (build_plan, match_window, WINDOW_LABELS,
                              FROZEN_FINGERPRINT)

WINDOW_ORDER = ["early", "mid", "late"]
TRANSITIONS = [("early", "mid"), ("mid", "late")]  # Wed->Fri, Fri->Sat


def _pct(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/price_pressure_descriptive.json")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",")]
    assert 2025 not in seasons, "2025 sealed"

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL required", file=sys.stderr)
        return 2
    hq = sports.historical_quotes_table(args.sport)
    cols = ("provider_event_id, kickoff, book_key, market, selection, "
            "line, american_odds, fair_probability, observed_at")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "kickoff", "book_key", "market", "selection",
         "line", "american_odds", "fair_probability", "observed_at"], r))
        for r in rows]

    plan = build_plan(args.sport, seasons)

    # Match each quote to (season, week, window)
    # key: (event_id, season, week, window, market, selection, line) -> list of (book, fair_prob)
    contracts = defaultdict(list)
    # key: (event_id, season, week, window, market) -> list of (book, line, selection, fair_prob)
    events = defaultdict(list)
    for r in recs:
        if r["market"] not in ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL"):
            continue
        if r["fair_probability"] is None:
            continue
        t = match_window(r["observed_at"], plan)
        if t is None:
            continue
        season, week, window = plan[t]
        if season not in seasons:
            continue
        ck = (r["provider_event_id"], season, week, window,
              r["market"], r["selection"], float(r["line"]))
        contracts[ck].append((r["book_key"], float(r["fair_probability"])))
        ek = (r["provider_event_id"], season, week, window, r["market"])
        events[ek].append((r["book_key"], float(r["line"]),
                           r["selection"], float(r["fair_probability"])))

    # ── Q1: same-line convergence ──
    # For each exact contract at t and t+1: dispersion_t, dispersion_t1
    q1 = {tr: [] for tr in TRANSITIONS}
    q1_by_season = defaultdict(lambda: {tr: [] for tr in TRANSITIONS})
    for ck, quotes in contracts.items():
        eid, season, week, window, market, sel, line = ck
        books = {b for b, _ in quotes}
        if len(books) < 3:
            continue
        probs = [p for _, p in quotes]
        disp = statistics.pstdev(probs) if len(probs) > 1 else 0.0
        # find next window
        wi = WINDOW_ORDER.index(window)
        if wi >= len(WINDOW_ORDER) - 1:
            continue
        next_w = WINDOW_ORDER[wi + 1]
        ck2 = (eid, season, week, next_w, market, sel, line)
        q2 = contracts.get(ck2)
        if not q2:
            continue
        books2 = {b for b, _ in q2}
        if len(books2) < 3:
            continue
        probs2 = [p for _, p in q2]
        disp2 = statistics.pstdev(probs2) if len(probs2) > 1 else 0.0
        conv = disp - disp2  # positive = converged
        tr = (window, next_w)
        q1[tr].append((disp, conv, market))
        q1_by_season[season][tr].append((disp, conv, market))

    def summarize_q1(pairs):
        if len(pairs) < 30:
            return {"n": len(pairs), "insufficient": True}
        # quartile bins on dispersion
        disps = sorted(p[0] for p in pairs)
        q25, q50, q75 = _pct(disps, 25), _pct(disps, 50), _pct(disps, 75)
        bins = {"q1_low": [], "q2": [], "q3": [], "q4_high": []}
        for d, c, m in pairs:
            if d <= q25:
                bins["q1_low"].append(c)
            elif d <= q50:
                bins["q2"].append(c)
            elif d <= q75:
                bins["q3"].append(c)
            else:
                bins["q4_high"].append(c)
        # correlation between dispersion and convergence
        n = len(pairs)
        md = sum(p[0] for p in pairs) / n
        mc = sum(p[1] for p in pairs) / n
        cov = sum((p[0] - md) * (p[1] - mc) for p in pairs) / n
        vd = sum((p[0] - md) ** 2 for p in pairs) / n
        vc = sum((p[1] - mc) ** 2 for p in pairs) / n
        corr = cov / math.sqrt(vd * vc) if vd > 0 and vc > 0 else 0.0
        return {
            "n": n,
            "dispersion_quartiles": [round(q25, 4), round(q50, 4), round(q75, 4)],
            "mean_convergence_by_dispersion_bin": {
                k: round(statistics.fmean(v), 5) if v else None
                for k, v in bins.items()},
            "n_by_bin": {k: len(v) for k, v in bins.items()},
            "correlation_dispersion_convergence": round(corr, 4),
            "mean_convergence_overall": round(mc, 5),
        }

    # ── Q2: directional repricing (totals; sign-safe) ──
    # For each event at t: consensus line, Over pressure. At t+1: line move.
    q2 = {tr: [] for tr in TRANSITIONS}
    for ek, quotes in events.items():
        eid, season, week, window, market = ek
        if market != "FULL_GAME_TOTAL":
            continue
        wi = WINDOW_ORDER.index(window)
        if wi >= len(WINDOW_ORDER) - 1:
            continue
        next_w = WINDOW_ORDER[wi + 1]
        ek2 = (eid, season, week, next_w, market)
        q2q = events.get(ek2)
        if not q2q:
            continue
        # consensus line = median
        lines_t = [ln for _, ln, _, _ in quotes]
        lines_t1 = [ln for _, ln, _, _ in q2q]
        if len(set(b for b, _, _, _ in quotes)) < 3:
            continue
        cl_t = statistics.median(lines_t)
        cl_t1 = statistics.median(lines_t1)
        # Over pressure: mean Over fair_prob at lines near consensus, minus 0.5
        over_ps = [fp for _, ln, sel, fp in quotes
                   if sel == "Over" and abs(ln - cl_t) < 0.01]
        if len(over_ps) < 3:
            continue
        pressure = statistics.fmean(over_ps) - 0.5
        move = cl_t1 - cl_t
        tr = (window, next_w)
        q2[tr].append((pressure, move))

    def summarize_q2(pairs):
        if len(pairs) < 30:
            return {"n": len(pairs), "insufficient": True}
        # does sign(pressure) predict sign(move)?
        hits = sum(1 for p, m in pairs
                   if (p > 0 and m > 0) or (p < 0 and m < 0))
        moves = sum(1 for _, m in pairs if m != 0)
        # correlation
        n = len(pairs)
        mp = sum(p[0] for p in pairs) / n
        mm = sum(p[1] for p in pairs) / n
        cov = sum((p[0] - mp) * (p[1] - mm) for p in pairs) / n
        vp = sum((p[0] - mp) ** 2 for p in pairs) / n
        vm = sum((p[1] - mm) ** 2 for p in pairs) / n
        corr = cov / math.sqrt(vp * vm) if vp > 0 and vm > 0 else 0.0
        # binned by |pressure|
        absps = sorted(abs(p[0]) for p in pairs)
        med = _pct(absps, 50)
        strong = [(p, m) for p, m in pairs if abs(p) >= med]
        weak = [(p, m) for p, m in pairs if abs(p) < med]
        def hitrate(ps):
            h = sum(1 for p, m in ps if (p > 0 and m > 0) or (p < 0 and m < 0))
            mv = sum(1 for _, m in ps if m != 0)
            return round(h / mv, 4) if mv else None
        return {
            "n": n, "n_with_move": moves,
            "directional_hit_rate": hitrate(pairs),
            "hit_rate_strong_pressure": hitrate(strong),
            "hit_rate_weak_pressure": hitrate(weak),
            "correlation_pressure_move": round(corr, 4),
            "mean_abs_move": round(statistics.fmean(abs(p[1]) for p in pairs), 3),
        }

    # ── Q3: line migration rate (totals + spreads, consensus) ──
    mig = {tr: {"n": 0, "migrated": 0} for tr in TRANSITIONS}
    for ek, quotes in events.items():
        eid, season, week, window, market = ek
        wi = WINDOW_ORDER.index(window)
        if wi >= len(WINDOW_ORDER) - 1:
            continue
        next_w = WINDOW_ORDER[wi + 1]
        ek2 = (eid, season, week, next_w, market)
        q2q = events.get(ek2)
        if not q2q:
            continue
        cl_t = statistics.median(ln for _, ln, _, _ in quotes)
        cl_t1 = statistics.median(ln for _, ln, _, _ in q2q)
        tr = (window, next_w)
        mig[tr]["n"] += 1
        if abs(cl_t1 - cl_t) > 0.01:
            mig[tr]["migrated"] += 1

    result = {
        "preregistration": "nfl-edge/docs/ncaaf-investment-card-price-pressure.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "scope": {"sport": args.sport, "seasons": seasons},
        "q1_same_line_convergence": {
            f"{a}->{b}": summarize_q1(q1[(a, b)]) for a, b in TRANSITIONS},
        "q1_pooled": summarize_q1([p for tr in TRANSITIONS for p in q1[tr]]),
        "q1_by_season": {
            str(s): {f"{a}->{b}": summarize_q1(q1_by_season[s][(a, b)])
                     for a, b in TRANSITIONS}
            for s in sorted(q1_by_season)},
        "q2_totals_directional": {
            f"{a}->{b}": summarize_q2(q2[(a, b)]) for a, b in TRANSITIONS},
        "q2_pooled": summarize_q2([p for tr in TRANSITIONS for p in q2[tr]]),
        "line_migration": {
            f"{a}->{b}": {"n": v["n"],
                          "migration_rate": round(v["migrated"] / v["n"], 4)
                          if v["n"] else None}
            for (a, b), v in mig.items()},
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
