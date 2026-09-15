#!/usr/bin/env python3
"""Clustered uncertainty for Q2 totals directional result.

The descriptive reported 67.1% hit rate for strong pressure, but pairs from
the same game/week are not independent. This computes cluster-robust SEs.

Strong pressure definition (from descriptive): |pressure| >= median(|pressure|)
across all Q2 pairs. This definition is FROZEN — not re-optimized.

Read-only. Zero API credits. 2025 sealed.
"""
import os, sys, json, argparse, math, statistics
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402
from market_structure import build_plan, match_window, FROZEN_FINGERPRINT

WINDOW_ORDER = ["early", "mid", "late"]
TRANSITIONS = [("early", "mid"), ("mid", "late")]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/price_pressure_clustered.json")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",")]
    assert 2025 not in seasons

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    hq = sports.historical_quotes_table(args.sport)
    cols = ("provider_event_id, kickoff, book_key, market, selection, "
            "line, fair_probability, observed_at")
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "kickoff", "book_key", "market", "selection",
         "line", "fair_probability", "observed_at"], r)) for r in rows]

    plan = build_plan(args.sport, seasons)
    events = defaultdict(list)
    for r in recs:
        if r["market"] != "FULL_GAME_TOTAL" or r["fair_probability"] is None:
            continue
        t = match_window(r["observed_at"], plan)
        if t is None:
            continue
        season, week, window = plan[t]
        if season not in seasons:
            continue
        ek = (r["provider_event_id"], season, week, window)
        events[ek].append((r["book_key"], float(r["line"]),
                           r["selection"], float(r["fair_probability"])))

    # Rebuild Q2 pairs with cluster keys
    pairs = []  # (pressure, move, cluster_key)
    for ek, quotes in events.items():
        eid, season, week, window = ek
        wi = WINDOW_ORDER.index(window)
        if wi >= len(WINDOW_ORDER) - 1:
            continue
        next_w = WINDOW_ORDER[wi + 1]
        ek2 = (eid, season, week, next_w)
        q2 = events.get(ek2)
        if not q2 or len({b for b, _, _, _ in quotes}) < 3:
            continue
        cl_t = statistics.median(ln for _, ln, _, _ in quotes)
        cl_t1 = statistics.median(ln for _, ln, _, _ in q2)
        over_ps = [fp for _, ln, sel, fp in quotes
                   if sel == "Over" and abs(ln - cl_t) < 0.01]
        if len(over_ps) < 3:
            continue
        pressure = statistics.fmean(over_ps) - 0.5
        move = cl_t1 - cl_t
        if move == 0:
            continue
        cluster = (eid, season, week)  # game-week cluster
        pairs.append((pressure, move, cluster, (window, next_w)))

    # Frozen strong-pressure definition: |pressure| >= median
    absps = sorted(abs(p[0]) for p in pairs)
    n = len(absps)
    med = absps[n // 2] if n % 2 == 1 else (absps[n//2 - 1] + absps[n//2]) / 2

    strong = [(p, m, c, tr) for p, m, c, tr in pairs if abs(p) >= med]

    # Hit indicator
    hits = [1 if (p > 0 and m > 0) or (p < 0 and m < 0) else 0
            for p, m, _, _ in strong]
    n_s = len(hits)
    hr = sum(hits) / n_s if n_s else 0

    # Naive SE
    se_naive = math.sqrt(hr * (1 - hr) / n_s) if n_s else 0

    # Cluster-robust SE: group by cluster, compute cluster means
    clusters = defaultdict(list)
    for (p, m, c, tr), h in zip(strong, hits):
        clusters[c].append(h)
    n_cl = len(clusters)
    # Cluster-robust variance of mean: var = (1/n_cl) * var(cluster_totals / avg_size)...
    # Simpler: treat cluster hit-rates as observations, SE of their mean
    # weighted by cluster size. Use the standard cluster-robust formula:
    # Var(hr) = (1/N^2) * sum_g (sum_{i in g} (h_i - hr))^2 * (G/(G-1))
    # where N = total obs, G = n clusters
    G = n_cl
    N = n_s
    s = sum(sum(h - hr for h in hs) ** 2 for hs in clusters.values())
    var_cl = (G / (G - 1)) * s / (N ** 2) if G > 1 and N > 0 else 0
    se_cl = math.sqrt(var_cl)

    # Unique games and weeks
    uniq_games = len({c[0] for c in clusters})
    uniq_weeks = len({(c[1], c[2]) for c in clusters})

    # Effective sample size approximation
    # n_eff = n / (1 + (avg_cluster_size - 1) * icc)
    # Estimate ICC from cluster variance
    avg_cs = N / G if G else 0

    # By transition
    by_tr = {}
    for tr in TRANSITIONS:
        tr_pairs = [(p, m, c) for p, m, c, t in strong if t == tr]
        tr_hits = [1 if (p > 0 and m > 0) or (p < 0 and m < 0) else 0
                   for p, m, _ in tr_pairs]
        tr_cl = defaultdict(list)
        for (p, m, c), h in zip(tr_pairs, tr_hits):
            tr_cl[c].append(h)
        ntr = len(tr_hits)
        hrtr = sum(tr_hits) / ntr if ntr else 0
        Gtr = len(tr_cl)
        str_ = sum(sum(h - hrtr for h in hs) ** 2 for hs in tr_cl.values())
        vartr = (Gtr / (Gtr - 1)) * str_ / (ntr ** 2) if Gtr > 1 and ntr > 0 else 0
        by_tr[f"{tr[0]}->{tr[1]}"] = {
            "n_pairs": ntr,
            "n_clusters": Gtr,
            "hit_rate": round(hrtr, 4),
            "se_clustered": round(math.sqrt(vartr), 4),
            "ci95": [round(hrtr - 1.96 * math.sqrt(vartr), 4),
                     round(hrtr + 1.96 * math.sqrt(vartr), 4)],
        }

    result = {
        "definition_frozen": ("strong pressure = |pressure| >= median(|pressure|) "
                              f"= {med:.5f}, from descriptive run 35015417472"),
        "n_pairs_strong": n_s,
        "n_clusters_game_week": n_cl,
        "n_unique_games": uniq_games,
        "avg_pairs_per_cluster": round(avg_cs, 2),
        "hit_rate": round(hr, 4),
        "se_naive": round(se_naive, 4),
        "se_clustered": round(se_cl, 4),
        "ci95_clustered": [round(hr - 1.96 * se_cl, 4),
                           round(hr + 1.96 * se_cl, 4)],
        "ci95_naive": [round(hr - 1.96 * se_naive, 4),
                       round(hr + 1.96 * se_naive, 4)],
        "by_transition": by_tr,
        "null_50pct_z_clustered": round((hr - 0.5) / se_cl, 2) if se_cl > 0 else None,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
