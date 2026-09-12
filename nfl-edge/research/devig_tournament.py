"""De-vig / calibration tournament v1 — LOBO closing-convergence comparison.

Preregistered: docs/preregistrations/devig-tournament-v1.md
Frozen dataset: 2022-2024 weeks 1-18, fingerprint
43f853a44bf93937d85149ca5fd7241b (docs/dataset-freeze.md).

Protocol summary (see preregistration for the full text):
- Unit of analysis is the canonical GAME, not the provider's event id:
  The Odds API re-issues event ids for the same real game across
  snapshots (and kickoff times jitter / move with flex scheduling), so
  provider_event_id is not a stable game identity. Canonical identity
  is (home_team, away_team) + kickoff-proximity clustering: quotes whose
  kickoffs are within GAME_CLUSTER_GAP of each other belong to one game.
  Measured on the frozen dataset: within-game kickoff gaps are all
  <= 48h (flex moves), between-game gaps are all >= 528h (22 days), so
  the 7-day threshold separates them with wide margin on both sides.
- For each game/market, the latest snapshot is the closing snapshot;
  earlier snapshots are prediction snapshots.
- For each (prediction snapshot, held-out book) cell with a valid
  two-sided pair, each candidate de-vig method's fair probability f for
  the reference selection (home side / Over) is compared against the
  closing LOBO consensus for the same method built from the OTHER books
  (median; >= 2 required). The held-out book never evaluates itself.
- Complete-case across methods: a cell counts only if all three methods
  yield valid probabilities for the prediction pair and every
  consensus pair.
- Decision metric: mean squared error vs closing consensus. Inference on
  game-level mean SEs: paired tests on the 3 pairwise differences,
  Holm-Bonferroni at family-wise alpha 0.05. A method wins iff it beats
  BOTH others significantly; otherwise "no method significantly better".

Read-only: SELECTs against nfl_edge_historical_quotes only. Zero API
credits, no provider HTTP calls. Pure core (run_tournament) takes quote
dicts and is unit-tested; main() handles DB I/O.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from research.devig import DEVIG_METHODS  # noqa: E402
from model.research.market_features import implied_prob  # noqa: E402

PREREG_PATH = "docs/preregistrations/devig-tournament-v1.md"
DATASET_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
EXPECTED_QUOTES = 291586
EXPECTED_SNAPSHOTS = 162
ALPHA = 0.05

SPREAD = "FULL_GAME_SPREAD"
TOTAL = "FULL_GAME_TOTAL"
MARKETS = (SPREAD, TOTAL)

# Canonical game-identity clustering. The provider re-issues event ids
# for the same real game across snapshots, and kickoff times jitter by
# minutes (data feed) or move by hours/days (flex scheduling), so
# provider_event_id is NOT a stable game identity. Canonical identity is
# (home_team, away_team) plus kickoff-proximity clustering: a gap larger
# than this between consecutive kickoffs of one team pair starts a new
# game. Measured on the frozen dataset (2026-09-12 audit): within-game
# kickoff gaps are all <= 48h, between-game gaps are all >= 528h, so the
# 7-day threshold separates them with wide margin on both sides. NFL
# teams never meet twice within 7 days.
GAME_CLUSTER_GAP = dt.timedelta(days=7)


def _as_utc(kickoff):
    """Normalize a kickoff value to an aware datetime."""
    if isinstance(kickoff, dt.datetime):
        return kickoff if kickoff.tzinfo else kickoff.replace(
            tzinfo=dt.timezone.utc)
    if isinstance(kickoff, str):
        s = kickoff.replace("Z", "+00:00")
        try:
            parsed = dt.datetime.fromisoformat(s)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(
            tzinfo=dt.timezone.utc)
    return None


def canonical_game_keys(quotes):
    """Assign each quote a canonical game key.

    Returns dict mapping id(quote) -> game key string. Quotes missing
    home/away/kickoff are skipped (absent from the map).
    """
    by_pair: dict[tuple, list] = collections.defaultdict(list)
    for q in quotes:
        home, away = q.get("home_team"), q.get("away_team")
        ko = _as_utc(q.get("kickoff"))
        if not home or not away or ko is None:
            continue
        by_pair[(home, away)].append((ko, id(q)))
    key_of: dict[int, str] = {}
    for (home, away), items in by_pair.items():
        items.sort(key=lambda t: t[0])
        cluster_idx = 0
        cluster_start = items[0][0]
        prev = items[0][0]
        stamp = {}
        for ko, qid in items:
            if ko - prev > GAME_CLUSTER_GAP:
                cluster_idx += 1
                cluster_start = ko
            stamp[qid] = (cluster_idx, cluster_start)
            prev = ko
        for _, qid in items:
            idx, start = stamp[qid]
            key_of[qid] = "%s|%s|%s#%d" % (
                home, away, start.strftime("%Y%m%d"), idx)
    return key_of


def _ref_other_selections(market, home_team, away_team):
    if market == SPREAD:
        return home_team, away_team
    if market == TOTAL:
        return "Over", "Under"
    if market == "FULL_GAME_MONEYLINE":
        return home_team, away_team
    return None, None


def _book_triples(snap_quotes, book_key, market, home_team, away_team):
    """Valid (line, p_ref, p_other) triples for one book at one snapshot."""
    ref_sel, other_sel = _ref_other_selections(market, home_team, away_team)
    if ref_sel is None:
        return []
    by_line: dict[float, dict] = {}
    for q in snap_quotes:
        if q.get("book_key") != book_key or q.get("market") != market:
            continue
        sel = q.get("selection")
        if sel not in (ref_sel, other_sel):
            continue
        try:
            line = float(q.get("line"))
        except (TypeError, ValueError):
            continue
        p = implied_prob(q.get("american_odds"))
        if p is None:
            continue
        by_line.setdefault(line, {})[sel] = p
    triples = []
    for line, slot in by_line.items():
        if ref_sel in slot and other_sel in slot:
            triples.append((line, slot[ref_sel], slot[other_sel]))
    return triples


def _select_pair(triples, all_lines):
    """Deterministic pair choice: modal line; ties -> closest to the
    cross-book median line (then smallest line). Snapshot-local only."""
    if not triples:
        return None
    counts = collections.Counter(t[0] for t in triples)
    best = max(counts.values())
    cands = [t for t in triples if counts[t[0]] == best]
    if len(cands) == 1:
        return cands[0]
    med = statistics.median(all_lines) if all_lines else cands[0][0]
    return min(cands, key=lambda t: (abs(t[0] - med), t[0]))


def _pairs_at_snapshot(ev_quotes, market, snap, home_team, away_team):
    """Map book_key -> (line, p_ref, p_other) for one snapshot."""
    sq = [q for q in ev_quotes
          if q.get("market") == market and q.get("observed_at") == snap]
    books = {q.get("book_key") for q in sq}
    triples: dict = {}
    all_lines = []
    for b in books:
        tr = _book_triples(sq, b, market, home_team, away_team)
        triples[b] = tr
        all_lines.extend(t[0] for t in tr)
    return {b: _select_pair(tr, all_lines) for b, tr in triples.items()}


def _game_market_cells(game_id, ev_quotes, market, home_team, away_team,
                       methods):
    """LOBO cells for one (game, market). Each cell carries per-method
    squared errors of the held-out book's fair prob vs the closing LOBO
    consensus (same method, other books only)."""
    snaps = sorted({q["observed_at"] for q in ev_quotes
                    if q.get("market") == market})
    if len(snaps) < 2:
        return []
    close = snaps[-1]
    pair_close = _pairs_at_snapshot(ev_quotes, market, close,
                                    home_team, away_team)
    provider_ids = sorted({q.get("provider_event_id") for q in ev_quotes
                           if q.get("provider_event_id")})
    cells = []
    for s in snaps[:-1]:
        pair_s = _pairs_at_snapshot(ev_quotes, market, s,
                                    home_team, away_team)
        for b in sorted(pair_s):
            ps = pair_s[b]
            if ps is None:
                continue
            others = sorted(bb for bb in pair_close
                            if bb != b and pair_close[bb] is not None)
            if len(others) < 2:
                continue
            f, cons = {}, {}
            ok = True
            for mname, mfn in methods.items():
                r = mfn(ps[1], ps[2])
                if r is None:
                    ok = False
                    break
                f[mname] = r[0]
            if ok:
                for mname, mfn in methods.items():
                    vals = []
                    for bb in others:
                        pc = pair_close[bb]
                        r = mfn(pc[1], pc[2])
                        if r is None:
                            ok = False
                            break
                        vals.append(r[0])
                    if not ok:
                        break
                    cons[mname] = statistics.median(vals)
            if not ok:
                continue
            snap_iso = s.isoformat() if hasattr(s, "isoformat") else str(s)
            cells.append({
                "game_id": game_id,
                "provider_event_ids": provider_ids,
                "market": market,
                "snapshot": snap_iso,
                "book": b,
                "n_consensus_books": len(others),
                "se": {m: (f[m] - cons[m]) ** 2 for m in methods},
                "f": dict(f),
                "consensus": dict(cons),
            })
    return cells


def _paired_test(diffs):
    """Two-sided paired test via normal approximation (valid at the
    observed game counts; numerically identical to the paired t at
    n >> 30). Returns (mean_diff, z, p_value) or None if n < 2."""
    n = len(diffs)
    if n < 2:
        return None
    mean = statistics.fmean(diffs)
    sd = statistics.pstdev(diffs)
    if sd == 0:
        return (mean, 0.0, 1.0 if mean == 0 else 0.0)
    z = mean / (sd / math.sqrt(n))
    p = math.erfc(abs(z) / math.sqrt(2.0))
    return mean, z, p


def _holm_bonferroni(pvals):
    """Holm-Bonferroni adjusted p-values. pvals: list of (key, p).
    Returns dict key -> adjusted p."""
    m = len(pvals)
    ordered = sorted(pvals, key=lambda kv: kv[1])
    adj = {}
    running_max = 0.0
    for i, (key, p) in enumerate(ordered, start=1):
        running_max = max(running_max, min(1.0, (m - i + 1) * p))
        adj[key] = running_max
    return adj


def run_tournament(quotes, methods=None):
    """Pure tournament core. quotes: iterable of quote dicts shaped like
    nfl_edge_historical_quotes rows. Returns (results, cells); results is
    JSON-serializable, cells are the per-cell records."""
    methods = dict(methods or DEVIG_METHODS)
    mnames = sorted(methods)
    # Unit of analysis: the canonical game (see canonical_game_keys).
    # provider_event_id is unstable across snapshots and must NOT be
    # used as the grouping key.
    quotes = list(quotes)
    key_of = canonical_game_keys(quotes)
    by_game: dict = collections.defaultdict(list)
    for q in quotes:
        if q.get("market") in MARKETS and id(q) in key_of:
            by_game[key_of[id(q)]].append(q)
    cells = []
    for game_id, ev_quotes in by_game.items():
        q0 = ev_quotes[0]
        home_team, away_team = q0.get("home_team"), q0.get("away_team")
        for market in MARKETS:
            cells.extend(_game_market_cells(
                game_id, ev_quotes, market, home_team, away_team, methods))
    # per-method aggregates over cells
    method_stats = {}
    for m in mnames:
        ses = [c["se"][m] for c in cells]
        method_stats[m] = {
            "n_cells": len(ses),
            "mean_se": statistics.fmean(ses) if ses else None,
            "rmse": math.sqrt(statistics.fmean(ses)) if ses else None,
        }
    # game-level mean SEs for inference (absorbs within-game correlation)
    per_game: dict = collections.defaultdict(lambda: {m: [] for m in mnames})
    for c in cells:
        for m in mnames:
            per_game[c["game_id"]][m].append(c["se"][m])
    game_means = {g: {m: statistics.fmean(v[m]) for m in mnames}
                  for g, v in per_game.items()}
    # pairwise comparisons on game-level differences
    pairs = []
    for i in range(len(mnames)):
        for j in range(i + 1, len(mnames)):
            a, b = mnames[i], mnames[j]
            diffs = [game_means[g][a] - game_means[g][b]
                     for g in game_means]
            t = _paired_test(diffs)
            pairs.append({
                "a": a, "b": b,
                "n_games": len(diffs),
                "mean_diff_a_minus_b": t[0] if t else None,
                "z": t[1] if t else None,
                "p_raw": t[2] if t else None,
            })
    adj = _holm_bonferroni([(p["a"] + "_vs_" + p["b"], p["p_raw"])
                            for p in pairs if p["p_raw"] is not None])
    for p in pairs:
        key = p["a"] + "_vs_" + p["b"]
        p["p_holm"] = adj.get(key)
        p["significant"] = (p["p_holm"] is not None
                            and p["p_holm"] < ALPHA)
    # decision rule: winner beats BOTH others significantly
    beats = {m: set() for m in mnames}
    for p in pairs:
        if not p["significant"]:
            continue
        if p["mean_diff_a_minus_b"] < 0:
            beats[p["a"]].add(p["b"])
        elif p["mean_diff_a_minus_b"] > 0:
            beats[p["b"]].add(p["a"])
    winners = [m for m in mnames
               if beats[m] == set(mnames) - {m}]
    decision = (winners[0] if len(winners) == 1
                else "no method significantly better")
    # exploratory: calibration deciles of f vs mean consensus, per method
    calibration = {}
    for m in mnames:
        pts = sorted((c["f"][m], c["consensus"][m]) for c in cells)
        deciles = []
        n = len(pts)
        for d in range(10):
            chunk = pts[d * n // 10:(d + 1) * n // 10]
            if chunk:
                deciles.append({
                    "decile": d + 1,
                    "n": len(chunk),
                    "mean_f": statistics.fmean(p[0] for p in chunk),
                    "mean_consensus": statistics.fmean(p[1] for p in chunk),
                })
        calibration[m] = deciles
    # exploratory: per-market and per-book mean SE
    by_market, by_book = {}, {}
    for m in mnames:
        by_market[m] = {}
        for mk in MARKETS:
            ses = [c["se"][m] for c in cells if c["market"] == mk]
            by_market[m][mk] = {"n_cells": len(ses),
                                "mean_se": statistics.fmean(ses) if ses
                                else None}
        by_book[m] = {}
        for c in cells:
            by_book[m].setdefault(c["book"], []).append(c["se"][m])
        by_book[m] = {b: {"n_cells": len(v),
                          "mean_se": statistics.fmean(v)}
                      for b, v in sorted(by_book[m].items())}
    results = {
        "preregistration": PREREG_PATH,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "methods": mnames,
        "n_cells": len(cells),
        "n_games": len(game_means),
        "method_stats": method_stats,
        "pairwise_tests": pairs,
        "multiple_testing": "Holm-Bonferroni, family-wise alpha=0.05, "
                            "3 pairwise comparisons on game-level mean SEs",
        "decision": decision,
        "primary_metric_note": "Mean log-loss vs settled outcomes is "
            "DECLARED UNTESTABLE this round: settled outcomes are not "
            "stored in Neon for the historical dataset.",
        "exploratory": {
            "calibration_deciles": calibration,
            "mean_se_by_market": by_market,
            "mean_se_by_book": by_book,
        },
    }
    return results, cells


def _content_hash(rows):
    h = hashlib.sha256()
    for quote_id, american_odds, line, observed_at in rows:
        h.update(("%s|%s|%s|%s\n" % (
            quote_id, american_odds, line, observed_at)).encode())
    return h.hexdigest()


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Run the preregistered de-vig tournament (read-only).")
    ap.add_argument("--out", required=True,
                    help="Path for the results JSON file.")
    ap.add_argument("--database-url", default=os.environ.get(
        "NFL_EDGE_DATABASE_URL"),
        help="Neon DSN (default: NFL_EDGE_DATABASE_URL env).")
    args = ap.parse_args(argv)
    if not args.database_url:
        raise SystemExit("error: NFL_EDGE_DATABASE_URL is not set")
    import psycopg
    conn = psycopg.connect(args.database_url, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nfl_edge_historical_quotes")
            n_quotes = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT observed_at) "
                        "FROM nfl_edge_historical_quotes")
            n_snaps = cur.fetchone()[0]
            print("freeze check: quotes=%d snapshots=%d"
                  % (n_quotes, n_snaps), flush=True)
            if n_quotes != EXPECTED_QUOTES or n_snaps != EXPECTED_SNAPSHOTS:
                raise SystemExit(
                    "error: dataset does not match the frozen fingerprint "
                    "(quotes=%d expected %d, snapshots=%d expected %d)" % (
                        n_quotes, EXPECTED_QUOTES, n_snaps,
                        EXPECTED_SNAPSHOTS))
            cur.execute("SELECT quote_id, american_odds, line, observed_at "
                        "FROM nfl_edge_historical_quotes "
                        "ORDER BY quote_id")
            content_hash = _content_hash(cur.fetchall())
            print("content sha256: %s" % content_hash, flush=True)
            cur.execute("SELECT quote_id, provider_event_id, home_team, "
                        "away_team, kickoff, book_key, market, selection, "
                        "line, american_odds, observed_at "
                        "FROM nfl_edge_historical_quotes")
            cols = [d[0] for d in cur.description]
            quotes = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
    print("loaded %d quotes" % len(quotes), flush=True)
    results, cells = run_tournament(quotes)
    results["generated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    results["freeze_check"] = {
        "n_quotes": n_quotes,
        "n_snapshots": n_snaps,
        "content_sha256": content_hash,
        "fingerprint_match": content_hash == DATASET_FINGERPRINT,
    }
    results["n_cells_with_details"] = len(cells)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, sort_keys=True)
    print("wrote %s (%d cells, decision: %s)"
          % (args.out, len(cells), results["decision"]), flush=True)


if __name__ == "__main__":
    main()
