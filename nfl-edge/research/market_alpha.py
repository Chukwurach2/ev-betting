"""Market-alpha v1: Pinnacle lead/lag + stale-price tracks.

Preregistered: docs/preregistrations/market-alpha-v1.md (written before
any execution). Frozen dataset: 2022-2024 weeks 1-18, fingerprint
43f853a44bf93937d85149ca5fd7241b (docs/dataset-freeze.md).

Track A (lead/lag): hypothesis "Pinnacle moves first; other books
converge toward Pinnacle's earlier price in later snapshots." A Pinnacle
move (|Delta no-vig fair prob| >= 0.01 between consecutive snapshots of
the same canonical game) defines an event; each follower book's next
snapshot change is tested for directional agreement (one-sided binomial
vs 0.5, Holm across the 5 majors). Falsification: the reverse test
(follower leads Pinnacle) must not be significant.

Track B (stale prices): hypothesis "at a given snapshot, some book's
quote is stale relative to the cross-book LOBO consensus, and betting
the stale side would have beaten the closing consensus." Stale =
|f_b - LOBO_consensus_excl_b| >= 0.02 at the same snapshot, consensus
formed at the exact same line (Amendment A1). Inference: directional lag
test -- a stale book's deviation points opposite the market's move, using
a split consensus (dev vs 2 books, move vs 2 disjoint books) so the null
is exactly 50/50 (Amendment A1). The preregistered t-test was found
miscalibrated under the noise null (selection bias) by null simulation
during implementation and is reported superseded.

Game identity reuses canonical_game_keys from research.devig_tournament
((home, away) + kickoff-proximity clustering); provider event ids are
never used as identity. Pair selection, reference selection, and
multiplicative de-vig follow the de-vig tournament. Target book never
enters its own consensus. All snapshots are pre-kickoff; closing
snapshots are ex-post benchmarks only, never signal inputs.

Read-only: SELECTs against nfl_edge_historical_quotes only. Zero API
credits, no provider HTTP calls. Pure core functions take quote dicts
and are unit-tested; main() handles I/O.
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
from research.devig import devig_multiplicative  # noqa: E402
from research.devig_tournament import (  # noqa: E402
    _book_triples,
    _holm_bonferroni,
    _pairs_at_snapshot,
    canonical_game_keys,
)

PREREG_PATH = "docs/preregistrations/market-alpha-v1.md"
DATASET_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
EXPECTED_QUOTES = 291586
EXPECTED_SNAPSHOTS = 162
ALPHA = 0.05

SPREAD = "FULL_GAME_SPREAD"
TOTAL = "FULL_GAME_TOTAL"
MARKETS = (SPREAD, TOTAL)

PINNACLE = "pinnacle"
FOLLOWERS = ("draftkings", "fanduel", "betmgm", "betrivers", "williamhill_us")

# Preregistered constants.
MOVE_THRESH = 0.01      # |Delta f| that counts as a "move"
RESP_MIN = 0.002        # follower |Delta| below this = "no response" (excluded)
DENOM_MIN = 0.005       # min |f_B(t) - f_pin(t)| for the catch-up fraction
MIN_EVENTS = 30         # min eligible events per book, else infeasible
STALE_THRESH = 0.02     # |f_b - LOBO consensus| that flags a stale quote
PRACTICAL_EDGE = 0.01   # min mean game-level edge (prob. points) to matter
MIN_CONSENSUS_BOOKS = 2  # other books required for any LOBO consensus
LAG_MOVE_THRESH = 0.01  # |D| that counts as a market move (Amendment A1)
MIN_LAG_UNITS = 30  # valid flagged units required, else infeasible (A1)


def _as_utc(value):
    """Normalize a datetime/ISO string to an aware UTC datetime."""
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
    return None


def _fair_ref(triple):
    """Multiplicative no-vig fair prob of the reference selection.

    triple = (line, p_ref, p_other) as returned by _pairs_at_snapshot.
    Returns float or None.
    """
    if triple is None:
        return None
    _, p_ref, p_other = triple
    r = devig_multiplicative(p_ref, p_other)
    return r[0] if r else None


def _all_pairs_at_snapshot(ev_quotes, market, snap, home_team, away_team):
    """Map book_key -> list of (line, f_ref) for every valid two-sided line.

    Same pairing rules as _pairs_at_snapshot, but keeps all of a book's
    lines instead of only the selected modal line. Used by Track B so the
    stale consensus is always formed at the exact same line.
    """
    sq = [q for q in ev_quotes
          if q.get("market") == market and q.get("observed_at") == snap]
    out = {}
    for b in {q.get("book_key") for q in sq}:
        pairs = []
        for line, p_ref, p_other in _book_triples(sq, b, market,
                                                  home_team, away_team):
            r = devig_multiplicative(p_ref, p_other)
            if r:
                pairs.append((line, r[0]))
        if pairs:
            out[b] = pairs
    return out


def build_game_series(quotes, markets=None):
    """Group quotes into canonical games with per-book fair-prob series.

    Returns dict: game_key -> {
        "home_team": str, "away_team": str,
        "kickoff": median aware-UTC kickoff across the game's quotes,
        "series": {market: {snapshot: {book: (line, f_ref)}}},
        "line_pairs": {market: {snapshot: {book: [(line, f_ref), ...]}}},
    }
    "series" holds each book's selected pair (Track A). "line_pairs" holds
    every valid two-sided line per book (Track B exact-line consensus).
    Snapshots are aware UTC datetimes sorted ascending. observed_at/kickoff
    are normalized to aware datetimes (CSV input carries ISO strings).
    Quotes that fail canonical keying or yield no valid pair are skipped.
    `markets` defaults to (FULL_GAME_SPREAD, FULL_GAME_TOTAL).
    """
    markets = tuple(markets) if markets else MARKETS
    normed = []
    for q in quotes:
        nq = dict(q)
        nq["observed_at"] = _as_utc(q.get("observed_at"))
        nq["kickoff"] = _as_utc(q.get("kickoff"))
        normed.append(nq)
    key_of = canonical_game_keys(normed)
    by_game = collections.defaultdict(list)
    for q in normed:
        k = key_of.get(id(q))
        if k is not None:
            by_game[k].append(q)
    games = {}
    for gkey, gquotes in by_game.items():
        home = gquotes[0].get("home_team")
        away = gquotes[0].get("away_team")
        kos = sorted(q["kickoff"] for q in gquotes if q.get("kickoff"))
        kickoff = kos[len(kos) // 2] if kos else None
        series, line_pairs = {}, {}
        for market in markets:
            snaps = sorted({
                q.get("observed_at")
                for q in gquotes
                if q.get("market") == market and q.get("observed_at") is not None
            })
            per_snap, per_snap_all = {}, {}
            for s in snaps:
                pairs = _pairs_at_snapshot(gquotes, market, s, home, away)
                books = {}
                for b, triple in pairs.items():
                    f = _fair_ref(triple)
                    if f is not None:
                        books[b] = (triple[0], f)
                if books:
                    per_snap[s] = books
                allp = _all_pairs_at_snapshot(gquotes, market, s, home, away)
                if allp:
                    per_snap_all[s] = allp
            if per_snap:
                series[market] = per_snap
            if per_snap_all:
                line_pairs[market] = per_snap_all
        if series:
            games[gkey] = {"home_team": home, "away_team": away,
                           "kickoff": kickoff,
                           "series": series, "line_pairs": line_pairs}
    return games


def binomial_one_sided_ge(k, n):
    """Exact P(X >= k) for X ~ Binomial(n, 0.5). Returns float or None."""
    if n <= 0:
        return None
    k = max(0, k)
    if k == 0:
        return 1.0
    # Sum the smaller tail for stability, then complement if needed.
    total = 0
    # P(X >= k) directly; n is at most a few thousand here.
    for i in range(k, n + 1):
        total += math.comb(n, i)
    return total / (2 ** n)


def one_sample_one_sided(vals):
    """One-sided one-sample test of mean(vals) > 0 via normal approximation
    (same convention as the tournament's paired test; valid at large n).
    Returns (mean, sd, n, z, p) or None if n < 2.
    """
    n = len(vals)
    if n < 2:
        return None
    mean = statistics.fmean(vals)
    sd = statistics.pstdev(vals)
    if sd == 0:
        return (mean, 0.0, n, 0.0, 1.0 if mean <= 0 else 0.0)
    z = mean / (sd / math.sqrt(n))
    p = 0.5 * math.erfc(z / math.sqrt(2.0))
    return (mean, sd, n, z, p)


# ---------------------------------------------------------------------------
# Track A: lead/lag
# ---------------------------------------------------------------------------

def lead_lag_events(game_series, leader=PINNACLE, followers=FOLLOWERS,
                    move_thresh=MOVE_THRESH, resp_min=RESP_MIN,
                    denom_min=DENOM_MIN):
    """Detect leader moves and follower responses.

    The game's snapshot grid is the sorted distinct observed_at values for
    that game+market. A leader move at grid step i needs the leader's fair
    prob at s[i-1] and s[i]; the follower response needs the follower's
    fair prob at s[i] and s[i+1]. All four observations come from the same
    canonical game and market; the response is measured strictly after the
    move (point-in-time safe).

    Returns a list of events: {
        "game": key, "market": m, "snap_t": iso of s[i],
        "leader_move": float, "direction": +1/-1,
        "followers": {book: {"agree": True/False/None,
                             "catchup": float/None,
                             "delta": float}},
    }
    agree=None means |delta| < resp_min ("no response", excluded from the
    agreement test); catchup=None means the denominator was too small.
    """
    events = []
    for gkey, g in game_series.items():
        for market, per_snap in g["series"].items():
            snaps = sorted(per_snap)
            for i in range(1, len(snaps)):
                s_prev, s_cur = snaps[i - 1], snaps[i]
                if leader not in per_snap[s_prev] or leader not in per_snap[s_cur]:
                    continue
                f_prev = per_snap[s_prev][leader][1]
                f_cur = per_snap[s_cur][leader][1]
                move = f_cur - f_prev
                if abs(move) < move_thresh:
                    continue
                direction = 1 if move > 0 else -1
                if i + 1 >= len(snaps):
                    continue  # no later snapshot for the response
                s_next = snaps[i + 1]
                fols = {}
                for b in followers:
                    if b not in per_snap[s_cur] or b not in per_snap[s_next]:
                        continue
                    f_b_cur = per_snap[s_cur][b][1]
                    f_b_next = per_snap[s_next][b][1]
                    delta = f_b_next - f_b_cur
                    if abs(delta) < resp_min:
                        fols[b] = {"agree": None, "catchup": None,
                                   "delta": delta}
                        continue
                    agree = (delta > 0) == (direction > 0)
                    denom = abs(f_b_cur - f_cur)
                    catchup = None
                    if denom >= denom_min:
                        catchup = 1.0 - abs(f_b_next - f_cur) / denom
                    fols[b] = {"agree": agree, "catchup": catchup,
                               "delta": delta}
                if fols:
                    events.append({
                        "game": gkey, "market": market,
                        "snap_t": s_cur.isoformat(),
                        "leader": leader,
                        "leader_move": move, "direction": direction,
                        "followers": fols,
                    })
    return events


def analyze_lead_lag(events, followers=FOLLOWERS, min_events=MIN_EVENTS,
                     alpha=ALPHA):
    """Per-book agreement tests + pooled test.

    Returns {"books": {b: {...}}, "pooled": {...}} with n, agreements,
    rate, p_raw (one-sided binomial), p_holm, significant, feasible,
    and catch-up fraction mean/median.
    """
    books = {}
    pooled_agree = 0
    pooled_n = 0
    for b in followers:
        agrees = [e["followers"][b]["agree"] for e in events
                  if b in e["followers"]
                  and e["followers"][b]["agree"] is not None]
        n = len(agrees)
        k = sum(1 for a in agrees if a)
        catchups = [e["followers"][b]["catchup"] for e in events
                    if b in e["followers"]
                    and e["followers"][b]["catchup"] is not None]
        feasible = n >= min_events
        p_raw = binomial_one_sided_ge(k, n) if feasible else None
        books[b] = {
            "n_events": n, "n_agree": k,
            "agree_rate": (k / n) if n else None,
            "p_raw": p_raw, "p_holm": None,
            "significant": None, "feasible": feasible,
            "n_no_response": sum(
                1 for e in events if b in e["followers"]
                and e["followers"][b]["agree"] is None),
            "catchup_mean": statistics.fmean(catchups) if catchups else None,
            "catchup_median": statistics.median(catchups) if catchups else None,
            "n_catchup": len(catchups),
        }
        pooled_agree += k
        pooled_n += n
    holm = _holm_bonferroni([(b, books[b]["p_raw"]) for b in followers
                             if books[b]["p_raw"] is not None])
    for b, p_adj in holm.items():
        books[b]["p_holm"] = p_adj
        books[b]["significant"] = p_adj < alpha
    pooled_p = binomial_one_sided_ge(pooled_agree, pooled_n)
    pooled = {"n_events": pooled_n, "n_agree": pooled_agree,
              "agree_rate": (pooled_agree / pooled_n) if pooled_n else None,
              "p": pooled_p,
              "significant": (pooled_p is not None and pooled_p < alpha)}
    return {"books": books, "pooled": pooled}


def decide_track_a(analysis, reverse_analysis, alpha=ALPHA):
    """Apply the preregistered Track A decision rule.

    Returns (verdict, detail). Verdict in {"lead_lag_edge",
    "no_lead_lag_signal", "mixed_inconclusive"}.
    """
    books = analysis["books"]
    n_sig = sum(1 for b in books if books[b]["significant"])
    pooled_sig = analysis["pooled"]["significant"]
    rev_sig = [b for b in reverse_analysis["books"]
               if reverse_analysis["books"][b]["significant"]]
    detail = {"n_books_significant": n_sig, "pooled_significant": pooled_sig,
              "reverse_significant_books": rev_sig}
    if n_sig >= 3 and pooled_sig and not rev_sig:
        return "lead_lag_edge", detail
    if n_sig == 0 and not pooled_sig:
        return "no_lead_lag_signal", detail
    return "mixed_inconclusive", detail


# ---------------------------------------------------------------------------
# Track B: stale prices
# ---------------------------------------------------------------------------

def stale_cells(game_series, stale_thresh=STALE_THRESH,
                min_consensus=MIN_CONSENSUS_BOOKS):
    """Flag stale quotes and score them against the closing consensus.

    Amendment A1 (exact-line comparability): a stale candidate is a
    (book b, line L) pair at snapshot s. The same-snapshot LOBO consensus
    C_{!=b,L}(s) is the median f_ref over books != b offering exactly line
    L at s (>= min_consensus required). Stale iff |f_b(s) - C| >=
    stale_thresh. The hypothetical bet is the reference selection if
    f_b < C else the other selection.

    Edge = closing LOBO consensus for the bet selection at exactly line L
    (books != b at the game's latest snapshot, >= min_consensus required)
    minus the book's no-vig price for the bet selection at s. Cells AT the
    closing snapshot are excluded: comparing a quote to the consensus of
    its own snapshot is not "beating the close."

    Returns list of cells: {game, market, snapshot, book, line, f_b,
    consensus_s, bet ("ref"/"other"), p_bet_b, f_close, edge}.
    """
    cells = []
    for gkey, g in game_series.items():
        for market, per_snap in g["line_pairs"].items():
            snaps = sorted(per_snap)
            if len(snaps) < 2:
                continue
            close = snaps[-1]
            books_c = per_snap[close]
            for s in snaps[:-1]:
                books_s = per_snap[s]
                for b in sorted(books_s):
                    for line_b, f_b in books_s[b]:
                        others = [f for bb, plist in books_s.items()
                                  if bb != b
                                  for (l, f) in plist if l == line_b]
                        if len(others) < min_consensus:
                            continue
                        cons = statistics.median(others)
                        if abs(f_b - cons) < stale_thresh:
                            continue
                        bet_ref = f_b < cons
                        p_bet_b = f_b if bet_ref else 1.0 - f_b
                        others_c = [f for bb, plist in books_c.items()
                                    if bb != b
                                    for (l, f) in plist if l == line_b]
                        if len(others_c) < min_consensus:
                            continue
                        if bet_ref:
                            f_close = statistics.median(others_c)
                        else:
                            f_close = statistics.median(
                                1.0 - v for v in others_c)
                        cells.append({
                            "game": gkey, "market": market,
                            "snapshot": s.isoformat(), "book": b,
                            "line": line_b,
                            "f_b": f_b, "consensus_s": cons,
                            "bet": "ref" if bet_ref else "other",
                            "p_bet_b": p_bet_b, "f_close": f_close,
                            "edge": f_close - p_bet_b,
                        })
    return cells


def _grand_mean_and_hit(cells):
    """Grand mean of per-game mean edges + cell-level hit rate."""
    by_game = collections.defaultdict(list)
    for c in cells:
        by_game[c["game"]].append(c["edge"])
    game_edges = [statistics.fmean(v) for v in by_game.values()]
    mean = statistics.fmean(game_edges) if game_edges else None
    hit = (sum(1 for c in cells if c["edge"] > 0) / len(cells)
           if cells else None)
    return mean, hit, len(game_edges)


def directional_lag_units(game_series, stale_thresh=STALE_THRESH,
                          move_thresh=LAG_MOVE_THRESH,
                          min_consensus=MIN_CONSENSUS_BOOKS):
    """Split-consensus directional lag units (Amendment A1).

    For each (game, market, snapshot s strictly before the close, book b,
    line L): split the other books at line L (sorted by key,
    deterministic) into A (first 2) and B (next 2); require >= 4 others.
    dev_b = f_b(s,L) - median(A at s); stale flag |dev_b| >= stale_thresh.
    D = median(B at s) - median(B at s-1); require B present at s-1 and
    |D| >= move_thresh. lagging = sign(dev_b) == -sign(D).

    A and B are disjoint, so under the null dev_b and D are independent
    and dev_b is symmetric around 0: P(lagging) = 0.5 exactly. The
    evaluated book never enters either consensus. Each unit also carries
    the stale-side bet's edge vs the closing same-line LOBO consensus
    (excl. b; None if unavailable).
    """
    units = []
    for gkey, g in game_series.items():
        for market, per_snap in g["line_pairs"].items():
            snaps = sorted(per_snap)
            if len(snaps) < 3:
                continue
            close = snaps[-1]
            line_f = {}
            for s in snaps:
                lf = {}
                for b, plist in per_snap[s].items():
                    for (l, f) in plist:
                        lf.setdefault(l, {})[b] = f
                line_f[s] = lf
            for idx in range(1, len(snaps) - 1):
                s, sp = snaps[idx], snaps[idx - 1]
                for L in set(line_f[s]) & set(line_f[sp]):
                    fb_s, fb_p = line_f[s][L], line_f[sp][L]
                    for b in sorted(fb_s):
                        others = sorted(bb for bb in fb_s if bb != b)
                        if len(others) < 4:
                            continue
                        A, B = others[:2], others[2:4]
                        if any(bb not in fb_p for bb in B):
                            continue
                        dev = fb_s[b] - statistics.median(
                            fb_s[bb] for bb in A)
                        if abs(dev) < stale_thresh:
                            continue
                        D = (statistics.median(fb_s[bb] for bb in B)
                             - statistics.median(fb_p[bb] for bb in B))
                        if abs(D) < move_thresh:
                            continue
                        lagging = (dev > 0) != (D > 0)
                        bet_ref = dev < 0
                        f_b = fb_s[b]
                        p_bet_b = f_b if bet_ref else 1.0 - f_b
                        others_c = [f for bb, f in line_f[close].get(L, {}).items()
                                    if bb != b]
                        edge = None
                        if len(others_c) >= min_consensus:
                            if bet_ref:
                                f_close = statistics.median(others_c)
                            else:
                                f_close = statistics.median(
                                    1.0 - v for v in others_c)
                            edge = f_close - p_bet_b
                        units.append({
                            "game": gkey, "market": market,
                            "snapshot": s.isoformat(), "book": b,
                            "line": L, "dev": dev, "D": D,
                            "lagging": lagging,
                            "bet": "ref" if bet_ref else "other",
                            "edge": edge,
                        })
    return units


def analyze_stale(game_series):
    """Track B analysis (Amendment A1).

    Primary: directional lag test (exact binomial vs 0.5). Profitability:
    mean game-level edge over lagging units (descriptive, with the
    line-shopping caveat). Also reports the superseded naive t-test over
    all stale cells for transparency (miscalibrated; NOT used).
    """
    units = directional_lag_units(game_series)
    n = len(units)
    k = sum(1 for u in units if u["lagging"])
    p_lag = binomial_one_sided_ge(k, n) if n >= MIN_LAG_UNITS else None
    by_game = collections.defaultdict(list)
    for u in units:
        if u["lagging"] and u["edge"] is not None:
            by_game[u["game"]].append(u["edge"])
    game_edges = [statistics.fmean(v) for v in by_game.values()]
    mean = statistics.fmean(game_edges) if game_edges else None
    ci95 = None
    if len(game_edges) >= 2:
        sd = statistics.pstdev(game_edges)
        half = 1.96 * sd / math.sqrt(len(game_edges))
        ci95 = [mean - half, mean + half]
    # Superseded transparency: preregistered t-test over all stale cells.
    cells = stale_cells(game_series)
    cell_by_game = collections.defaultdict(list)
    for c in cells:
        cell_by_game[c["game"]].append(c["edge"])
    cell_game_edges = [statistics.fmean(v) for v in cell_by_game.values()]
    t = one_sample_one_sided(cell_game_edges) if cell_game_edges else None
    return {
        "n_units": n,
        "n_lagging": k,
        "lagging_fraction": (k / n) if n else None,
        "p_lag": p_lag,
        "n_games_lagging": len(game_edges),
        "mean_edge_lagging": mean,
        "ci95_lagging": ci95,
        "units": units,
        # Superseded by Amendment A1 (selection bias under the noise null).
        "naive_t_p_superseded": t[4] if t is not None else None,
        "naive_t_mean_superseded": t[0] if t is not None else None,
        "n_cells_all": len(cells),
    }


def decide_track_b(analysis, alpha=ALPHA, practical_edge=PRACTICAL_EDGE):
    """Apply the Amendment A1 Track B decision rule.

    Returns (verdict, detail) with verdict in {"stale_price_edge",
    "no_stale_price_signal", "significant_but_negligible",
    "infeasible"}.
    """
    n = analysis["n_units"]
    if n < MIN_LAG_UNITS:
        return "infeasible", {
            "reason": "fewer than %d valid flagged units" % MIN_LAG_UNITS,
            "n_units": n}
    p, mean = analysis["p_lag"], analysis["mean_edge_lagging"]
    detail = {"p_lag": p, "lagging_fraction": analysis["lagging_fraction"],
              "n_units": n, "n_lagging": analysis["n_lagging"],
              "n_games_lagging": analysis["n_games_lagging"],
              "mean_edge_lagging": mean,
              "ci95_lagging": analysis["ci95_lagging"],
              "naive_t_p_superseded": analysis["naive_t_p_superseded"],
              "naive_t_mean_superseded": analysis["naive_t_mean_superseded"]}
    if p is not None and p < alpha and mean is not None \
            and mean >= practical_edge:
        return "stale_price_edge", detail
    if p is not None and p < alpha:
        return "significant_but_negligible", detail
    return "no_stale_price_signal", detail


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _load_quotes_csv(path):
    import csv
    quotes = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for k in ("line", "american_odds"):
                try:
                    row[k] = float(row[k]) if row[k] not in (None, "") else None
                except (TypeError, ValueError):
                    row[k] = None
            quotes.append(row)
    return quotes


def run_market_alpha(quotes):
    """Run both tracks on quote dicts. Returns (results, game_series)."""
    game_series = build_game_series(quotes)

    # Track A
    events = lead_lag_events(game_series)
    analysis_a = analyze_lead_lag(events)
    # reverse: each follower as leader, pinnacle as the responder
    reverse = {}
    for b in FOLLOWERS:
        r_ev = lead_lag_events(game_series, leader=b, followers=(PINNACLE,))
        r_an = analyze_lead_lag(r_ev, followers=(PINNACLE,))
        reverse[b] = r_an["books"][PINNACLE]
    rev_holm = _holm_bonferroni(
        [(b, reverse[b]["p_raw"]) for b in FOLLOWERS
         if reverse[b]["p_raw"] is not None])
    for b, p_adj in rev_holm.items():
        reverse[b]["p_holm"] = p_adj
        reverse[b]["significant"] = p_adj < ALPHA
    reverse_analysis = {"books": reverse}
    verdict_a, detail_a = decide_track_a(analysis_a, reverse_analysis)

    # Track B
    analysis_b = analyze_stale(game_series)
    verdict_b, detail_b = decide_track_b(analysis_b)

    results = {
        "preregistration": PREREG_PATH,
        "dataset_fingerprint": DATASET_FINGERPRINT,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "SHADOW",
        "n_games": len(game_series),
        "track_a": {
            "n_events": len(events),
            "analysis": _jsonable(analysis_a),
            "reverse": _jsonable(reverse_analysis),
            "verdict": verdict_a, "verdict_detail": detail_a,
        },
        "track_b": {
            "n_lag_units": analysis_b["n_units"],
            "n_cells_all": analysis_b["n_cells_all"],
            "analysis": _jsonable(analysis_b),
            "verdict": verdict_b, "verdict_detail": detail_b,
        },
    }
    return results, game_series


def _jsonable(obj):
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", default=os.environ.get("NFL_EDGE_DATABASE_URL"))
    ap.add_argument("--csv", default=None, help="local quotes CSV (alternative to DB)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

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
    print("loaded %d quotes" % len(quotes), flush=True)
    results, _ = run_market_alpha(quotes)
    results["freeze_check"] = {"n_quotes": n_quotes, "n_snapshots": n_snaps}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, sort_keys=True)
    print("wrote %s (track_a=%s, track_b=%s)"
          % (args.out, results["track_a"]["verdict"],
             results["track_b"]["verdict"]), flush=True)


if __name__ == "__main__":
    main()
