"""Leave-one-book-out (LOBO) fair pricing and stale-price signals.

For each identical event/market/selection/line, the target book is judged
against a no-vig consensus built EXCLUSIVELY from the other books' quotes.
The target book's own quote must never contribute to the fair price used to
evaluate it -- otherwise a stale or shaded quote would validate itself.

This engine is deterministic and model-free: it is the benchmark that ML
challengers (e.g. market-only-v1) must beat, and the source of the
"estimated edge after removing vig" signal.

Pure functions only: deterministic, no I/O, no network, no DB.
"""
from __future__ import annotations

import os
import statistics
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from model.research.market_features import (_match, _to_dt, implied_prob,
                                            novig_prob)

_FRESH_MINUTES = 30.0


def _per_book_two_sided(sel_exact_timed, pair_timed, selection):
    """Map book_key -> no-vig prob for selection from its latest quotes.

    The requested selection must be quoted at EXACTLY the reference line;
    the opposite selection may be at the line or its negation (spread
    sides are negations; total sides share the line). A book contributes
    only if it has both sides at as_of (no-vig is undefined one-sided).
    Returns (per_book, latest_any) where latest_any maps book_key ->
    (observed_at, quote) of the book's latest quote for the requested
    selection at the exact line.
    """
    by_book: dict[str, list] = {}
    for t, q in pair_timed:
        key = q.get("book_key")
        if key is None:
            continue
        by_book.setdefault(key, []).append((t, q))
    exact_latest = {}
    for t, q in sel_exact_timed:
        key = q.get("book_key")
        if key is None:
            continue
        if key not in exact_latest or t >= exact_latest[key][0]:
            exact_latest[key] = (t, q)
    per_book = {}
    for key, timed in by_book.items():
        if key not in exact_latest:
            continue
        other_latest = None
        for t, q in timed:
            if q.get("selection") == selection:
                continue
            if other_latest is None or t >= other_latest[0]:
                other_latest = (t, q)
        if other_latest is None:
            continue
        nv = novig_prob(
            implied_prob(exact_latest[key][1].get("american_odds")),
            implied_prob(other_latest[1].get("american_odds")))
        if nv is not None:
            per_book[key] = nv
    return per_book, exact_latest


def lobo_signal(quotes, as_of, provider_event_id, market, selection, line,
                target_book) -> dict:
    """Price the target book's quote against everyone else.

    Args:
        quotes: iterable of quote dicts (nfl_edge_odds_quotes shape). Should
            include BOTH selections so per-book no-vig can be computed.
        as_of: point-in-time cutoff; later quotes are ignored.
        provider_event_id, market, selection, line: identical market.
        target_book: book_key being evaluated (excluded from consensus).

    Returns:
        Dict with keys: consensus_prob (median no-vig across other books),
        offered_prob (target's raw implied), fair_prob (= consensus_prob),
        edge_pp ((fair - offered) * 100; positive means the offered price is
        generous relative to fair), n_books_used, dispersion (stdev of the
        other books' no-vig probs), quote_age_seconds (target quote age at
        as_of), signal_at (as_of ISO), books (contributing book keys),
        fresh (all contributing quotes <= 30 min old). When fewer than two
        other books can form a no-vig price, returns {"insufficient_data":
        True, "edge_pp": None, ...} with the remaining keys present but None.
    """
    as_of = _to_dt(as_of)
    base = {
        "consensus_prob": None,
        "offered_prob": None,
        "fair_prob": None,
        "edge_pp": None,
        "n_books_used": 0,
        "dispersion": None,
        "quote_age_seconds": None,
        "signal_at": as_of.isoformat() if as_of else None,
        "books": [],
        "fresh": False,
        "insufficient_data": False,
        "target_book": target_book,
    }
    if as_of is None:
        base["insufficient_data"] = True
        return base

    both_timed = _match(quotes, as_of, provider_event_id, market, line,
                        selection=None, line_mode="pair")
    sel_exact_timed = _match(quotes, as_of, provider_event_id, market, line,
                             selection=selection, line_mode="exact")
    per_book, latest_any = _per_book_two_sided(sel_exact_timed, both_timed,
                                               selection)

    # The target book's own latest quote for the offered price.
    tgt = latest_any.get(target_book)
    if tgt is not None:
        t_tgt, q_tgt = tgt
        base["offered_prob"] = implied_prob(q_tgt.get("american_odds"))
        base["quote_age_seconds"] = (as_of - t_tgt).total_seconds()

    # LOBO: the target book never contributes to its own consensus.
    others = {k: v for k, v in per_book.items() if k != target_book}
    if len(others) < 2:
        base["insufficient_data"] = True
        return base

    probs = list(others.values())
    consensus = statistics.median(probs)
    base["consensus_prob"] = consensus
    base["fair_prob"] = consensus
    base["n_books_used"] = len(others)
    base["dispersion"] = statistics.stdev(probs) if len(probs) >= 2 else None
    base["books"] = sorted(others)
    if base["offered_prob"] is not None:
        base["edge_pp"] = (consensus - base["offered_prob"]) * 100.0
    # Freshness over the quotes that formed the consensus.
    ages = []
    for key in others:
        t_q, _ = latest_any[key]
        ages.append((as_of - t_q).total_seconds())
    base["fresh"] = all(a <= _FRESH_MINUTES * 60 for a in ages)
    return base


def best_price_across_books(quotes, as_of, provider_event_id, market,
                            selection, line) -> dict | None:
    """Cheapest (lowest implied prob) latest quote across books.

    Returns {"book_key", "american_odds", "implied_prob", "observed_at"}
    or None when no quotes qualify at as_of.
    """
    as_of = _to_dt(as_of)
    if as_of is None:
        return None
    sel_timed = _match(quotes, as_of, provider_event_id, market, line,
                       selection=selection)
    best = None
    for t, q in sel_timed:
        p = implied_prob(q.get("american_odds"))
        if p is None:
            continue
        if best is None or p < best[0]:
            best = (p, t, q)
    if best is None:
        return None
    p, t, q = best
    return {
        "book_key": q.get("book_key"),
        "american_odds": q.get("american_odds"),
        "implied_prob": p,
        "observed_at": t.isoformat(),
    }
