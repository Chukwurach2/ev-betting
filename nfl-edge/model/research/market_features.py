"""Point-in-time-safe market microstructure features over quote dicts.

Quotes are plain dicts shaped like the nfl_edge_odds_quotes table
(provider_event_id, home_team, away_team, kickoff, sportsbook, book_key,
market, selection, line, american_odds, fair_probability, observed_at,
collected_at). observed_at / kickoff may be datetimes or ISO strings.

Point-in-time safety: EVERY feature is computed from quotes with
observed_at <= as_of only. A quote observed after as_of can never change
an earlier feature vector. Feature snapshots built this way are
reproducible and leakage-free, which is what lets the forward
market-only-v1 family train on them later.

Pure functions only: deterministic, no I/O, no network, no DB.
"""
from __future__ import annotations

import os
import statistics
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

FEATURE_VERSION = "1"

_STALE_MINUTES = 15.0
_FRESH_MINUTES = 30.0


def feature_names() -> list[str]:
    """Ordered feature keys returned by build_features (excl. version)."""
    return [
        "opener_line",
        "opener_price",
        "current_line",
        "current_price",
        "line_move",
        "price_move",
        "move_velocity_per_hour",
        "move_direction",
        "minutes_since_opener",
        "minutes_to_kickoff",
        "n_books",
        "line_dispersion",
        "prob_dispersion",
        "novig_consensus_prob",
        "best_price",
        "best_price_vs_consensus_pp",
        "median_price",
        "dk_fd_line_gap",
        "dk_fd_prob_gap",
        "hold_pct",
        "quote_age_seconds",
        "stale_flag",
        "line_moved_price_static",
        "price_moved_line_static",
        "is_leading",
        "is_lagging",
    ]


def _to_dt(value):
    """Coerce datetime/ISO-string to an aware UTC datetime, else None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def implied_prob(american_odds) -> float | None:
    """American odds -> raw implied probability (with vig)."""
    o = _num(american_odds)
    if o is None or o == 0:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    return -o / (-o + 100.0)


def novig_prob(p_a, p_b) -> float | None:
    """Remove vig from a two-outcome pair: p_a / (p_a + p_b)."""
    if p_a is None or p_b is None or (p_a + p_b) <= 0:
        return None
    return p_a / (p_a + p_b)


def _match(quotes, as_of, provider_event_id, market, line,
           selection=None, line_mode="exact"):
    """Quotes for the event/market observed at/before as_of.

    selection=None matches both sides (needed for no-vig math).
    line_mode:
      "exact" -- quote line must equal the reference line.
      "pair"  -- quote line may equal the reference line or its negation
                 (spread: HOME -3.0 pairs with AWAY +3.0; totals: both
                 sides share the line, and -line == line only at 0).
      "any"   -- no line filter (for measuring line movement itself).
    """
    ref = _num(line)
    out = []
    for q in quotes or []:
        if q.get("provider_event_id") != provider_event_id:
            continue
        if q.get("market") != market:
            continue
        lv = _num(q.get("line"))
        if line_mode == "exact" and lv != ref:
            continue
        if line_mode == "pair" and lv != ref and lv != -ref:
            continue
        if selection is not None and q.get("selection") != selection:
            continue
        t = _to_dt(q.get("observed_at"))
        if t is None or t > as_of:
            continue
        if lv is None or implied_prob(q.get("american_odds")) is None:
            continue
        out.append((t, q))
    out.sort(key=lambda tq: (tq[0], str(tq[1].get("quote_id", ""))))
    return out


def _latest_per_book(timed):
    """Map book_key -> (observed_at, quote) of that book's latest quote."""
    latest = {}
    for t, q in timed:
        key = q.get("book_key")
        if key is None:
            continue
        if key not in latest or t >= latest[key][0]:
            latest[key] = (t, q)
    return latest


def _latest_at(timed, book_key, t_max):
    """Book's latest quote at/before t_max, or None."""
    best = None
    for t, q in timed:
        if q.get("book_key") != book_key or t > t_max:
            continue
        if best is None or t >= best[0]:
            best = (t, q)
    return best


def _median_book_novig(sel_timed, opp_timed, selection):
    """Median across books of each book's no-vig prob for selection.

    Pairing is per book against ITS OWN latest line: the book's latest
    requested-selection quote is at line L; the opposite side must be at
    L or -L (spread sides are negations; total sides share the line).
    This keeps no-vig valid after the market has moved off the reference
    line. Returns (median_prob, {book_key: novig_prob}).
    """
    latest_sel = {}
    for t, q in sel_timed:
        key = q.get("book_key")
        if key is None:
            continue
        if key not in latest_sel or t >= latest_sel[key][0]:
            latest_sel[key] = (t, q)
    opp_by_book: dict[str, list] = {}
    for t, q in opp_timed:
        if q.get("selection") == selection:
            continue
        key = q.get("book_key")
        if key is None:
            continue
        opp_by_book.setdefault(key, []).append((t, q))
    per_book = {}
    for key, (t_s, q_s) in latest_sel.items():
        line_l = _num(q_s.get("line"))
        cands = [(t, q) for t, q in opp_by_book.get(key, [])
                 if _num(q.get("line")) in (line_l, -line_l)]
        if not cands:
            continue
        t_o, q_o = max(cands, key=lambda tq: tq[0])
        p_sel = implied_prob(q_s.get("american_odds"))
        p_other = implied_prob(q_o.get("american_odds"))
        nv = novig_prob(p_sel, p_other)
        if nv is not None:
            per_book[key] = nv
    if not per_book:
        return None, {}
    return statistics.median(per_book.values()), per_book


def build_features(quotes, as_of, provider_event_id, market, selection,
                   line, target_book=None) -> dict:
    """Build the market microstructure feature vector at a point in time.

    Args:
        quotes: iterable of quote dicts (nfl_edge_odds_quotes shape).
        as_of: point-in-time cutoff (datetime or ISO string). Quotes with
            observed_at > as_of are ignored.
        provider_event_id, market, selection, line: the identical
            market/selection/line being priced.
        target_book: optional book_key. When given, target-specific
            features (quote age, staleness, lead/lag, static-move flags)
            describe that book; market-wide features are unchanged.

    Returns:
        Dict with FEATURE_VERSION under "feature_version" plus one key per
        name in feature_names(). Missing data -> None (never fabricated).
    """
    as_of = _to_dt(as_of)
    feats = {name: None for name in feature_names()}
    feats["feature_version"] = FEATURE_VERSION
    if as_of is None:
        return feats

    sel_timed = _match(quotes, as_of, provider_event_id, market, line,
                       selection=selection, line_mode="any")
    if not sel_timed:
        return feats
    both_timed = _match(quotes, as_of, provider_event_id, market, line,
                        selection=None, line_mode="any")

    # --- opener / current / movement (market-wide, earliest/latest) ---
    t_open, q_open = sel_timed[0]
    t_cur, q_cur = sel_timed[-1]
    opener_line = _num(q_open.get("line"))
    opener_price = q_open.get("american_odds")
    current_line = _num(q_cur.get("line"))
    current_price = q_cur.get("american_odds")
    feats["opener_line"] = opener_line
    feats["opener_price"] = opener_price
    feats["current_line"] = current_line
    feats["current_price"] = current_price
    line_move = (current_line - opener_line
                 if opener_line is not None and current_line is not None
                 else None)
    price_move = None
    if opener_price is not None and current_price is not None:
        try:
            price_move = int(current_price) - int(opener_price)
        except (TypeError, ValueError):
            price_move = None
    feats["line_move"] = line_move
    feats["price_move"] = price_move
    hours = (t_cur - t_open).total_seconds() / 3600.0
    feats["move_velocity_per_hour"] = (line_move / hours
                                       if line_move is not None and hours > 1e-9
                                       else None)
    feats["move_direction"] = (1 if (line_move or 0) > 0 else
                               -1 if (line_move or 0) < 0 else 0)
    feats["minutes_since_opener"] = (as_of - t_open).total_seconds() / 60.0
    kickoff = _to_dt(q_cur.get("kickoff"))
    feats["minutes_to_kickoff"] = (
        (kickoff - as_of).total_seconds() / 60.0 if kickoff else None)

    # --- per-book latest snapshot ---
    latest = _latest_per_book(sel_timed)
    feats["n_books"] = len(latest)
    book_lines = [_num(q.get("line")) for _, q in latest.values()]
    book_lines = [v for v in book_lines if v is not None]
    book_probs = [implied_prob(q.get("american_odds"))
                  for _, q in latest.values()]
    book_probs = [v for v in book_probs if v is not None]
    feats["line_dispersion"] = (statistics.stdev(book_lines)
                                if len(book_lines) >= 2 else None)
    feats["prob_dispersion"] = (statistics.stdev(book_probs)
                                if len(book_probs) >= 2 else None)

    # --- no-vig consensus: median book's no-vig prob for the selection ---
    median_nv, per_book_nv = _median_book_novig(sel_timed, both_timed,
                                                   selection)
    feats["novig_consensus_prob"] = median_nv

    # --- best price vs consensus ---
    if latest:
        best_key = min(latest,
                       key=lambda k: implied_prob(
                           latest[k][1].get("american_odds")))
        best_q = latest[best_key][1]
        feats["best_price"] = best_q.get("american_odds")
        best_p = implied_prob(best_q.get("american_odds"))
        feats["best_price_vs_consensus_pp"] = (
            (median_nv - best_p) * 100.0
            if median_nv is not None and best_p is not None else None)
        prices = []
        for _, q in latest.values():
            try:
                prices.append(int(q.get("american_odds")))
            except (TypeError, ValueError):
                continue
        feats["median_price"] = (statistics.median(prices)
                                 if prices else None)

    # --- DraftKings vs FanDuel disagreement ---
    dk = latest.get("draftkings")
    fd = latest.get("fanduel")
    if dk and fd:
        dk_line, fd_line = _num(dk[1].get("line")), _num(fd[1].get("line"))
        feats["dk_fd_line_gap"] = (dk_line - fd_line
                                   if dk_line is not None
                                   and fd_line is not None else None)
        dk_p = implied_prob(dk[1].get("american_odds"))
        fd_p = implied_prob(fd[1].get("american_odds"))
        feats["dk_fd_prob_gap"] = (dk_p - fd_p
                                   if dk_p is not None and fd_p is not None
                                   else None)

    # --- hold: target book's two-sided hold, else median book's ---
    hold_book = target_book if target_book in per_book_nv else None
    if hold_book is None and per_book_nv:
        med = statistics.median(per_book_nv.values())
        hold_book = min(per_book_nv, key=lambda k: abs(per_book_nv[k] - med))
    if hold_book:
        per_sel = {}
        for t, q in both_timed:
            if q.get("book_key") != hold_book:
                continue
            sel = q.get("selection")
            if sel not in per_sel or t >= per_sel[sel][0]:
                per_sel[sel] = (t, q)
        if len(per_sel) >= 2:
            ps = [implied_prob(q.get("american_odds"))
                  for _, q in per_sel.values()]
            if all(p is not None for p in ps):
                feats["hold_pct"] = (sum(ps) - 1.0) * 100.0

    # --- target-book specific features ---
    series = [(t, q) for t, q in sel_timed
              if q.get("book_key") == target_book] if target_book else None
    if target_book and series:
        t_tgt, q_tgt = series[-1]
        feats["quote_age_seconds"] = (as_of - t_tgt).total_seconds()
        # Stale: target quote older than 15 min while peers moved since.
        cutoff = as_of - timedelta(minutes=_STALE_MINUTES)
        target_stale = (as_of - t_tgt).total_seconds() > _STALE_MINUTES * 60
        moved = False
        for key in latest:
            if key == target_book:
                continue
            now_line = _num(latest[key][1].get("line"))
            prev = _latest_at(sel_timed, key, cutoff)
            prev_line = _num(prev[1].get("line")) if prev else None
            if now_line is not None and prev_line is not None \
                    and now_line != prev_line:
                moved = True
                break
        feats["stale_flag"] = bool(target_stale and moved)
        # Static-move flags on the target book's own series.
        t0, q0 = series[0]
        tl, pl = _num(q0.get("line")), _num(q_tgt.get("line"))
        p0, p1 = q0.get("american_odds"), q_tgt.get("american_odds")
        try:
            p0i, p1i = int(p0), int(p1)
        except (TypeError, ValueError):
            p0i = p1i = None
        feats["line_moved_price_static"] = (
            tl is not None and pl is not None and abs(pl - tl) >= 0.5
            and p0i is not None and p0i == p1i)
        feats["price_moved_line_static"] = (
            tl is not None and pl is not None and pl == tl
            and p0i is not None and p1i is not None and p0i != p1i)
        # Lead/lag: whose line moved most recently before as_of.
        def last_change(bk):
            book_series = (series if bk == target_book else
                           [(tt, qq) for tt, qq in sel_timed
                            if qq.get("book_key") == bk])
            prev_line = None
            change_t = None
            for t, q in book_series:
                lv = _num(q.get("line"))
                if prev_line is not None and lv is not None \
                        and prev_line is not None and lv != prev_line:
                    change_t = t
                if lv is not None:
                    prev_line = lv
            return change_t
        tgt_change = last_change(target_book)
        con_change = None
        for key in latest:
            if key == target_book:
                continue
            c = last_change(key)
            if c is not None and (con_change is None or c > con_change):
                con_change = c
        if tgt_change is not None and con_change is not None:
            feats["is_leading"] = tgt_change < con_change
            feats["is_lagging"] = tgt_change > con_change
        elif tgt_change is None and con_change is None:
            feats["is_leading"] = False
            feats["is_lagging"] = False
    elif target_book:
        # Target book requested but has no quotes: leave target features None.
        pass
    else:
        # No target book: static-move flags describe the overall series.
        tl, pl = opener_line, current_line
        try:
            p0i, p1i = int(opener_price), int(current_price)
        except (TypeError, ValueError):
            p0i = p1i = None
        feats["line_moved_price_static"] = (
            tl is not None and pl is not None and abs(pl - tl) >= 0.5
            and p0i is not None and p0i == p1i)
        feats["price_moved_line_static"] = (
            tl is not None and pl is not None and pl == tl
            and p0i is not None and p1i is not None and p0i != p1i)

    return feats
