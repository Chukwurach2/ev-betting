"""Correlation-aware opportunity selection.

Ranks +EV candidates by expected value x confidence x market quality, sizes
with fractional Kelly, then greedily fills under correlation caps (per
event, per team, per day). Deterministic: identical inputs always produce
identical outputs.

Documented simplifications:
  * Same-game side+total correlation is approximated by the per-event unit
    and count caps, not by a full covariance model. Two bets on one game
    share the event cap whether or not they are directionally correlated.
  * Team caps use the candidate's "teams" list when present; candidates
    without it are capped by event only.
  * Sizing uses fair_prob vs the offered price (edge = fair*decimal - 1);
    the precomputed edge_pp is used for filtering and ranking only.

Alpha attribution (kept separate forever):
  football   edge from game modeling the market hasn't priced (residuals)
  market     edge from a stale/off-market quote (LOBO signal)
  execution  edge from taking the best available line/price across books
Candidates may carry "attribution" = {"football_pp","market_pp",
"execution_pp"}; components must sum to edge_pp (validated, fail loud).
Selected outputs always include the attribution (zeros when unknown).

Pure functions. No I/O, no network.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

try:
    from model.markets.registry import get_market as _get_market
except Exception:  # pragma: no cover - registry always present in practice
    _get_market = None

DEFAULT_CONFIG = {
    "min_edge_pp": 2.0,
    "max_quote_age_s": 900,
    "kelly_fraction": 0.25,
    "max_units_per_bet": 1.0,
    "max_units_per_event": 1.5,
    "max_bets_per_event": 4,
    "max_units_per_day": 8.0,
    "max_units_per_team": 2.0,
    "min_confidence": 0.5,
    "min_units": 0.05,
}

_MARKET_QUALITY = {"core": 1.0, "derivative": 0.7, "prop": 0.5}


_ATTRIBUTION_KEYS = ("football_pp", "market_pp", "execution_pp")


def _attribution(candidate):
    """Validate and normalize the alpha attribution of a candidate.

    Components must sum to edge_pp within 0.01pp; mismatch raises
    ValueError (research integrity: never silently misattribute edge).
    Missing attribution -> all zeros.
    """
    raw = candidate.get("attribution") or {}
    vals = {k: float(raw.get(k, 0.0)) for k in _ATTRIBUTION_KEYS}
    if raw:
        total = sum(vals.values())
        edge = float(candidate.get("edge_pp") or 0.0)
        if abs(total - edge) > 0.01:
            raise ValueError(
                "attribution sums to %.2fpp but edge_pp is %.2fpp "
                "(football=%.2f market=%.2f execution=%.2f)" % (
                    total, edge, vals["football_pp"], vals["market_pp"],
                    vals["execution_pp"]))
    return vals


def american_to_decimal(odds):
    """American odds (int) -> decimal odds."""
    odds = float(odds)
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def market_quality(market_key):
    """Rank weight by market risk class: core 1.0, derivative 0.7, prop 0.5.

    Unknown markets default to 0.5 (treated as prop-grade evidence).
    """
    risk_class = None
    if _get_market is not None:
        try:
            risk_class = _get_market(market_key)["risk_class"]
        except ValueError:
            risk_class = None
    return _MARKET_QUALITY.get(risk_class, 0.5)


def kelly_units(fair_prob, american_odds, bankroll_u, kelly_fraction,
                max_units):
    """Fractional-Kelly stake in units, capped. Returns 0 when no edge."""
    decimal = american_to_decimal(american_odds)
    if decimal <= 1.0:
        return 0.0
    edge = fair_prob * decimal - 1.0
    if edge <= 0:
        return 0.0
    f = edge / (decimal - 1.0)
    return min(f * kelly_fraction * bankroll_u, max_units)


def select_opportunities(candidates, bankroll_u=100.0, config=None):
    """Filter, rank, size, and correlation-cap +EV candidates.

    candidates: list of dicts with keys event_id, market, selection, line,
    book, american_odds, fair_prob, edge_pp, confidence (0..1),
    quote_age_s, model, data_quality ("ok"|"thin"), and optional
    teams ([home_abbr, away_abbr]).

    Returns the selected candidates, each augmented with "units" and "rank"
    (1-based), in rank order.
    """
    cfg = dict(DEFAULT_CONFIG)
    if config:
        cfg.update(config)

    # (a) quality filters (+ attribution validation, fail loud)
    eligible = []
    for c in candidates:
        _attribution(c)  # raises on misattributed edge
        if c.get("data_quality") != "ok":
            continue
        if (c.get("edge_pp") or 0) < cfg["min_edge_pp"]:
            continue
        if (c.get("quote_age_s") if c.get("quote_age_s") is not None
                else float("inf")) > cfg["max_quote_age_s"]:
            continue
        if (c.get("confidence") or 0) < cfg["min_confidence"]:
            continue
        eligible.append(c)

    # (b) rank: edge x confidence x market quality, deterministic ties
    def rank_key(c):
        score = (c["edge_pp"] * c["confidence"]
                 * market_quality(c.get("market")))
        return (-score, str(c.get("event_id")), str(c.get("market")),
                str(c.get("selection")), str(c.get("book")),
                str(c.get("line")))

    eligible.sort(key=rank_key)

    # (c)+(d) fractional-Kelly sizing with greedy correlation caps
    selected = []
    event_units, event_count = {}, {}
    team_units = {}
    day_units = 0.0
    for c in eligible:
        units = kelly_units(c["fair_prob"], c["american_odds"], bankroll_u,
                            cfg["kelly_fraction"], cfg["max_units_per_bet"])
        if units < cfg["min_units"]:
            continue
        ev = c["event_id"]
        if event_count.get(ev, 0) >= cfg["max_bets_per_event"]:
            continue
        if event_units.get(ev, 0.0) + units > cfg["max_units_per_event"] \
                + 1e-9:
            continue
        teams = c.get("teams") or []
        if any(team_units.get(t, 0.0) + units > cfg["max_units_per_team"]
               + 1e-9 for t in teams):
            continue
        if day_units + units > cfg["max_units_per_day"] + 1e-9:
            continue
        event_units[ev] = event_units.get(ev, 0.0) + units
        event_count[ev] = event_count.get(ev, 0) + 1
        for t in teams:
            team_units[t] = team_units.get(t, 0.0) + units
        day_units += units
        out = dict(c)
        out["units"] = round(units, 4)
        out["rank"] = len(selected) + 1
        out["attribution"] = _attribution(c)
        selected.append(out)
    return selected
