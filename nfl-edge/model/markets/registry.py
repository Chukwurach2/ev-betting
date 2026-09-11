"""Extensible market registry.

A "market" is a bettable market type (spread, total, moneyline, ...), not a
single game line. The engine asks the registry which markets are technically
supported and what validation status each holds. Markets move through the
status chain only via explicit, validated transitions:

    unsupported -> research -> candidate -> challenger -> production

Forward moves may never skip a step (research -> production is rejected).
Backward moves (demotion on deteriorating evidence, e.g.
production -> challenger) are always allowed. Statuses match the vocabulary
in model/research/MARKET_MATRIX.md.

Pure functions over module state. No I/O, no network.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

STATUSES = ("unsupported", "research", "candidate", "challenger", "production")


def _market(key, label, line_applies, selection_ids, settlement,
            push_handling, min_sample_bets, risk_class, status):
    return {
        "key": key,
        "label": label,
        "line_applies": line_applies,
        "selection_ids": list(selection_ids),
        "settlement": settlement,
        "push_handling": push_handling,
        "min_sample_bets": min_sample_bets,
        "risk_class": risk_class,
        "status": status,
    }


MARKETS = {
    "moneyline": _market(
        "moneyline", "Moneyline", False, ["home", "away"],
        "moneyline", "n/a", 500, "core", "unsupported"),
    "full_game_spread": _market(
        "full_game_spread", "Full-game spread", True, ["home", "away"],
        "score_margin_vs_line", "push_excluded", 500, "core", "research"),
    "full_game_total": _market(
        "full_game_total", "Full-game total", True, ["over", "under"],
        "total_vs_line", "push_excluded", 500, "core", "research"),
    "team_total_home": _market(
        "team_total_home", "Home team total", True, ["over", "under"],
        "total_vs_line", "push_excluded", 300, "derivative", "unsupported"),
    "team_total_away": _market(
        "team_total_away", "Away team total", True, ["over", "under"],
        "total_vs_line", "push_excluded", 300, "derivative", "unsupported"),
    "first_half_spread": _market(
        "first_half_spread", "First-half spread", True, ["home", "away"],
        "score_margin_vs_line", "push_excluded", 300, "derivative",
        "unsupported"),
    "first_half_total": _market(
        "first_half_total", "First-half total", True, ["over", "under"],
        "total_vs_line", "push_excluded", 300, "derivative", "unsupported"),
    "first_quarter_spread": _market(
        "first_quarter_spread", "First-quarter spread", True, ["home", "away"],
        "score_margin_vs_line", "push_excluded", 300, "derivative",
        "unsupported"),
    "first_quarter_total": _market(
        "first_quarter_total", "First-quarter total", True, ["over", "under"],
        "total_vs_line", "push_excluded", 300, "derivative", "research"),
    "first_drive_result": _market(
        "first_drive_result", "First-drive result", False,
        ["score", "no_score"], "tbd", "n/a", 500, "derivative", "unsupported"),
}

# Status notes: full_game_spread / full_game_total are "research" because
# elo-v1 was evaluated there (and rejected as a family); the markets remain
# open for future families. first_quarter_total is "research" because
# Q1_TOTAL quotes already exist in the quote store. Everything else is
# "unsupported" until evidence or data justifies research.


def get_market(key):
    """Return the market dict for key. Unknown keys raise ValueError."""
    try:
        return MARKETS[key]
    except KeyError:
        raise ValueError("unknown market %r (have %s)"
                         % (key, sorted(MARKETS)))


def set_status(key, new):
    """Move a market along the status chain.

    Forward moves must advance exactly one step; backward moves (demotion)
    may jump to any earlier status. Raises ValueError on unknown markets,
    unknown statuses, or skipped forward steps. Returns the updated market.
    """
    market = get_market(key)
    if new not in STATUSES:
        raise ValueError("unknown status %r (have %s)" % (new, STATUSES))
    cur_idx = STATUSES.index(market["status"])
    new_idx = STATUSES.index(new)
    if new_idx > cur_idx + 1:
        raise ValueError(
            "cannot skip forward from %r to %r: advance one step at a time"
            % (market["status"], new))
    market["status"] = new
    return market


def markets_by_status(status):
    """Sorted list of market keys currently holding status."""
    if status not in STATUSES:
        raise ValueError("unknown status %r (have %s)" % (status, STATUSES))
    return sorted(k for k, m in MARKETS.items() if m["status"] == status)


def to_dict():
    """Full registry as {key: {field: value}} with copies (no aliases)."""
    return {k: dict(m, selection_ids=list(m["selection_ids"]))
            for k, m in MARKETS.items()}
