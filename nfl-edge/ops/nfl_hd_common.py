#!/usr/bin/env python3
"""Shared frozen logic for H-D (precipitation -> attempt props).

- norm_name: provider/nflverse player-name normalization.
- designate: frozen QB1/RB1 designation rule (lagged snap shares, weeks < t;
  tie-breaks: t-1 snaps -> t-2 -> t-3 -> player_id alphabetical).
  NOTE: the frozen attempts tie-break (most pass/rush attempts in the most
  recent week) is verified separately in nfl_hd_test.py, which reports
  whether it binds anywhere. The integrity gate must not read outcome
  columns, so it uses the snap+player_id chain only.
- consensus_line: median line across qualifying books (>=2).
- american_to_decimal / de_vig: same-book fair probabilities (informational).
"""
import re
import statistics

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def norm_name(s):
    if s is None:
        return ""
    s = s.lower().replace(".", " ").replace("'", "").replace("-", " ")
    toks = [t for t in s.split() if t and t not in SUFFIXES]
    return " ".join(toks)


def lagged_weeks(team, season, week, snaps):
    """Three most recent REG weeks with snap data for team before (season, week)."""
    df = snaps[(snaps["team"] == team)]
    cands = sorted(
        {(int(r.season), int(r.week)) for r in df.itertuples()
         if (int(r.season), int(r.week)) < (season, week)},
        reverse=True,
    )
    return cands[:3]


def designate(team, season, week, position, snaps):
    """Return (player_display_name, detail) for the frozen designation rule.

    position: 'QB' or 'RB'. Highest lagged offensive snap share at the
    position on the team; most recent week first; ties -> earlier lagged
    weeks -> player name alphabetical (attempts tie-break verified in test).
    """
    weeks = lagged_weeks(team, season, week, snaps)
    if not weeks:
        return None, {"reason": "no_lagged_weeks"}
    pos = snaps[(snaps["team"] == team) & (snaps["position"] == position)]
    players = sorted(pos["player"].dropna().unique())
    if not players:
        return None, {"reason": f"no_{position}_in_snaps"}
    snap = {(p, w): 0 for p in players for w in weeks}
    for r in pos.itertuples():
        key = (r.player, (int(r.season), int(r.week)))
        if key in snap:
            snap[key] = int(r.offense_snaps or 0)

    def sort_key(p):
        return tuple(-snap[(p, w)] for w in weeks) + (norm_name(p),)

    best = min(players, key=sort_key)
    return best, {"weeks": [f"{s}_{w}" for s, w in weeks],
                  "snaps": [snap[(best, w)] for w in weeks]}


def consensus_line(quotes):
    """quotes: list of dicts with book/line. Returns (L, n_books) or (None, 0)."""
    lines = [q["line"] for q in quotes
             if q.get("line") is not None]
    if len(lines) < 2:
        return None, len(lines)
    return statistics.median(lines), len(lines)


def books_at_line(quotes, L, tol=0.01):
    return [q for q in quotes
            if q.get("line") is not None and abs(q["line"] - L) <= tol]


def american_to_decimal(o):
    o = float(o)
    return 1 + o / 100.0 if o > 0 else 1 + 100.0 / abs(o)


def de_vig(over_price, under_price):
    """Same-book fair probabilities from a two-way American-price pair."""
    io = 1.0 / american_to_decimal(over_price)
    iu = 1.0 / american_to_decimal(under_price)
    tot = io + iu
    return io / tot, iu / tot


def american_profit(o):
    """Profit per 1u stake at American odds o."""
    o = float(o)
    return o / 100.0 if o > 0 else 100.0 / abs(o)
