"""Sport registry for Football Edge: shared platform, sport-specific config.

Sport key: 'nfl' | 'ncaaf'. Everything that differs by sport lives here:
The Odds API sport key, table prefixes, season calendars, and snapshot
cadences. Code paths take a sport and look it up; they never hard-code
'americanfootball_nfl' or 'nfl_edge_' outside this module.

Safety rule: the frozen NFL dataset (nfl_edge_* tables) is never written
by the ncaaf path. Each sport gets its own table prefix, enforced here.

Calendars:
  NFL: weeks 1-18, anchored on the Week-1 Sunday. Historical cadence
    Wed 12:00 / Sat 12:00 / Sun 15:30 UTC (Sun 15:30 ~= 90 min before the
    1pm ET slate: the close proxy).
  NCAAF: weeks 0-14 (Week 0 through conference championships; bowls are
    out of scope for the frozen contract), anchored on the Week-1
    Saturday. Historical cadence Wed 12:00 (early-week) / Fri 20:00
    (late-week) / Sat 15:00 UTC (close proxy, ~1h before the noon ET
    window). Tunable after the phase-C probe; cadence is config, not code.
"""
from __future__ import annotations

import datetime as dt

UTC = dt.timezone.utc

SPORTS = {
    "nfl": {
        "label": "NFL",
        "odds_api_sport": "americanfootball_nfl",
        "table_prefix": "nfl_edge",
        # Week-1 anchor Sundays. 2022+ only: American odds in historical
        # snapshots are reliable from 2022-09-18.
        "week1_anchor": {
            2022: dt.date(2022, 9, 11),
            2023: dt.date(2023, 9, 10),
            2024: dt.date(2024, 9, 8),
        },
        "default_weeks": "1-18",
        # (day offset from anchor, hour, minute) in UTC.
        "historical_cadence": [(-4, 12, 0), (-1, 12, 0), (0, 15, 30)],
    },
    "ncaaf": {
        "label": "NCAAF",
        "odds_api_sport": "americanfootball_ncaaf",
        "table_prefix": "ncaaf_edge",
        # Week-1 anchor Saturdays (verified: each date is a Saturday).
        "week1_anchor": {
            2022: dt.date(2022, 9, 3),
            2023: dt.date(2023, 9, 2),
            2024: dt.date(2024, 8, 31),
            2025: dt.date(2025, 8, 30),
        },
        "default_weeks": "0-14",
        "historical_cadence": [(-3, 12, 0), (-1, 20, 0), (0, 15, 0)],
    },
}


def get(sport):
    """Return the config dict for a sport key, or raise ValueError."""
    try:
        return SPORTS[sport]
    except KeyError:
        raise ValueError("unknown sport %r (known: %s)"
                         % (sport, sorted(SPORTS)))


def odds_api_sport(sport):
    return get(sport)["odds_api_sport"]


def market_history_table(sport):
    return "%s_market_history" % get(sport)["table_prefix"]


def historical_quotes_table(sport):
    return "%s_historical_quotes" % get(sport)["table_prefix"]


def checkpoints_table(sport):
    return "%s_checkpoints" % get(sport)["table_prefix"]


def odds_quotes_table(sport):
    return "%s_odds_quotes" % get(sport)["table_prefix"]


def supported_seasons(sport):
    return sorted(get(sport)["week1_anchor"])


def parse_weeks(spec):
    """'1-18' -> [1..18]; '0-14' -> [0..14]; '5' -> [5]."""
    spec = spec.strip()
    if "-" in spec:
        lo, hi = spec.split("-", 1)
        return list(range(int(lo), int(hi) + 1))
    return [int(spec)]


def snapshot_times(sport, season, week):
    """The weekly historical snapshot instants (aware UTC datetimes).

    Anchor = week-1 anchor date + 7*(week-1) days; each cadence entry is a
    (day offset, hour, minute) applied to the anchor date.
    """
    cfg = get(sport)
    try:
        anchor = cfg["week1_anchor"][season]
    except KeyError:
        raise ValueError("unsupported season %d for sport %s (known: %s)"
                         % (season, sport, sorted(cfg["week1_anchor"])))
    out = []
    for off, hour, minute in cfg["historical_cadence"]:
        d = anchor + dt.timedelta(days=7 * (week - 1) + off)
        out.append(dt.datetime(d.year, d.month, d.day, hour, minute,
                               tzinfo=UTC))
    return out
