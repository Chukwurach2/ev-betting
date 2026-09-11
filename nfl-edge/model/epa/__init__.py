"""EPA-v1 game-state feature store.

Reusable football-intelligence layer, NOT a spread model. It turns nflverse
play-by-play into point-in-time team features that many markets can share:
spreads, totals, team totals, 1H/1Q totals, drive models, passing/rushing/
receiving props.

Point-in-time discipline (same contract as model/research/families.py):
every feature for (team, as_of) uses only games strictly before `as_of`.
No future leakage by construction: the store pre-aggregates per-team-game
rows in chronological order, and queries slice history with date < as_of.

Data: nflverse play_by_play_YYYY (CC BY 4.0, github.com/nflverse/nflverse-data).
Cached under .cache/ (never committed). CSV-gzip is used because this
environment has no parquet engine; parquet would be byte-identical upstream.

Limitations (v1):
  - Weekly rosters are NOT ingested; QB identity comes from pbp passer ids.
  - No injury/inactive feed: the QB proxy cannot see surprise inactives.
  - Opponent adjustment is a documented one-step strength-of-schedule.
  - Pace is a clock-difference proxy, not tracking data.
"""
