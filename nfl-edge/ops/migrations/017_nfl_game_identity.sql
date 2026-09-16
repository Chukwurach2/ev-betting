-- 017_nfl_game_identity.sql
-- Canonical NFL game identity: nflverse game_id <-> Odds API event id.
--
-- Purpose: deterministic cross-source identity layer (read-only matching
-- logic in ops/nfl_canonical_identity.py; explicit team aliases in
-- ops/nfl_team_aliases.json; bundled nflverse schedules in
-- ops/nflverse_schedules_2022_2024.json).
--
-- This table is SHARED INFRASTRUCTURE. It becomes the join key for:
-- injuries, rosters, officiating, play-by-play-derived features, and
-- eventually weather. It never touches the frozen NFL dataset
-- (nfl_edge_* quote/history tables), the frozen NFL v1.3 model, or the
-- forward shadow experiment.
--
-- Identity states (match_type):
--   alias_date        Odds full name resolved via explicit alias table,
--                     matched on exact UTC commence date
--   alias_date_shift  alias-resolved, matched on UTC date +/- 1 day
--                     (US-evening games landing on the next UTC day)
--   swapped           home/away flipped relative to nflverse
--   swapped_date_shift both
-- There is no fuzzy match type. Unmatched and ambiguous states are
-- preserved in the CI artifact (nfl_canonical_identity.json), never
-- silently resolved.
--
-- Bundle completeness: the bundled nflverse schedules hold 815 of the 816
-- scheduled 2022-2024 regular-season games. The missing game is the
-- 2022 Week 17 Bills @ Bengals game scheduled 2023-01-03, suspended in Q1
-- and never completed or resumed; nflverse excludes it by construction.
-- It is carried explicitly in the identity artifact under
-- results['missing_from_source'] with state 'missing_from_source'
-- (see ops/nfl_canonical_identity.py KNOWN_MISSING_FROM_SOURCE).
-- It has no nflverse_game_id and therefore cannot be a row in this table;
-- downstream joins must consume the artifact registry as known-absent
-- (never synthesize or invent the game, never pad a season count to 272).
-- Its provider-side Odds event appears in the artifact's unmatched_odds
-- (reason 'no_match', 2023-01-03 Bengals home vs Bills).
--
-- Multiplicity (measured 2026-09-16, run 35041420559): the Odds provider
-- re-issued event ids across historical snapshots, so one nflverse game
-- maps to a median of 2 (max 5) distinct Odds event ids. The table is
-- therefore keyed by (nflverse_game_id, odds_event_id) pairs, not 1:1.
-- Downstream quote joins should map ANY observed odds_event_id to its
-- nflverse_game_id.

CREATE TABLE IF NOT EXISTS public.nfl_game_identity (
    nflverse_game_id TEXT NOT NULL,
    odds_event_id TEXT NOT NULL,
    match_type TEXT NOT NULL
        CHECK (match_type IN ('alias_date', 'alias_date_shift',
                             'swapped', 'swapped_date_shift')),
    season INTEGER NOT NULL,
    week INTEGER,
    game_type TEXT,
    gameday DATE,
    odds_commence_time TIMESTAMPTZ,
    date_shift INTEGER NOT NULL DEFAULT 0,
    -- Extensible join hooks (populated by downstream lanes; NULL until then)
    nflverse_gsis TEXT,
    injury_report_key TEXT,
    weather_station_key TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (nflverse_game_id, odds_event_id)
);

CREATE INDEX IF NOT EXISTS idx_nfl_game_identity_event
    ON public.nfl_game_identity (odds_event_id);
CREATE INDEX IF NOT EXISTS idx_nfl_game_identity_season
    ON public.nfl_game_identity (season, week);
