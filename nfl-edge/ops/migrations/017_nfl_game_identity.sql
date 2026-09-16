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

CREATE TABLE IF NOT EXISTS public.nfl_game_identity (
    nflverse_game_id TEXT PRIMARY KEY,
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nfl_game_identity_event
    ON public.nfl_game_identity (odds_event_id);
CREATE INDEX IF NOT EXISTS idx_nfl_game_identity_season
    ON public.nfl_game_identity (season, week);
