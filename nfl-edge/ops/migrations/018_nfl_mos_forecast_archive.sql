-- 018_nfl_mos_forecast_archive.sql
--
-- Prospective NFL MOS forecast archive (infrastructure collection, NOT a
-- claim-bearing test). Every unarchived week is point-in-time forecast
-- information that can never be recreated as cleanly, so the collector
-- (nfl-edge/ops/mos_forecast_archive.py) runs 4x daily and stores every
-- forecast cycle retrieved before each game's frozen prediction cutoff.
--
-- Frozen research rules (RULE_VERSION = 'v1'; also recorded per selection row):
--   * prediction_cutoff = kickoff - 24 hours (kickoff parsed from the
--     nflverse schedule as America/New_York -> UTC).
--   * A cycle is ELIGIBLE iff runtime + 4h (dissemination lag) <= cutoff.
--   * The mechanically SELECTED cycle = max eligible runtime per game.
--     Selection is derived, never hand-picked; raw cycles are retained so
--     the information set is auditable without outcome-driven selection.
--   * Retrieval is prospective only: rows are inserted only while
--     retrieved_at <= prediction_cutoff. Retrieval failures and unavailable
--     states are preserved in nfl_mos_retrieval_attempts and NEVER
--     backfilled later.
--
-- Tables:
--   nfl_mos_forecast_archive   one row per (game, station, model, runtime,
--                              ftime): the immutable forecast facts.
--   nfl_mos_cycle_selection_log append-only log of what the mechanical
--                              selection rule produced on each run; the
--                              latest row per game is authoritative.
--   nfl_mos_retrieval_attempts append-only log of every retrieval attempt
--                              with an explicit status.
--
-- Immutability: the archive table rejects UPDATE and DELETE via trigger.
-- Only INSERT (collector) is allowed. Selection and attempt logs are
-- append-only by convention (the collector only INSERTs).

CREATE TABLE IF NOT EXISTS public.nfl_mos_forecast_archive (
    nflverse_game_id TEXT NOT NULL,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    stadium TEXT NOT NULL,
    mos_station TEXT NOT NULL,
    forecast_model TEXT NOT NULL,
    runtime TIMESTAMPTZ NOT NULL,
    ftime TIMESTAMPTZ NOT NULL,
    kickoff TIMESTAMPTZ NOT NULL,
    prediction_cutoff TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw JSONB NOT NULL,
    PRIMARY KEY (nflverse_game_id, mos_station, forecast_model, runtime, ftime)
);

CREATE INDEX IF NOT EXISTS nfl_mos_archive_station_runtime
    ON public.nfl_mos_forecast_archive (mos_station, forecast_model, runtime);
CREATE INDEX IF NOT EXISTS nfl_mos_archive_game
    ON public.nfl_mos_forecast_archive (nflverse_game_id, runtime);

-- Append-only enforcement for the archive table.
CREATE OR REPLACE FUNCTION public.nfl_mos_archive_no_rewrite()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'nfl_mos_forecast_archive is append-only: % is not allowed', TG_OP;
END $$;

DROP TRIGGER IF EXISTS nfl_mos_archive_immutable ON public.nfl_mos_forecast_archive;
CREATE TRIGGER nfl_mos_archive_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_mos_forecast_archive
    FOR EACH ROW EXECUTE FUNCTION public.nfl_mos_archive_no_rewrite();

CREATE TABLE IF NOT EXISTS public.nfl_mos_cycle_selection_log (
    id SERIAL PRIMARY KEY,
    nflverse_game_id TEXT NOT NULL,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rule_version TEXT NOT NULL,
    prediction_cutoff TIMESTAMPTZ NOT NULL,
    selected_runtime TIMESTAMPTZ,
    n_cycles_archived INTEGER NOT NULL DEFAULT 0,
    n_eligible_cycles INTEGER NOT NULL DEFAULT 0,
    is_final BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS nfl_mos_selection_log_game
    ON public.nfl_mos_cycle_selection_log (nflverse_game_id, computed_at);

CREATE TABLE IF NOT EXISTS public.nfl_mos_retrieval_attempts (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    nflverse_game_id TEXT,
    stadium TEXT,
    mos_station TEXT,
    forecast_model TEXT NOT NULL,
    runtime TIMESTAMPTZ,
    status TEXT NOT NULL,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS nfl_mos_attempts_game
    ON public.nfl_mos_retrieval_attempts (nflverse_game_id, attempted_at);

-- Convenience view: the latest mechanical selection per game.
CREATE OR REPLACE VIEW public.nfl_mos_selected_cycle AS
SELECT DISTINCT ON (nflverse_game_id)
    nflverse_game_id,
    season,
    week,
    computed_at AS selected_at,
    rule_version,
    prediction_cutoff,
    selected_runtime,
    n_cycles_archived,
    n_eligible_cycles,
    is_final
FROM public.nfl_mos_cycle_selection_log
ORDER BY nflverse_game_id, computed_at DESC;
