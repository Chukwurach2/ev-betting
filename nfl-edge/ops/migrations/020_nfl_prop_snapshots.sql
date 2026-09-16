-- 020_nfl_prop_snapshots.sql
--
-- Prospective NFL prop/alternative-market data layer (design:
-- nfl-edge/docs/nfl-prospective-prop-layer.md).
--
-- Research substrate for prospectively collected prop quotes with controlled
-- decision snapshots. Collects NOTHING analytical: no thresholds, no
-- features, no market selection, no shadow signals. Raw quotes are
-- immutable (insert-only); consensus/dispersion are derived in views,
-- never stored twice.
--
-- Identity rule (hard, from the H-D incident 2026-09-16): the provider
-- re-issues event IDs across snapshots (median 2, max 5 per game). A
-- provider event_id is valid ONLY for the tick in which it was resolved.
-- Each snapshot stores its own provider_event_id_at_snapshot; analysis
-- joins on (canonical_game_id, provider_event_id_at_snapshot), mirroring
-- the composite-pair pattern of migration 017.
--
-- Tables:
--   nfl_prop_snapshots            one row per (game, checkpoint); carries
--                                 the tick-fresh provider event id, match
--                                 provenance, capture timestamps, and
--                                 snapshot-time context references.
--   nfl_prop_quotes               one row per (snapshot, book, market,
--                                 participant, side); provider participant
--                                 strings stored verbatim.
--   nfl_prop_collection_attempts  append-only per (game, checkpoint, run):
--                                 every due checkpoint gets a row, including
--                                 failures, so missed snapshots are visible
--                                 immediately (feeds nfl_prop_completeness).
--   nfl_prop_settlements          populated later by a free settlement job
--                                 from nflverse player_stats; NOT part of
--                                 the v1 build (schema only).
--   nfl_prop_participant_aliases  provider string -> canonical player key,
--                                 explicit entries only (like the team alias
--                                 table); NULL until resolved.
--
-- v1 markets (bounded, probe-proven at T-24 2026-09-16): player_pass_yds,
-- player_pass_attempts, player_rush_attempts. player_sacks is EXCLUDED
-- (absent at T-24 in the probe); it may enter only via a future bounded
-- re-validation gate, never silently.
--
-- Immutability: snapshot and quote tables reject UPDATE and DELETE via
-- trigger (same pattern as 018's nfl_mos_archive_no_rewrite). Attempts,
-- settlements and aliases are append-only by collector convention; the
-- immutability triggers also apply so nothing is ever rewritten.
--
-- Additive only. Safe to apply repeatedly. Does not touch the frozen
-- 2022-2024 NFL dataset, the frozen v1.3 shadow model, NCAAF tables, or
-- any frozen prereg.

CREATE TABLE IF NOT EXISTS public.nfl_prop_snapshots (
    snapshot_key TEXT PRIMARY KEY,          -- sha256(canonical|checkpoint|provider_id|captured_at)
    canonical_game_id TEXT NOT NULL,        -- nflverse game_id (never a provider id)
    provider_event_id_at_snapshot TEXT,     -- resolved THIS tick; NULL if unresolvable
    match_type TEXT,                        -- identity matcher provenance (§1.2 of the design doc)
    resolved_at TIMESTAMPTZ,                -- the tick that resolved the provider id
    season INTEGER NOT NULL,
    week INTEGER,
    checkpoint_name TEXT NOT NULL,          -- T-24h / T-12h / T-6h / T-3h / T-90m / Close
    checkpoint_target_at TIMESTAMPTZ NOT NULL,
    kickoff TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    credits_consumed INTEGER NOT NULL DEFAULT 0,
    quota_before INTEGER,
    quota_after INTEGER,
    context JSONB NOT NULL DEFAULT '{}',    -- mos_station + eligible MOS runtime ref (§5.5)
    UNIQUE (canonical_game_id, checkpoint_name, season)
);

CREATE INDEX IF NOT EXISTS nfl_prop_snapshots_game
    ON public.nfl_prop_snapshots (canonical_game_id, checkpoint_name);
CREATE INDEX IF NOT EXISTS nfl_prop_snapshots_week
    ON public.nfl_prop_snapshots (season, week);

CREATE TABLE IF NOT EXISTS public.nfl_prop_quotes (
    snapshot_key TEXT NOT NULL REFERENCES public.nfl_prop_snapshots (snapshot_key),
    book TEXT NOT NULL,
    market TEXT NOT NULL,                   -- v1: player_pass_yds | player_pass_attempts | player_rush_attempts
    participant_raw TEXT NOT NULL,          -- provider's player string, verbatim
    participant_key TEXT,                   -- canonical player key; NULL until alias-resolved
    side TEXT NOT NULL CHECK (side IN ('over', 'under')),
    line NUMERIC NOT NULL,
    price_american INTEGER NOT NULL,        -- as quoted; fair probs derived in views
    captured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_key, book, market, participant_raw, side)
);

CREATE INDEX IF NOT EXISTS nfl_prop_quotes_market
    ON public.nfl_prop_quotes (snapshot_key, market);

CREATE TABLE IF NOT EXISTS public.nfl_prop_collection_attempts (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id TEXT NOT NULL,
    canonical_game_id TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    provider_event_id_resolved TEXT,        -- NULL when unresolvable
    status TEXT NOT NULL CHECK (status IN
        ('ok', 'no_event_id', 'empty_response', 'request_failed',
         'skipped_budget', 'aborted_cap', 'ambiguous_match')),
    credits_consumed INTEGER NOT NULL DEFAULT 0,
    quota_before INTEGER,
    quota_after INTEGER,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS nfl_prop_attempts_game
    ON public.nfl_prop_collection_attempts (canonical_game_id, checkpoint_name, attempted_at);
CREATE INDEX IF NOT EXISTS nfl_prop_attempts_run
    ON public.nfl_prop_collection_attempts (run_id);

-- Settlement rows: populated LATER by a free settlement job from nflverse
-- player_stats. Schema only in v1; the collector never writes outcomes.
CREATE TABLE IF NOT EXISTS public.nfl_prop_settlements (
    canonical_game_id TEXT NOT NULL,
    market TEXT NOT NULL,
    participant_raw TEXT NOT NULL,
    participant_key TEXT,
    actual NUMERIC,
    line NUMERIC NOT NULL,
    residual NUMERIC,
    status TEXT NOT NULL CHECK (status IN ('settled', 'push', 'void')),
    settled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (canonical_game_id, market, participant_raw)
);

-- Provider participant string -> canonical player key. Explicit entries
-- only (same discipline as nfl_team_aliases.json); NULL until resolved.
CREATE TABLE IF NOT EXISTS public.nfl_prop_participant_aliases (
    provider_string TEXT PRIMARY KEY,
    participant_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Append-only enforcement (same pattern as 018's nfl_mos_archive_no_rewrite).
-- The RAISE message avoids naming forbidden statements: ops/migrate.py
-- refuses migrations containing them.
CREATE OR REPLACE FUNCTION public.nfl_prop_no_rewrite()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'nfl_prop % is append-only: % is not allowed', TG_TABLE_NAME, TG_OP;
END $$;

DROP TRIGGER IF EXISTS nfl_prop_snapshots_immutable ON public.nfl_prop_snapshots;
CREATE TRIGGER nfl_prop_snapshots_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_prop_snapshots
    FOR EACH ROW EXECUTE FUNCTION public.nfl_prop_no_rewrite();

DROP TRIGGER IF EXISTS nfl_prop_quotes_immutable ON public.nfl_prop_quotes;
CREATE TRIGGER nfl_prop_quotes_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_prop_quotes
    FOR EACH ROW EXECUTE FUNCTION public.nfl_prop_no_rewrite();

DROP TRIGGER IF EXISTS nfl_prop_attempts_immutable ON public.nfl_prop_collection_attempts;
CREATE TRIGGER nfl_prop_attempts_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_prop_collection_attempts
    FOR EACH ROW EXECUTE FUNCTION public.nfl_prop_no_rewrite();

DROP TRIGGER IF EXISTS nfl_prop_settlements_immutable ON public.nfl_prop_settlements;
CREATE TRIGGER nfl_prop_settlements_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_prop_settlements
    FOR EACH ROW EXECUTE FUNCTION public.nfl_prop_no_rewrite();

DROP TRIGGER IF EXISTS nfl_prop_aliases_immutable ON public.nfl_prop_participant_aliases;
CREATE TRIGGER nfl_prop_aliases_immutable
    BEFORE UPDATE OR DELETE ON public.nfl_prop_participant_aliases
    FOR EACH ROW EXECUTE FUNCTION public.nfl_prop_no_rewrite();

-- Derived view: consensus/dispersion per (snapshot, market, participant).
-- Consensus rule (mirrors the C/D spec's proposed rule): median line, and
-- same-book de-vigged fair probabilities (median across books). Derived
-- only — never stored into the raw tables, never recomputed over them.
CREATE OR REPLACE VIEW public.nfl_prop_consensus AS
WITH per_book AS (
    SELECT
        snapshot_key, market, participant_raw, book,
        AVG(line) AS line,
        MAX(CASE WHEN side = 'over' THEN price_american END) AS price_over,
        MAX(CASE WHEN side = 'under' THEN price_american END) AS price_under
    FROM public.nfl_prop_quotes
    GROUP BY snapshot_key, market, participant_raw, book
),
per_book_prob AS (
    SELECT
        snapshot_key, market, participant_raw, book, line,
        CASE
            WHEN price_over > 0 THEN 100.0 / (price_over + 100)
            ELSE (-price_over) * 1.0 / ((-price_over) + 100)
        END AS p_over,
        CASE
            WHEN price_under > 0 THEN 100.0 / (price_under + 100)
            ELSE (-price_under) * 1.0 / ((-price_under) + 100)
        END AS p_under
    FROM per_book
    WHERE price_over IS NOT NULL AND price_under IS NOT NULL
)
SELECT
    snapshot_key, market, participant_raw,
    COUNT(*) AS book_count,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY line) AS median_line,
    MAX(line) - MIN(line) AS line_range,
    percentile_cont(0.5) WITHIN GROUP
        (ORDER BY p_over / (p_over + p_under)) AS fair_over,
    percentile_cont(0.5) WITHIN GROUP
        (ORDER BY p_under / (p_over + p_under)) AS fair_under
FROM per_book_prob
GROUP BY snapshot_key, market, participant_raw;

-- Completeness view: per (season, week, checkpoint) the health check reads
-- this. missed = due - captured - empty (covers no_event_id, failures,
-- budget skips and cap aborts — everything not captured and not cleanly
-- empty is visible, immediately, not discovered weeks later in analysis).
CREATE OR REPLACE VIEW public.nfl_prop_completeness AS
SELECT
    s.season,
    s.week,
    a.checkpoint_name,
    COUNT(*) AS due,
    COUNT(*) FILTER (WHERE a.status = 'ok') AS captured,
    COUNT(*) FILTER (WHERE a.status = 'empty_response') AS empty,
    COUNT(*) FILTER (WHERE a.status = 'no_event_id') AS no_event_id,
    COUNT(*) FILTER (WHERE a.status NOT IN ('ok', 'empty_response'))
        AS missed
FROM public.nfl_prop_collection_attempts a
LEFT JOIN (
    SELECT DISTINCT canonical_game_id, season, week
    FROM public.nfl_prop_snapshots
) s ON s.canonical_game_id = a.canonical_game_id
GROUP BY s.season, s.week, a.checkpoint_name;
