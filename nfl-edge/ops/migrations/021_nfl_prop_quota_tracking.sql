-- 021_nfl_prop_quota_tracking.sql
--
-- Empirical credit tracking for the NFL prop layer (design §7:
-- nfl-edge/docs/nfl-prospective-prop-layer.md).
--
-- Standing rule: future research economics are MEASURED, not assumed
-- (the H-D lesson, 2026-09-16: historical per-event odds billed ~18.6/call
-- for 2 markets vs the 10x2 nominal). Every provider call the prop
-- collector makes — including the free events-list calls (logged at 0, so
-- a future audit can see the full request pattern) — is logged through
-- the extended ops/quota_log.py with per-call billed credits from the
-- x-requests-* response headers.
--
-- The ncaaf_quota_log table is already shared across sports via its
-- `sport` column (see ops/quota_log.py); these columns are additive.
-- The empirical cost-model view is what any future prereg or authorization
-- request cites for budgeting — never the nominal A1 assumptions.
--
-- Additive only. Safe to apply repeatedly.

-- The ncaaf_quota_log table is created lazily by ops/quota_log.py on first
-- use; create it here too so this migration applies cleanly on a fresh
-- database (same base columns as quota_log.py, plus the new ones).
CREATE TABLE IF NOT EXISTS public.ncaaf_quota_log (
    id SERIAL PRIMARY KEY,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sport TEXT NOT NULL,
    request_type TEXT NOT NULL,
    quota_remaining_before INTEGER,
    quota_used_before INTEGER,
    quota_remaining_after INTEGER,
    quota_used_after INTEGER,
    credits_used INTEGER,
    n_events INTEGER DEFAULT 0,
    endpoint TEXT,                      -- e.g. nfl_prop_odds_call, nfl_prop_events_list
    n_markets INTEGER,                  -- markets requested on this call
    billed_credits INTEGER,             -- measured from x-requests-* headers
    run_id TEXT                         -- collector run that made the call
);

-- Belt-and-braces for tables created by an older quota_log.py: the ALTERs
-- are no-ops when the columns already exist.
ALTER TABLE public.ncaaf_quota_log
    ADD COLUMN IF NOT EXISTS endpoint TEXT;
ALTER TABLE public.ncaaf_quota_log
    ADD COLUMN IF NOT EXISTS n_markets INTEGER;
ALTER TABLE public.ncaaf_quota_log
    ADD COLUMN IF NOT EXISTS billed_credits INTEGER;
ALTER TABLE public.ncaaf_quota_log
    ADD COLUMN IF NOT EXISTS run_id TEXT;

CREATE INDEX IF NOT EXISTS ncaaf_quota_log_endpoint
    ON public.ncaaf_quota_log (endpoint, logged_at);

-- Empirical cost model: rolling 30-day measured billed credits per call,
-- by (endpoint, n_markets). Rows here come from x-requests-* headers on
-- real responses — the standing answer to the H-D billing lesson.
-- The nfl_prop_odds_call row IS the per-game-checkpoint realized cost
-- (one paid call per game-checkpoint by construction).
CREATE OR REPLACE VIEW public.nfl_prop_cost_model AS
SELECT
    endpoint,
    n_markets,
    COUNT(*) AS n_calls,
    AVG(billed_credits) AS avg_billed,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY billed_credits) AS p50_billed,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY billed_credits) AS p90_billed,
    MIN(logged_at) AS first_seen,
    MAX(logged_at) AS last_seen
FROM public.ncaaf_quota_log
WHERE logged_at > NOW() - INTERVAL '30 days'
  AND endpoint LIKE 'nfl_prop\_%' ESCAPE '\'
  AND billed_credits IS NOT NULL
GROUP BY endpoint, n_markets;
