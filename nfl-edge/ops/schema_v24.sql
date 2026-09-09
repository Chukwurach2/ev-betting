-- Additive production-hardening migration; no destructive statements.
CREATE TABLE IF NOT EXISTS public.nfl_edge_run_receipts (
 id bigserial primary key, run_key text unique not null, started_at timestamptz not null,
 ended_at timestamptz, source_coverage jsonb not null default '{}'::jsonb,
 snapshots_written integer not null default 0, missed_checkpoints jsonb not null default '[]'::jsonb,
 errors jsonb not null default '[]'::jsonb, publication_status text not null default 'pending'
);
ALTER TABLE public.odds_snapshots ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE public.odds_snapshots ADD COLUMN IF NOT EXISTS source_observed_at timestamptz;
ALTER TABLE public.odds_snapshots ADD COLUMN IF NOT EXISTS ingested_at timestamptz not null default now();
ALTER TABLE public.odds_snapshots ADD COLUMN IF NOT EXISTS settlement_rules text;
ALTER TABLE public.odds_snapshots ADD COLUMN IF NOT EXISTS market_key text;
ALTER TABLE public.model_snapshots ADD COLUMN IF NOT EXISTS source_observed_at timestamptz;
ALTER TABLE public.model_snapshots ADD COLUMN IF NOT EXISTS minutes_to_kickoff integer;
ALTER TABLE public.model_snapshots ADD COLUMN IF NOT EXISTS settlement_rules text;
ALTER TABLE public.model_snapshots ADD COLUMN IF NOT EXISTS deterministic_key text;
CREATE UNIQUE INDEX IF NOT EXISTS model_snapshots_deterministic_key_uidx ON public.model_snapshots(deterministic_key) WHERE deterministic_key IS NOT NULL;
