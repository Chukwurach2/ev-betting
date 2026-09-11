-- 006: Pipeline heartbeats for health monitoring.
--
-- Each scheduled component upserts its last successful run here. The health
-- check (ops/health.py) and the public /api/health endpoint compare these
-- timestamps against freshness thresholds. No secrets, no internals: only
-- coarse ok/stale/idle status is ever exposed.
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.nfl_edge_heartbeats (
  component text PRIMARY KEY,
  last_ok_at timestamptz NOT NULL DEFAULT now(),
  detail jsonb
);

ALTER TABLE public.nfl_edge_heartbeats ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_heartbeats FROM PUBLIC, anonymous, authenticated;
