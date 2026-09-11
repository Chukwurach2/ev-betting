-- 008: Audit-trail columns for historical pulls (mandated 2026-09-12).
--
-- Per-pull audit requirements: provider snapshot ID, requested timestamp,
-- returned timestamp, regions, books present, credit cost. requested_at,
-- snapshot_at (the envelope's returned timestamp), regions, and
-- credits_used already existed; this adds the rest.
--
-- provider_snapshot_id: the historical odds endpoint mints no snapshot IDs;
-- snapshots are keyed by request date, so the requested ISO datetime is the
-- canonical ID. books_present: sorted book keys seen in the raw payload.
--
-- Additive only. Safe to apply repeatedly.
ALTER TABLE public.nfl_edge_market_history
  ADD COLUMN IF NOT EXISTS provider_snapshot_id text,
  ADD COLUMN IF NOT EXISTS books_present text[];
