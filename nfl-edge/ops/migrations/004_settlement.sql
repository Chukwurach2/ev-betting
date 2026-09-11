-- 004: Settlement and CLV fields on the shadow picks ledger.
--
-- Additive only. Safe to apply repeatedly.

ALTER TABLE public.nfl_edge_picks
  ADD COLUMN IF NOT EXISTS final_home_score integer,
  ADD COLUMN IF NOT EXISTS final_away_score integer,
  ADD COLUMN IF NOT EXISTS clv_prob_points numeric,
  ADD COLUMN IF NOT EXISTS settled_by text;
