-- 009: Allow the 'Opener' checkpoint in the decision_window check.
--
-- The Opener capture feature (first-seen lines for games beyond the T-24
-- horizon) stores c.name='Opener' in nfl_edge_checkpoints.decision_window,
-- but 001's CHECK constraint only allowed ('T-24','T-3','T-90','Close').
-- Every collector run since the feature shipped died on the first Opener
-- claim with CheckViolation (diagnosed 2026-09-12 via loud failure).
--
-- Additive only. Safe to apply repeatedly.
ALTER TABLE public.nfl_edge_checkpoints
  DROP CONSTRAINT IF EXISTS nfl_edge_checkpoints_decision_window_check;
ALTER TABLE public.nfl_edge_checkpoints
  ADD CONSTRAINT nfl_edge_checkpoints_decision_window_check
  CHECK (decision_window IN ('T-24','T-3','T-90','Close','Opener'));
