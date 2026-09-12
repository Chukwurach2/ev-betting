-- 012: Link each shadow pick to the checkpoint whose quotes fed it.
--
-- The weekly forward-shadow report must attribute picks to decision windows
-- (Opener/T-24/T-3/T-90/Close) so a "great" CLV week cannot hide a
-- partial-data week. The checkpoint_key is available on the quote row the
-- picks engine reads (nfl_edge_odds_quotes.checkpoint_key), so this is a
-- plain attribution column, not a new data source.
--
-- Additive only. Safe to apply repeatedly.
ALTER TABLE public.nfl_edge_picks
  ADD COLUMN IF NOT EXISTS checkpoint_key text
  REFERENCES public.nfl_edge_checkpoints(checkpoint_key);
