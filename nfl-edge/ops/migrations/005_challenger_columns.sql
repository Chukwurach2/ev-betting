-- 005: Challenger model annotation on shadow picks (informational only).
--
-- The challenger is a research signal: its fair probability for the picked
-- selection/line plus its predicted margin/total, recorded so model-vs-market
-- disagreement can be studied. It NEVER triggers picks by itself; the
-- validation gate (model/challenger/validate.py) has not promoted it.
--
-- Additive only. Safe to apply repeatedly.

ALTER TABLE public.nfl_edge_picks
  ADD COLUMN IF NOT EXISTS challenger_version text,
  ADD COLUMN IF NOT EXISTS challenger_fair_prob double precision
    CHECK (challenger_fair_prob IS NULL OR (challenger_fair_prob > 0 AND challenger_fair_prob < 1)),
  ADD COLUMN IF NOT EXISTS challenger_pred_margin double precision,
  ADD COLUMN IF NOT EXISTS challenger_pred_total double precision;
