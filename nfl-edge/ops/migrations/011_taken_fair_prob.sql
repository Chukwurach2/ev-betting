-- 011: Store the taken quote's de-vigged fair probability on each shadow pick.
--
-- CLV must compare the TAKEN price against the closing price at the same
-- line (positive = beat the close). The pick-time consensus alone cannot
-- serve as the entry price: consensus-minus-close has the wrong sign in the
-- canonical slow-book scenario and rewards the bet exactly when the market
-- proves the pick wrong. Additive only. Safe to apply repeatedly.

ALTER TABLE public.nfl_edge_picks
  ADD COLUMN IF NOT EXISTS taken_fair_prob numeric;
