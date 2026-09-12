-- 010: allow moneyline quotes in the frozen-adjacent historical table.
-- The 2022-2024 spread/total dataset (fingerprint 43f853a44bf93937d85149ca5fd7241b)
-- is untouched; this only widens the market CHECK so the moneyline pilot
-- (preregistered docs/preregistrations/moneyline-pilot-v1.md) can be stored
-- with the same audit trail. Idempotent.
ALTER TABLE public.nfl_edge_historical_quotes
  DROP CONSTRAINT IF EXISTS nfl_edge_historical_quotes_market_check;
ALTER TABLE public.nfl_edge_historical_quotes
  ADD CONSTRAINT nfl_edge_historical_quotes_market_check
  CHECK (market IN ('FULL_GAME_SPREAD', 'FULL_GAME_TOTAL', 'FULL_GAME_MONEYLINE'));
