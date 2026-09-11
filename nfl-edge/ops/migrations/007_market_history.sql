-- 007: Historical market snapshots (paid-tier backfill).
--
-- The Odds API historical endpoints (5-min granularity since Sep 2022) are
-- the training data for market-only-v1: opener->close movement, stale-price
-- patterns, book disagreement. nfl_edge_market_history stores the raw
-- snapshot envelopes; nfl_edge_historical_quotes stores the normalized
-- quote grain (same shape as nfl_edge_odds_quotes, minus the live
-- checkpoint FK) so the research feature builders can read both tables
-- with one code path.
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.nfl_edge_market_history (
  snapshot_at timestamptz NOT NULL,
  regions text NOT NULL,
  markets text NOT NULL,
  requested_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  credits_used integer,
  collected_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (snapshot_at, regions, markets)
);

CREATE TABLE IF NOT EXISTS public.nfl_edge_historical_quotes (
  quote_id text PRIMARY KEY,
  provider text NOT NULL DEFAULT 'the_odds_api',
  provider_event_id text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  kickoff timestamptz NOT NULL,
  sportsbook text NOT NULL,
  book_key text NOT NULL,
  market text NOT NULL CHECK (market IN ('FULL_GAME_SPREAD', 'FULL_GAME_TOTAL')),
  selection text NOT NULL,
  line numeric NOT NULL,
  american_odds integer NOT NULL,
  fair_probability numeric NOT NULL CHECK (fair_probability > 0 AND fair_probability < 1),
  observed_at timestamptz NOT NULL,
  collected_at timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL DEFAULT 'historical_backfill',
  ny_licensed boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS nfl_edge_hist_quotes_event_market
  ON public.nfl_edge_historical_quotes (provider_event_id, market, book_key, observed_at);
CREATE INDEX IF NOT EXISTS nfl_edge_hist_quotes_matchup
  ON public.nfl_edge_historical_quotes (home_team, away_team, kickoff, observed_at);

ALTER TABLE public.nfl_edge_market_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.nfl_edge_historical_quotes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_market_history FROM PUBLIC, anonymous, authenticated;
REVOKE ALL ON public.nfl_edge_historical_quotes FROM PUBLIC, anonymous, authenticated;
