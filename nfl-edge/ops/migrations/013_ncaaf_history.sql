-- 013: NCAAF historical market snapshots (sport-parameterized backfill).
--
-- Mirrors 007 (+008 audit columns, +010 moneyline market) for the ncaaf
-- sport. SEPARATE tables from the NFL ones: the frozen NFL dataset
-- (fingerprint 43f853a44bf93937d85149ca5fd7241b) is never written by the
-- ncaaf path. Table names come from the ops/sports.py registry
-- (table_prefix = 'ncaaf_edge').
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.ncaaf_edge_market_history (
  snapshot_at timestamptz NOT NULL,
  regions text NOT NULL,
  markets text NOT NULL,
  requested_at timestamptz NOT NULL,
  provider_snapshot_id text,
  books_present text[],
  payload jsonb NOT NULL,
  credits_used integer,
  collected_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (snapshot_at, regions, markets)
);

CREATE TABLE IF NOT EXISTS public.ncaaf_edge_historical_quotes (
  quote_id text PRIMARY KEY,
  provider text NOT NULL DEFAULT 'the_odds_api',
  provider_event_id text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  kickoff timestamptz NOT NULL,
  sportsbook text NOT NULL,
  book_key text NOT NULL,
  market text NOT NULL CHECK (market IN ('FULL_GAME_SPREAD', 'FULL_GAME_TOTAL', 'FULL_GAME_MONEYLINE')),
  selection text NOT NULL,
  line numeric NOT NULL,
  american_odds integer NOT NULL,
  fair_probability numeric NOT NULL CHECK (fair_probability > 0 AND fair_probability < 1),
  observed_at timestamptz NOT NULL,
  collected_at timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL DEFAULT 'historical_backfill',
  ny_licensed boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS ncaaf_edge_hist_quotes_event_market
  ON public.ncaaf_edge_historical_quotes (provider_event_id, market, book_key, observed_at);
CREATE INDEX IF NOT EXISTS ncaaf_edge_hist_quotes_matchup
  ON public.ncaaf_edge_historical_quotes (home_team, away_team, kickoff, observed_at);

ALTER TABLE public.ncaaf_edge_market_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ncaaf_edge_historical_quotes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.ncaaf_edge_market_history FROM PUBLIC, anonymous, authenticated;
REVOKE ALL ON public.ncaaf_edge_historical_quotes FROM PUBLIC, anonymous, authenticated;
