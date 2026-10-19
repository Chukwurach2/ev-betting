-- Separate exploratory NBA storage; no NFL model or personal journal changes.
CREATE TABLE IF NOT EXISTS public.nba_edge_odds_quotes (
 quote_id text PRIMARY KEY,
 provider_event_id text NOT NULL,
 home_team text NOT NULL, away_team text NOT NULL, kickoff timestamptz NOT NULL,
 sportsbook text NOT NULL, book_key text NOT NULL,
 market text NOT NULL CHECK (market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')),
 selection text NOT NULL, line numeric NOT NULL,
 american_odds integer NOT NULL CHECK (abs(american_odds)>=100),
 fair_probability numeric NOT NULL CHECK (fair_probability>0 AND fair_probability<1),
 observed_at timestamptz NOT NULL, collected_at timestamptz NOT NULL DEFAULT now(),
 settlement_rules text NOT NULL
);
CREATE INDEX IF NOT EXISTS nba_phone_quotes_time ON public.nba_edge_odds_quotes(kickoff,observed_at DESC);
