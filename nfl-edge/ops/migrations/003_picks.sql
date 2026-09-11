-- 003: Shadow picks ledger for the consensus edge engine.
--
-- Every row is a shadow recommendation: mode is always 'shadow' until a model
-- passes the production gate (see model/production_gate.py). No row in this
-- table may be presented as a production wager.
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.nfl_edge_picks (
  pick_id text PRIMARY KEY,
  engine_version text NOT NULL,
  mode text NOT NULL DEFAULT 'shadow'
    CHECK (mode IN ('shadow', 'challenger', 'production')),
  game_id text REFERENCES public.games(game_id),
  provider_event_id text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  kickoff timestamptz NOT NULL,
  market text NOT NULL,
  selection text NOT NULL,
  line numeric NOT NULL,
  sportsbook text NOT NULL,
  book_key text NOT NULL,
  american_odds integer NOT NULL,
  decimal_odds numeric NOT NULL CHECK (decimal_odds > 1),
  consensus_fair_prob numeric NOT NULL CHECK (consensus_fair_prob > 0 AND consensus_fair_prob < 1),
  edge numeric NOT NULL,
  kelly_fraction numeric NOT NULL CHECK (kelly_fraction >= 0),
  stake_units numeric NOT NULL CHECK (stake_units >= 0),
  consensus_books integer NOT NULL CHECK (consensus_books >= 2),
  observed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  result text CHECK (result IN ('win', 'loss', 'push', 'void')),
  settled_at timestamptz
);

CREATE INDEX IF NOT EXISTS nfl_edge_picks_event
  ON public.nfl_edge_picks (provider_event_id, market, created_at DESC);
CREATE INDEX IF NOT EXISTS nfl_edge_picks_mode
  ON public.nfl_edge_picks (mode, created_at DESC);

ALTER TABLE public.nfl_edge_picks ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_picks FROM PUBLIC, anonymous, authenticated;
