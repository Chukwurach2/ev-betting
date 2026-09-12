-- Immutable settled outcomes for historical calibration research.
-- Additive, RLS-protected, and safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.nfl_edge_historical_outcomes (
  nflverse_game_id text PRIMARY KEY,
  season integer NOT NULL CHECK (season BETWEEN 1920 AND 2200),
  week integer NOT NULL CHECK (week BETWEEN 1 AND 25),
  game_type text NOT NULL,
  game_date date NOT NULL,
  home_code text NOT NULL,
  away_code text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  home_score integer NOT NULL CHECK (home_score >= 0),
  away_score integer NOT NULL CHECK (away_score >= 0),
  source_url text NOT NULL,
  source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  ingested_at timestamptz NOT NULL DEFAULT now(),
  settlement_rules text NOT NULL DEFAULT 'NFL final score; spread pushes at adjusted margin=0; totals push at total=line',
  CHECK (home_code <> away_code),
  UNIQUE (season, week, game_type, home_code, away_code)
);

ALTER TABLE public.nfl_edge_historical_outcomes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_historical_outcomes FROM PUBLIC;

CREATE INDEX IF NOT EXISTS nfl_edge_historical_outcomes_match_idx
  ON public.nfl_edge_historical_outcomes (home_team, away_team, game_date);
