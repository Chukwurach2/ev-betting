-- 002: Normalized append-only history of collected shadow quotes.
--
-- nfl_edge_checkpoints stores one jsonb quotes array per checkpoint. This
-- migration fans each captured checkpoint out into one row per selection so
-- line movement can be reconstructed and true CLV computed later, without
-- depending on the shape of the checkpoints table.
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.nfl_edge_odds_quotes (
  quote_id text PRIMARY KEY,
  checkpoint_key text REFERENCES public.nfl_edge_checkpoints(checkpoint_key),
  provider text NOT NULL DEFAULT 'the_odds_api',
  provider_event_id text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  kickoff timestamptz NOT NULL,
  sportsbook text NOT NULL,
  book_key text NOT NULL,
  market text NOT NULL CHECK (market IN ('FULL_GAME_SPREAD', 'FULL_GAME_TOTAL', 'Q1_TOTAL')),
  selection text NOT NULL,
  line numeric NOT NULL,
  american_odds integer NOT NULL,
  fair_probability numeric NOT NULL CHECK (fair_probability > 0 AND fair_probability < 1),
  observed_at timestamptz NOT NULL,
  collected_at timestamptz NOT NULL DEFAULT now(),
  settlement_rules text,
  ny_licensed boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS nfl_edge_quotes_event_market
  ON public.nfl_edge_odds_quotes (provider_event_id, market, book_key, observed_at);
CREATE INDEX IF NOT EXISTS nfl_edge_quotes_matchup
  ON public.nfl_edge_odds_quotes (home_team, away_team, kickoff, observed_at);

ALTER TABLE public.nfl_edge_odds_quotes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_odds_quotes FROM PUBLIC, anonymous, authenticated;

CREATE OR REPLACE FUNCTION public.nfl_edge_fanout_quotes()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.status = 'captured' AND NEW.quotes IS NOT NULL
     AND jsonb_typeof(NEW.quotes) = 'array' THEN
    INSERT INTO public.nfl_edge_odds_quotes (
      quote_id, checkpoint_key, provider_event_id, home_team, away_team, kickoff,
      sportsbook, book_key, market, selection, line, american_odds,
      fair_probability, observed_at, settlement_rules, ny_licensed
    )
    SELECT
      encode(sha256(convert_to(
        NEW.checkpoint_key || '|' || (q->>'provider_event_id') || '|' || (q->>'sportsbook_key') || '|' ||
        (q->>'market') || '|' || (q->>'selection') || '|' || (q->>'line') || '|' || (q->>'observed_at'),
        'UTF8')), 'hex'),
      NEW.checkpoint_key,
      q->>'provider_event_id',
      g.home_team,
      g.away_team,
      NEW.kickoff,
      q->>'sportsbook',
      q->>'sportsbook_key',
      q->>'market',
      q->>'selection',
      (q->>'line')::numeric,
      (q->>'american_odds')::integer,
      (q->>'fair_probability')::numeric,
      (q->>'observed_at')::timestamptz,
      q->>'settlement_rules',
      COALESCE((q->>'ny_licensed')::boolean, true)
    FROM jsonb_array_elements(NEW.quotes) AS q
    JOIN public.games g ON g.game_id = NEW.game_id
    WHERE q ?& array['provider_event_id','sportsbook_key','market','selection','line',
                    'american_odds','fair_probability','observed_at']
    ON CONFLICT (quote_id) DO NOTHING;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS nfl_edge_quotes_fanout ON public.nfl_edge_checkpoints;
CREATE TRIGGER nfl_edge_quotes_fanout
  AFTER INSERT OR UPDATE OF status, quotes ON public.nfl_edge_checkpoints
  FOR EACH ROW EXECUTE FUNCTION public.nfl_edge_fanout_quotes();
