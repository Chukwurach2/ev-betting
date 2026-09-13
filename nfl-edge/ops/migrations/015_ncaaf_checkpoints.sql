-- 015: NCAAF checkpoint + quote tables (track D of the NCAAF research
-- contract). Mirrors the nfl_edge_* schemas from 001/002 so the common
-- odds/checkpoint/CLV layer reads both sports with one code path. The
-- frozen NFL tables are never written by the ncaaf path.
--
-- Dual timestamps per contract section 1.4 (never conflated):
--   * WINDOW MEMBERSHIP (collector capture time): checkpoints.target_at /
--     deadline_at / started_at / ended_at. A checkpoint belongs to a window
--     only if the collector captured it inside the declared interval.
--   * QUOTE FRESHNESS (provider observation time): odds_quotes.observed_at
--     is the provider's last_update for that quote; freshness checks use
--     this, never capture time.
--   * collected_at is materialization time (when the row was fanned out),
--     not a measurement timestamp.
--
-- Phase-1 markets only: FULL_GAME_SPREAD, FULL_GAME_TOTAL (contract 1.2).
--
-- Additive only. Safe to apply repeatedly.

CREATE TABLE IF NOT EXISTS public.ncaaf_edge_checkpoints (
 checkpoint_key text PRIMARY KEY,
 game_id text NOT NULL REFERENCES public.games(game_id),
 kickoff timestamptz NOT NULL,
 decision_window text NOT NULL CHECK (decision_window IN ('T-24','T-3','T-90','Close','Opener')),
 target_at timestamptz NOT NULL,
 deadline_at timestamptz NOT NULL,
 started_at timestamptz NOT NULL,
 ended_at timestamptz,
 status text NOT NULL CHECK (status IN ('running','captured','missed','unavailable','failed','abandoned')),
 error text,
 quotes jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(quotes)='array'),
 CHECK (target_at < deadline_at AND deadline_at <= kickoff)
);
ALTER TABLE public.ncaaf_edge_checkpoints ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.ncaaf_edge_checkpoints FROM PUBLIC, anonymous, authenticated;
CREATE INDEX IF NOT EXISTS ncaaf_edge_checkpoint_status ON public.ncaaf_edge_checkpoints(status,started_at);

CREATE TABLE IF NOT EXISTS public.ncaaf_edge_odds_quotes (
  quote_id text PRIMARY KEY,
  checkpoint_key text REFERENCES public.ncaaf_edge_checkpoints(checkpoint_key),
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
  settlement_rules text,
  ny_licensed boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS ncaaf_edge_quotes_event_market
  ON public.ncaaf_edge_odds_quotes (provider_event_id, market, book_key, observed_at);
CREATE INDEX IF NOT EXISTS ncaaf_edge_quotes_matchup
  ON public.ncaaf_edge_odds_quotes (home_team, away_team, kickoff, observed_at);

ALTER TABLE public.ncaaf_edge_odds_quotes ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.ncaaf_edge_odds_quotes FROM PUBLIC, anonymous, authenticated;

CREATE OR REPLACE FUNCTION public.ncaaf_edge_fanout_quotes()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.status = 'captured' AND NEW.quotes IS NOT NULL
     AND jsonb_typeof(NEW.quotes) = 'array' THEN
    INSERT INTO public.ncaaf_edge_odds_quotes (
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

DROP TRIGGER IF EXISTS ncaaf_edge_quotes_fanout ON public.ncaaf_edge_checkpoints;
CREATE TRIGGER ncaaf_edge_quotes_fanout
  AFTER INSERT OR UPDATE OF status, quotes ON public.ncaaf_edge_checkpoints
  FOR EACH ROW EXECUTE FUNCTION public.ncaaf_edge_fanout_quotes();
