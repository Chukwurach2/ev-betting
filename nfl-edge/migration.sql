BEGIN;
ALTER TABLE public.placed_bets ADD COLUMN IF NOT EXISTS client_id uuid;
ALTER TABLE public.placed_bets ADD COLUMN IF NOT EXISTS revision integer NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX IF NOT EXISTS placed_bets_user_client_id ON public.placed_bets(user_id,client_id);
CREATE OR REPLACE FUNCTION public.nfl_bet_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN NEW.revision := OLD.revision + 1; RETURN NEW; END $$;
CREATE OR REPLACE TRIGGER nfl_bet_revision BEFORE UPDATE ON public.placed_bets FOR EACH ROW EXECUTE FUNCTION public.nfl_bet_revision();
CREATE OR REPLACE VIEW public.nfl_public_predictions AS
SELECT m.id,m.game_id,m.model_version,m.decision_window,m.market,m.team,m.selection,m.line,
 m.model_probability,m.fair_market_probability,m.confidence_grade,m.qualifies,m.created_at,
 m.inputs->>'ready' AS inputs_ready,m.inputs->>'settlement_rules' AS settlement_rules,
 m.inputs->>'fair_method' AS fair_method,m.inputs->>'ny_licensed' AS ny_licensed,m.inputs->>'source_url' AS source_url,
 v.metrics->>'production_validated' AS production_validated,
 o.sportsbook,o.american_odds,o.observed_at,
 g.season,g.week,g.kickoff,g.status AS game_status,
 m.model_probability-m.fair_market_probability AS edge,
 m.model_probability*(CASE WHEN o.american_odds>=100 THEN 1+o.american_odds::numeric/100
 WHEN o.american_odds<=-100 THEN 1+100::numeric/abs(o.american_odds) ELSE NULL END)-1 AS expected_value
FROM public.model_snapshots m JOIN public.games g USING(game_id)
LEFT JOIN LATERAL (SELECT * FROM public.model_versions mv WHERE mv.version_name=m.model_version AND mv.role='champion' ORDER BY mv.created_at DESC LIMIT 1) v ON true
LEFT JOIN LATERAL (SELECT * FROM public.odds_snapshots os WHERE os.model_snapshot_id=m.id AND os.is_best_ny_price ORDER BY os.observed_at DESC,os.american_odds DESC LIMIT 1) o ON true;
GRANT SELECT ON public.nfl_public_predictions TO anonymous,authenticated;
NOTIFY pgrst, 'reload schema';
COMMIT;
