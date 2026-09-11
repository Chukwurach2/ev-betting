-- Isolated shadow collection. No change to bets, bankrolls or production picks.
CREATE TABLE IF NOT EXISTS public.nfl_edge_checkpoints (
 checkpoint_key text PRIMARY KEY,
 game_id text NOT NULL REFERENCES public.games(game_id),
 kickoff timestamptz NOT NULL,
 decision_window text NOT NULL CHECK (decision_window IN ('T-24','T-3','T-90','Close')),
 target_at timestamptz NOT NULL,
 deadline_at timestamptz NOT NULL,
 started_at timestamptz NOT NULL,
 ended_at timestamptz,
 status text NOT NULL CHECK (status IN ('running','captured','missed','unavailable','failed','abandoned')),
 error text,
 quotes jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(quotes)='array'),
 CHECK (target_at < deadline_at AND deadline_at <= kickoff)
);
ALTER TABLE public.nfl_edge_checkpoints ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nfl_edge_checkpoints FROM PUBLIC, anonymous, authenticated;
CREATE INDEX IF NOT EXISTS nfl_edge_checkpoint_status ON public.nfl_edge_checkpoints(status,started_at);
