-- 014: sport column on public.games (Football Edge: shared platform).
--
-- The checkpoint collector filters games by sport so the NFL and NCAAF
-- collectors read disjoint game sets. Existing rows are all NFL and take
-- the 'nfl' default; the NCAAF schedule sync (ops/sync_ncaaf_schedule.py)
-- writes sport='ncaaf' explicitly and refuses to run until this column
-- exists.
--
-- Additive only. Safe to apply repeatedly.

ALTER TABLE public.games
  ADD COLUMN IF NOT EXISTS sport text NOT NULL DEFAULT 'nfl';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'games_sport_check'
  ) THEN
    ALTER TABLE public.games
      ADD CONSTRAINT games_sport_check CHECK (sport IN ('nfl', 'ncaaf'));
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS games_sport_kickoff
  ON public.games (sport, kickoff);
