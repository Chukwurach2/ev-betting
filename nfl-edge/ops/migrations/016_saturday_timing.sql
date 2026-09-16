-- 016_saturday_timing.sql
-- Schema for the one-time Saturday 2026-09-19 opportunity-lifetime
-- experiment (experiment='saturday-timing'). Measurement-only tables;
-- they never touch the frozen v1.0 rule or any NFL tables.
--
-- Design:
--   ncaaf_timing_runs        one row per 15-minute observation run,
--                            with mechanical slot, credit accounting,
--                            and explicit run status.
--   ncaaf_timing_book_states explicit per-book snapshot states for the
--                            mechanical contract universe:
--                            available / absent / request_failed / unmapped.
--   ncaaf_timing_quotes      stored totals quotes, keyed to run+slot.

CREATE TABLE IF NOT EXISTS public.ncaaf_timing_runs (
    id SERIAL PRIMARY KEY,
    experiment TEXT NOT NULL,
    slot TIMESTAMPTZ NOT NULL,
    run_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMPTZ,
    n_events_universe INTEGER NOT NULL DEFAULT 0,
    n_events_attempted INTEGER NOT NULL DEFAULT 0,
    n_events_capped INTEGER NOT NULL DEFAULT 0,
    n_books_universe INTEGER NOT NULL DEFAULT 0,
    credits_remaining_before INTEGER,
    credits_used_before INTEGER,
    credits_remaining_after INTEGER,
    credits_used_after INTEGER,
    credits_consumed INTEGER,
    status TEXT NOT NULL,
    error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ncaaf_timing_runs_exp_slot
    ON public.ncaaf_timing_runs (experiment, slot);

CREATE TABLE IF NOT EXISTS public.ncaaf_timing_quotes (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES public.ncaaf_timing_runs (id),
    experiment TEXT NOT NULL,
    slot TIMESTAMPTZ NOT NULL,
    provider_event_id TEXT NOT NULL,
    book_key TEXT NOT NULL,
    market TEXT NOT NULL,
    selection TEXT NOT NULL,
    line DOUBLE PRECISION,
    american_odds INTEGER,
    fair_probability DOUBLE PRECISION,
    observed_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- run_id/slot were added after the first draft of this table; keep the
-- migration idempotent if an older shape already exists.
ALTER TABLE public.ncaaf_timing_quotes
    ADD COLUMN IF NOT EXISTS run_id INTEGER REFERENCES public.ncaaf_timing_runs (id);
ALTER TABLE public.ncaaf_timing_quotes
    ADD COLUMN IF NOT EXISTS slot TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ncaaf_timing_quotes_exp_slot_event
    ON public.ncaaf_timing_quotes (experiment, slot, provider_event_id);

CREATE TABLE IF NOT EXISTS public.ncaaf_timing_book_states (
    run_id INTEGER NOT NULL REFERENCES public.ncaaf_timing_runs (id),
    provider_event_id TEXT NOT NULL,
    book_key TEXT NOT NULL,
    universe_member BOOLEAN NOT NULL,
    state TEXT NOT NULL
        CHECK (state IN ('available', 'absent', 'request_failed', 'unmapped')),
    observed_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, provider_event_id, book_key)
);

CREATE INDEX IF NOT EXISTS ncaaf_timing_book_states_event
    ON public.ncaaf_timing_book_states (provider_event_id, run_id);
