-- 022: record which checkpoint's consensus the settlement line came from.
-- Additive only: existing settlement rows (if any) keep NULL line_source.
ALTER TABLE public.nfl_prop_settlements
    ADD COLUMN IF NOT EXISTS line_source TEXT;
