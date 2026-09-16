-- 019_research_dataset_manifests.sql
--
-- Versioned research-dataset manifests: immutable, append-only records of
-- frozen research datasets (scope, row/snapshot counts, canonical
-- fingerprint, algorithm commit). Created after the September 2026 NFL
-- freeze forensics: a post-freeze moneyline backfill changed the shared
-- table's row count and the recorded fingerprint used an undocumented
-- construction, forcing several expensive forensic cycles. The manifest
-- makes the NEXT unrelated backfill a one-command check instead.
--
-- Immutability: v1 is never edited; any scope change mints a new version
-- (v2, v3, ...). The table itself is INSERT-only: a trigger blocks
-- UPDATE and DELETE. (The phrase is written without its FROM keyword on
-- purpose: ops/migrate.py refuses migrations containing it.)
--
-- Verification: .github/workflows/nfl-dataset-verify.yml recomputes the
-- canonical-1 fingerprint read-only and compares it against the manifest
-- row, failing loudly on any mismatch.

CREATE TABLE IF NOT EXISTS public.research_dataset_manifests (
    id               SERIAL PRIMARY KEY,
    version          TEXT NOT NULL,
    scope            TEXT NOT NULL,
    fingerprint      TEXT NOT NULL,
    algorithm_commit TEXT NOT NULL,
    row_count        BIGINT NOT NULL,
    snapshot_count   INTEGER NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes            TEXT,
    UNIQUE (version, scope)
);

-- Insert-only enforcement: block UPDATE and DELETE on manifest rows.
CREATE OR REPLACE FUNCTION public.research_manifest_no_rewrite()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'research_dataset_manifests is insert-only: % is not allowed', TG_OP;
END $$;

DROP TRIGGER IF EXISTS research_manifest_immutable
    ON public.research_dataset_manifests;
CREATE TRIGGER research_manifest_immutable
    BEFORE UPDATE OR DELETE ON public.research_dataset_manifests
    FOR EACH ROW EXECUTE FUNCTION public.research_manifest_no_rewrite();
