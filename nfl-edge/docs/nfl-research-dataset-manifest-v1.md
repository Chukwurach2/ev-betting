# NFL research-dataset manifest — v1

## Identity

- **version:** v1
- **scope:** `nfl-2022-2024-spread-total`
- **table:** `public.nfl_edge_historical_quotes`
- **markets:** `FULL_GAME_SPREAD`, `FULL_GAME_TOTAL`
  (the post-freeze `FULL_GAME_MONEYLINE` backfill is out of scope)
- **seasons:** NFL regular seasons 2022–2024 (postseason excluded)
- **row_count:** 291,586
- **snapshot_count:** 162
- **dataset_fingerprint (canonical-1):** `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`
- **algorithm:** `nfl-edge/ops/fingerprint_dataset.py`, method `canonical-1`
- **algorithm_commit:** `16c80be7e3be653d2631fc9107e961181c77155b`
- **created_at:** `2026-09-16T02:05:15Z` (run 35046566235, zero API credits)
- **manifest table row:** `research_dataset_manifests(version='v1',
  scope='nfl-2022-2024-spread-total')`

## Fingerprint method (canonical-1, frozen)

SHA-256 over the canonical preimage: every quote row rendered
explicitly (NULL → `\x00`; datetimes → UTC Zulu
`2024-09-04T11:55:38Z`; numerics → normalized plain notation;
booleans → `true`/`false`), columns joined with `|`, rows terminated
with `\n`, rows sorted by rendered tuple in Python (no SQL ORDER BY, no
driver-dependent `str()`). `collected_at` excluded (insert metadata).
Full 64-char hex digest. Fully documented in the module docstring of
`nfl-edge/ops/fingerprint_dataset.py`; byte-pinned by unit tests in
`nfl-edge/tests/test_fingerprint_dataset.py`.

## Supersession note

The 2026-09-12 fingerprint `43f853a44bf93937d85149ca5fd7241b` stands as
the **historical record** of the freeze event, but its construction was
undocumented and is not reproducible — it is superseded by v1 as the
**verification authority** going forward. The frozen subset was
independently proven byte-intact at value level (forensic run
35045570451: all 291,586 `quote_id`s recompute exactly, all
`fair_probability` values re-derive exactly, zero unpaired groups).
See `nfl-edge/docs/nfl-freeze-addendum-2026-09-16.md`.

## Immutability policy

**v1 is never edited.** Any scope change (new seasons, new markets,
re-backfill, column changes) mints **v2** with its own fingerprint row.
The `research_dataset_manifests` table is INSERT-only: a trigger blocks
UPDATE and DELETE (migration
`nfl-edge/ops/migrations/019_research_dataset_manifests.sql`).

## Verification

One command, read-only on data tables, zero API credits:

```
# via GitHub Actions:
gh workflow run nfl-dataset-verify.yml \
  --ref main -f scope=nfl-2022-2024-spread-total -f version=latest
```

or locally:

```
NFL_EDGE_DATABASE_URL=... python nfl-edge/ops/verify_dataset_manifest.py \
  --scope nfl-2022-2024-spread-total --version latest
```

Any mismatch in row count, snapshot count, or fingerprint exits
nonzero with `::error::` annotations. After ANY unrelated backfill
touches `nfl_edge_historical_quotes`, run this before doing forensics.
