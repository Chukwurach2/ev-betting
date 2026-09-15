# NCAAF Edge historical dataset — freeze record

**Status: FROZEN — SHADOW (research only; no model promotion, no production picks)**

Generated: 2026-09-15 ~15:45 UTC
Repo commit: `39e08dfb4a205064000b6a92cefb434536db489d` (main, Chukwurach2/ev-betting)
Dataset fingerprint: `684c58410968c440a6d7500582ac9ecf`

## Scope

- Sport: NCAAF (separate evidence stack; never pooled with NFL)
- Seasons: 2022, 2023, 2024, 2025 (all before the 2026-08-01 experimental cutoff;
  the entire 2026 season is prospective shadow evidence only, never training data)
- Weeks: 0–14
- Markets: spreads, totals (FBS spreads+totals first per the frozen NCAAF plan)
- Regions: `us,eu` (eu carries Pinnacle)
- Cadence: 3 snapshots/week — Wed 12:00 UTC, Fri 20:00 UTC, Sat 15:00 UTC

## Volumes

- Planned snapshots: 180; received: 180/180 (45 per season)
- Normalized quotes: 686,578 paired spread/total quotes
- Pinnacle present in all 180 snapshots
- Snapshot-inventory fingerprint: `cf2f45bcfbc52ccfc2e6cacbdd9c069f`

## Integrity audit (PASS)

- Audit workflow run: `34989467267` (audit-dataset, workflow_dispatch,
  sport=ncaaf, seasons=2022,2023,2024,2025, weeks=0-14)
- Result: **PASS** — zero errors
- Findings: zero duplicate IDs, zero malformed prices, zero null lines,
  zero impossible fair probabilities, zero mis-paired groups.
- Coverage warnings (documented, not integrity failures): `espnbet` returned
  no quotes in any season (provider did not return it).

## Idempotency verification (PASS)

- Verify workflow run: `34989240353` (verify-backfill, workflow_dispatch,
  sport=ncaaf, since=2025-01-01)
- Result: **PASS**
- Fingerprint workflow run: `34989892600` (fingerprint-dataset, sport=ncaaf)

## Incident during acquisition (resolved)

- 2026-09-15: Neon free-tier `DiskFull` (512 MB) interrupted a quote-insert
  loop mid-snapshot, leaving a partial quote pair for the 2025-10-10 snapshot.
  The resume logic saw some rows and skipped the snapshot.
- Fixed: per-snapshot single DB transaction + auditable `--repair-snapshot-at`
  (commit `2a77d34c9e9c9f1349b9a4ce85de10ba90c7ad9b`).
- The incomplete snapshot was deleted and re-fetched cleanly
  (4,642 quotes, 24 books, ~40 credits — the normal single-snapshot cost).
- Neon upgraded to Launch; quota remaining after repair: 4,632 credits.

## Governance

- Audit E is now satisfied: the 2022–2025 dataset is fingerprinted and frozen.
- All results remain SHADOW. Historical results cannot promote anything.
- No NCAAF modeling may use seasons outside 2022–2025; the 2026 season is
  prospective shadow evidence only.
- FBS-vs-FBS must be tagged separately from FBS-vs-FCS; never pool casually.
- This freeze record pins the dataset for NCAAF baseline research;
  no retrospective changes to the frozen set are permitted.
