# NFL Edge historical dataset — freeze record

**Status: FROZEN — SHADOW (research only; no model promotion, no production picks)**

Generated: 2026-09-12 ~12:45 UTC
Repo commit: `a71996901c40bb8e140c2696ea89eaf5e63f57e9` (main, Chukwurach2/ev-betting)
Dataset fingerprint: `43f853a44bf93937d85149ca5fd7241b`

## Scope

- Seasons: 2022, 2023, 2024
- Weeks: 1–18
- Markets: spreads, totals
- Regions: `us,eu` (eu carries Pinnacle)
- Books present in quotes: pinnacle, draftkings, fanduel, betmgm, betrivers, williamhill_us
- Books absent from provider responses (documented coverage warnings, not integrity failures): fanatics, espnbet
- Cadence: 3 snapshots/week — Wed 12:00 UTC, Sat 12:00 UTC, Sun 15:30 UTC

## Volumes

- Planned snapshots: 162; received: 162/162
- Normalized quotes: 291,586 paired spread/total quotes
- Pinnacle present in all 162 snapshots

## Integrity audit (PASS)

- Audit workflow run: `34692341564` (audit-dataset, workflow_dispatch)
- Result: **PASS**
- Findings: zero duplicate IDs, zero malformed prices, zero null lines,
  zero impossible fair probabilities, zero mis-paired groups.
- 270-row persistence discrepancy seen during backfill did not produce stored corruption.
- Note: the audit workflow previously masked its exit code (`|| echo "audit exited $?"`);
  fixed in commit `a71996901c40` so a failing audit now fails the workflow run.

## Idempotency verification (PASS)

- Backfill workflow run: `34694240545` ("NFL Edge historical odds backfill", workflow_dispatch)
- Inputs: seasons=2022, weeks=1-9, regions=us,eu, recapture=false (already-complete range)
- Result: `plan: 27 snapshots (1 seasons x 9 weeks x 3)` →
  `done: snapshots=0 quotes=0 skipped=27 credits_spent~0`
- Zero new snapshots, zero new quotes, zero API credits spent — rerun is a pure no-op.

## Governance

- All results remain SHADOW. Historical results cannot promote anything.
- Positive forward-shadow performance against actual closing lines is required
  before any model, strategy, or pick logic advances.
- This freeze record pins the dataset for the preregistered de-vig/calibration
  tournament; no retrospective changes to the frozen set are permitted.
