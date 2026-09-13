# NCAAF Phase-C Probe Receipt — FROZEN 2026-09-13

Infrastructure validation only. Not research. No modeling may use this data
until the phase-E audit passes and the dataset is fingerprinted/frozen.

## Probe parameters

- Workflow: `backfill-history.yml`, dispatched 2026-09-13T22:09Z
- Inputs: `sport=ncaaf`, `seasons=2024`, `weeks=8`
- Plan: 3 snapshots (2024-10-16T12:00Z, 2024-10-18T20:00Z, 2024-10-19T15:00Z)
- Markets: spreads,totals; regions: us,eu (defaults)
- Run: https://github.com/Chukwurach2/ev-betting/actions/runs/34785926681

## Results

| Requested (UTC)       | Provider ts (UTC)     | Drift | Quotes | Books | Events w/quotes | Credits |
|-----------------------|-----------------------|-------|--------|-------|-----------------|---------|
| 2024-10-16T12:00:00   | 2024-10-16 11:55:39   | 261s  | 3,214  | 16    | 73              | 40      |
| 2024-10-18T20:00:00   | 2024-10-18 19:55:39   | 261s  | 2,998  | 16    | 78              | 40      |
| 2024-10-19T15:00:00   | 2024-10-19 14:55:39   | 261s  | 2,776  | 16    | 72              | 40      |

## Checklist (per 2026-09-13 gate definition)

- [x] Correct event mapping: 73/78/72 events with quotes; real 2024 week-8 games
- [x] All three intended timestamps captured
- [x] Paired spreads/totals: verifier pairing gate passed (every
      event/book/market/line group has exactly 2 quotes, fair probs sum to 1)
- [x] Book identities: 16 books/snapshot, incl. draftkings, fanduel, betmgm,
      betrivers, williamhill_us — and **pinnacle** (see note)
- [x] Signed lines: validated via pairing + fair-probability gates
- [x] Provider/collector timestamps: 261s drift (tolerance 1h); dual-timestamp
      schema live (collector capture time = window membership, provider
      observed_at = freshness)
- [ ] Settlement mapping: by design in phase F — quotes carry canonical
      market/selection/line for the outcomes ledger
- [x] Credit reconciliation: 3 x 40 = 120 used; remaining 12,100 -> 11,980
- [x] Idempotent rerun: 2026-09-13T22:26Z rerun skipped 3/3, ~0 credits
      (run 34786788967)

## Verification

- `verify-backfill.yml` sport=ncaaf (run 34786359174): **pass**
- `verify-backfill.yml` sport=nfl (run 34786357662): **pass** (NFL untouched)

## Notable finding

Pinnacle IS present in historical NCAAF snapshots via the eu region, even
though it is absent from the live us-region NCAAF feed. The contract's
"observable-book consensus; Pinnacle not required" stands, but the historical
dataset will carry a Pinnacle signal column for research. No action taken;
recorded for phase F/G design.

## Table isolation

All rows landed in `ncaaf_edge_market_history` / `ncaaf_edge_historical_quotes`
only. No `nfl_edge_*` writes. Migrations 013/014/015 applied cleanly.
