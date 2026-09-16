# H-N6 retirement — 2026-09-16

Prereg: `docs/preregistrations/nfl-h-n6-officiating-crew-totals-frozen.md`
(frozen 2026-09-16, unchanged).

## Verdict: INFEASIBLE — RETIRED

Coverage gate run 35045693211 (read-only, zero API credits, no DB writes):

- **Eligible N = 365** (prereg threshold: 700)
- Freeze check on frozen scope (spread+total): 291,586 quotes, 162/162
  snapshots — OK. 47,080 moneyline rows confirmed out-of-scope additive
  backfill (see `docs/nfl-freeze-addendum-2026-09-16.md`).
- Snapshot slots matched 162/162; officials anomalies: none.

## Attrition (the binding constraint is the decision snapshot)

| Season | Games | Identity-matched | Wed consensus | Sat | Sun | Crew | Eligible |
|---|---|---|---|---|---|---|---|
| 2022 | 271 | 271 | 104 | 117 | 137 | 271 | 104 |
| 2023 | 272 | 272 | 141 | 109 | 118 | 272 | 141 |
| 2024 | 272 | 256 | 120 | 106 | 125 | 256 | 120 |

- 273 games have totals consensus at Sat/Sun fallback slots but not at
  the Wed 12:00 UTC decision snapshot → counted as missingness per the
  frozen prereg, never as coverage.
- 161 games have no eligible consensus at any of the three slots.
- 16 games (all 2024) have no Odds event in the frozen dataset.
- Even the prereg-forbidden relaxation (fallbacks as coverage: 638) and
  the unauthorized missing-data pull (161 games → 1,610 credits: 526)
  both fall short of 700. The verdict is overdetermined.

## What was NOT done

- No outcomes inspected (realized totals never read).
- No timestamps widened, no threshold lowered, no crew definitions
  altered, no data purchased — per the frozen retirement rule.
- $0 API credits spent on H-N6 beyond the frozen dataset itself.

## Status

H-N6 is retired under its frozen specification. The prereg, the freeze
addendum, and this note are the complete record. The 17-referee
clustered design and the officiating-crew hypothesis family are not
pursued further under this specification.
