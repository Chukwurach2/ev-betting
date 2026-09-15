# H5 2025 Confirmation: Results (RECORDED)

**Date:** 2026-09-15.
**Prereg:** `ncaaf-h5-confirmation-2025.md` (SHA256: cfb5fa96e71de0dd)
**Status:** RECORDED. No further 2025 queries for H5.

## Data retrieved (claim-scoped)

- 11 Wednesday snapshots (2025-08-20 through 2025-11-19)
- Markets: spreads only. Regions: us only.
- Source: The Odds API historical (production 20K key)
- Workflow run: 35007775879. Artifact: h5-2025-snapshots.
- Credits: ~110.

**Firewall compliance:** Only FBS-vs-FCS games were examined. No 2025
small spreads, totals, Elo residuals, movement, or other windows were
inspected. H4-small was not touched.

## Coverage (critical finding)

| Category | Count | % |
|----------|-------|---|
| CFBD 2025 FBS-vs-FCS completed games | 126 | 100% |
| Found in any Wednesday snapshot | 53 | 42% |
| With 3+ books (meets consensus threshold) | 12 | 9.5% |
| With <3 books (excluded) | 41 | 33% |
| Not in any snapshot (excluded) | 73 | 58% |

**73 games** had no Wednesday odds from any book. **41 games** had
1-2 books (below the 3-book minimum). Only **12 games** qualified.

## Test results (n=12)

- Decisive games: 12
- Covers: 7 (58.3%)
- Pushes: 0
- Wilson 95% CI: [31.95%, 80.67%]
- Breakeven: 52.38%

## Decision (frozen rule)

**INCONCLUSIVE.**

n=12 < 100. Per the frozen three-way rule, this is INCONCLUSIVE.

This is a **feasibility inconclusive**, not a statistical inconclusive.
The test could not be conducted as specified because Wednesday
early-window FCS coverage is insufficient. The 95% CI is wide
([32%, 81%]) and includes breakeven, but with n=12 the interval is
not informative.

## Interpretation

- H5 is **not refuted**. The data does not establish U < 52.38%.
- H5 is **not confirmed**. The data does not establish L > 52.38%.
- H5 **remains an unresolved candidate** in the hypothesis registry.

## Lessons

1. **Feasibility must be checked before freezing.** The prereg assumed
   126 games would have Wednesday consensus data. Only 12 did. A
   coverage check on 2022-24 (or a small 2025 pilot) should have been
   done before committing to the Wednesday window.

2. **Wednesday FCS coverage is sparse.** Books price FCS games closer
   to kickoff (Fri/Sat), not early in the week. The 2022-24 discovery's
   942 observations likely came from later windows.

3. **The 110 credits bought a lesson, not a test.** This is the cost
   of the feasibility oversight.

## What happens next (per frozen prereg)

- H5 waits for **genuinely new prospective observations** (2026 shadow
  or later) with adequate coverage.
- **2025 is not re-examined** for H5. No alternative windows, no
  threshold changes, no "exploring why."
- If a future H5 test is designed, it must use a window with
  demonstrated coverage (not assumed coverage).

## 2025 firewall status

**RE-SEALED.** The H5 claim has been tested (to the extent feasible).
No further 2025 queries are authorized for any hypothesis. The next
2025 access requires a new frozen prereg and explicit user approval.

---
*Recorded 2026-09-15. One shot taken. Result: INCONCLUSIVE (n=12).*
*2025 re-sealed.*
