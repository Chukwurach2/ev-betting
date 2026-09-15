# NCAAF F2 Findings — Market vs Outcomes (2022–2024)

**Run:** 35004257658 (2026-09-15). **Status:** success.
**Prereg:** `docs/preregistrations/ncaaf-market-outcomes-f2.md` (+ Amendments A1–A4).

## Integrity
- Freeze gate: 135/135 snapshots, PASS.
- Matched: 2,456/2,470 events (99.43%). Unmatched: 9 team_mislabel, 4 kickoff_miss, 1 not_completed.
- Played event-windows: 12,053. Pushes: 187.
- Spread rows excluded (no CFBD line for sign): 67 (1.1%).
- Zero Odds API credits consumed.

## Spreads — home cover rate
| Split | Rate | n |
|---|---|---|
| Overall | 50.5% | 12,053* |
| [0,3) | 45.8% | 810 |
| [3,7) | 51.3% | 1,418 |
| [7,14) | 50.2% | 1,664 |
| [14,inf) | 50.3% | 2,175 |
| 2022 / 2023 / 2024 | 50.2% / 50.2% / 51.3% | 4,373 / 3,577 / 4,103 |
| early / mid / late | 50.5% / 50.6% / 50.6% | 3,882 / 3,901 / 4,270 |
| FBSvFBS | 50.2% | 10,677 |
| FBSvFCS | **55.6%** | 942 |
| P4 / G5/other | 51.5% / 49.8% | 6,170 / 5,449 |

\* Overall mixes spreads and totals; see market-specific rows.

**Calibration:** spread home-cover p clusters at ~0.50 (de-vigged); realized
rate 49.8–52.4% across bins — well calibrated, no systematic mispricing.

**Closing efficiency (1,705 paired events):** early-window consensus predicts
the cover at 50.3%, late-window at 50.0%. The closing line predicts no better
than the early line — no beat-the-close edge in direction.

## Totals — over rate
| Split | Rate | n |
|---|---|---|
| [0,45) | 51.8% | 795 |
| [45,55) | 52.4% | 2,768 |
| [55,65) | 50.4% | 2,015 |
| [65,inf) | 46.1% | 408 |
| Closing: early 50.2% → late 51.3% (1,664 paired) | | |

## Interpretation
1. **The NCAAF spread market is efficient at the home-cover level.**
   No bucket, season, window, or conference deviates meaningfully from 50%.
   The market's de-vigged probabilities are well calibrated.
2. **No timing edge.** Early lines predict covers as well as late lines.
3. **Lead for H:** FBSvFCS home cover 55.6% (n=942, ~3.5σ). May reflect
   favorite-backing in mismatches rather than a home edge; needs a
   preregistered H challenger with multiple-testing protection before any
   claim. Do NOT treat as an edge.
4. **Totals:** broadly efficient; the [65,inf) under-lean (46.1%, n=408) is
   a weak lead for H, not a finding.

## What F2 does NOT show
- No evidence of a home-field bias in spreads (50.5% overall).
- No evidence that Pinnacle or any window leads the market in *direction*
  (F1 already showed no level-lead; F2 shows no outcome-prediction lead).
- The 55.6% FBSvFCS number is a hypothesis generator, not an edge.

---
*Supersedes the spread sections of runs 35002977504 (A2 code, unsigned-line
bug) and 35003460992 (A3 code, invalid p-oracle). Totals sections of those
runs were valid and are consistent with this run.*
