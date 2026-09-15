# NCAAF F3 Findings — Market Baseline Completion (2022–2024)

**Run:** 35004930208 (2026-09-15). **Status:** success.
**Prereg:** `docs/preregistrations/ncaaf-market-baseline-f3.md`.
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`. **Sign:** CFBD oracle (A4).
**Integrity:** freeze 135/135. Spread rows excluded (no CFBD sign): 67.

## 1. Favorite/underdog spread covers

| | Cover rate | n |
|---|---|---|
| Home favorite | 48.8% | 3,984 |
| Home underdog | 52.1% | 2,083 |

By season (fav / dog):
- 2022: 48.4% (n=1,410) / 52.5% (n=810)
- 2023: 48.1% (n=1,180) / 51.2% (n=645)
- 2024: 49.7% (n=1,394) / 52.6% (n=628)

**The underdog covers more often in both roles.** When home is favored,
the away dog covers 51.2%; when home is the dog, home covers 52.1%.
The 3.3pp gap is consistent across all three seasons (~2.4σ).

This was a preregistered split (null: both ~50%). It rejects the null.
It is a **descriptive baseline finding**, not a betting edge — no threshold,
no execution, no ROI claim. Candidate mechanism for H: market overprices
favorites (public favorite bias), or underdog motivation/effort asymmetry.

## 2. Totals calibration (fine bins)

| P(Over) bin | Mean p | Realized | n |
|---|---|---|---|
| [0.35,0.45) | 0.427 | 100% | 2 |
| [0.45,0.50) | 0.496 | 41.9% | 215 |
| [0.50,0.55) | 0.500 | 51.5% | 5,769 |
| [0.55,0.65) | — | — | 0 |

The bulk (n=5,769) is well-calibrated. The [0.45,0.50) bin underperforms
by 7.7pp (n=215, ~2.3σ) — a weak lead for H, not a finding. The market
almost never posts P(Over) outside [0.45,0.55).

## 3. Baseline edge distribution (|p − 0.5|)

| Market | n | Mean | p90 | p99 | >2.4pp* | >5pp | >10pp |
|---|---|---|---|---|---|---|---|
| Spread | 6,264 | 0.11pp | 0.43pp | 1.08pp | 0.06% | 0.05% | 0.02% |
| Total | 6,093 | 0.04pp | 0.00pp | 0.90pp | 0.08% | 0.03% | 0.00% |

\* 2.4pp = breakeven edge at -110.

**The market's self-implied edge is essentially zero.** Fewer than 1 in
1,000 quotes imply even breakeven. A challenger cannot win by "finding
mispriced quotes" — the market doesn't misprice at the quote level. It
must find a **signal the market is missing** (information the consensus
hasn't incorporated).

## What F3 establishes (the H null)

1. The spread market is efficient at the quote level (|p−0.5| ≈ 0).
2. Home cover is ~50.5% overall, but **underdogs cover ~52%** in both roles.
3. Totals are calibrated where the market actually quotes (p ∈ [0.45,0.55]).
4. Any H challenger must beat: (a) the ~50.5% home-cover base rate, and
   (b) the ~52% underdog-cover base rate, with a preregistered mechanism,
   on uninspected evidence (2025 sealed).

## Leads for H (not findings)
- Underdog cover lean (48.8% fav / 52.1% dog, preregistered split).
- FBSvFCS home 55.6% (discovered in F2, registered separately).
- Totals [0.45,0.50) underperformance (41.9%, n=215).

---
*F3 completes Phase F market baseline. Phase H challenger tournament
preregistrations may now be written against this null.*
