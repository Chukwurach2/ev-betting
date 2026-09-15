# H1 Findings: Inter-Window Market-Movement Prediction

**Run:** 35006016755. **Artifact:** 10411956939.
**Prereg:** `docs/preregistrations/ncaaf-h1-movement.md`.
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**2025:** sealed, not touched. **API credits:** zero (read-only DB + fixtures).

## Result: H1 FAILS Stage 1. Hypothesis abandoned on 2022–24.

Inter-window consensus movement is **not predictable** from window-t
information (line level, cross-book dispersion/range, Pinnacle deviation,
book count).

## Primary test (spread, next-window)

| Metric | Value |
|---|---|
| Pooled OOS R² | **−0.011** |
| Mean fold R² | −0.0209 |
| Folds | 27 (rolling week-level origins) |
| OOS predictions | 4,743 |
| By season | 2022: −0.0133, 2023: −0.0057, 2024: −0.0196 |
| Directional accuracy | 0.242 |
| Permutation p (499, within-week shuffle) | **0.986** |

The OLS model performs **worse than the martingale null** (predict zero
movement) out-of-sample. The permutation p-value of 0.986 means shuffled
movement yields better R² 98.6% of the time — the features carry no signal.

## Secondary targets (all negative OOS R²)

| Target | Pooled OOS R² | By season |
|---|---|---|
| Total, next-window | −0.0059 | −0.0259 / −0.0007 / −0.0097 |
| Spread, to-close | −0.0199 | −0.0222 / −0.0309 / −0.0206 |
| Total, to-close | −0.0241 | −0.0442 / −0.0054 / −0.0289 |

Failure is consistent across markets, targets, and seasons. This is not a
one-season artifact.

## Stage-1 gate assessment

1. **Statistically credible** (perm p < 0.05): ❌ p = 0.986.
2. **Directionally stable** (positive OOS R² ≥2/3 seasons): ❌ all negative.
3. **Positive OOS improvement** (mean OOS R² > 0): ❌ −0.011.
4. **Economically signed**: ❌ directional accuracy 0.242 < 0.5.

## Interpretation

The NCAAF spread/total market is **efficient with respect to inter-window
movement**. Wednesday's cross-book structure (dispersion, Pinnacle
deviation, line level) contains no exploitable information about
Friday/Saturday movement. Movement is consistent with unpredictable
information arrival — the martingale null stands.

This extends Phase F's finding (Pinnacle's static level ≈ consensus) to
dynamics: neither levels nor changes in the observable window-t feature
set predict subsequent movement.

## Governance consequences

- **H1 is burned on 2022–24.** No H1b/H1c feature tinkering, no new
  targets, no window redefinitions. Per the formalized rules, a materially
  revised movement hypothesis becomes a **new family** requiring new
  confirmation data.
- H1 does not advance to Stage 2 (no executable economics to test).
- H1 does not consume 2025. The 2025 confirmation set remains sealed.
- **Mechanism analysis** on this result is allowed for designing future
  hypotheses (e.g., "what information DOES move lines?") but cannot claim
  confirmatory evidence from 2022–24.

## Data quality notes

- n_rows = 11,306 (FBSvFBS, consecutive windows present).
- n_not_fbs = 1,074 excluded (universe = FBSvFBS).
- n_outlier = 203 excluded (F1 cleaning rule: range > 10 spread / > 15 total).
- n_no_sign = 0 (CFBD oracle covered all spread rows in H1 universe).

---
*H1 failed cleanly. The null is a first-class result. Next: H3.*
