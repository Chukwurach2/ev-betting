# H1: Inter-Window Market-Movement Prediction (Preregistration)

**Family:** H1. **Date:** 2026-09-15 (revised). **Stage:** 1 (predict movement).
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**2025:** sealed, not used.

## Hypothesis
Information available at window t predicts the direction and magnitude of
consensus line movement at window t+1. Pinnacle-relative features may be
candidate predictors; H1 makes **no causal or leadership claim** about Pinnacle
(H2, dormant, would test that with adequate data).

## Target hierarchy (preregistered — one family)
- **Primary:** Next-window consensus movement.
  - Early (Wed 12:00 UTC) → Mid (Fri 20:00 UTC)
  - Mid (Fri 20:00 UTC) → Late (Sat 15:00 UTC)
- **Secondary:** Movement-to-close (Early → Late).

We do not optimize across targets/windows after seeing results. The primary
test uses next-window movement pooled across both transitions. The secondary
is reported, not optimized.

## Universe
FBS-vs-FBS games, 2022–2024, with the relevant consecutive windows present.
Spread and total markets separately.

## Features (window-t only, point-in-time)
1. `line_t` — consensus line at window t (signed via CFBD oracle for spreads).
2. `dispersion_t` — cross-book std of lines at window t.
3. `range_t` — max-min line across books at window t.
4. `pinnacle_dev_t` — Pinnacle line minus consensus at window t (0 if absent).
5. `n_books_t` — number of books quoting at window t.

No future information. No scores. No 2025.

## Target
`signed_movement = consensus_{t+1} − consensus_t`
- Spreads: signed from home-team perspective (CFBD oracle).
- Totals: natural points.

## Transformations / outlier handling
- Preregistered F1 cleaning rule: exclude events where window-t cross-book
  range > 10 (spread) or > 15 (total).
- Winsorize movement at ±7 (spread) / ±14 (total) for regression.

## Model (Stage 1)
**Primary:** OLS: `movement ~ line_t + dispersion_t + range_t + pinnacle_dev_t + n_books_t`.
**Null:** Martingale — E[movement | t-info] = 0. (R² = 0.)

## Validation: rolling week-level origins
- Train on weeks 1..k, predict weeks k+1..k+2 (or remaining), for k = 4..13.
- Advance origin weekly. **No future-season/week info in features.**
- Report out-of-sample R² and directional accuracy per fold.
- **Clustered uncertainty:** Games within the same week are not independent.
  Report week-clustered standard errors (cluster by week).
- **Season stability:** Report 2022, 2023, 2024 separately. More folds ≠ more
  independent seasons; a single-season driver invalidates.

## Statistical test (primary)
**H1a:** Out-of-sample R² > 0 (one-sided). Permutation null (shuffle movement
within week, recompute OOS R²). **This is H1's single primary test.**

## Stage-1 gate (revised — no arbitrary magnitude knockout)
Advance to Stage 2 IFF ALL hold:
1. **Statistically credible:** Permutation p < 0.05 (Holm-adjusted across H1/H3/H4/H5).
2. **Directionally stable:** Positive OOS R² in ≥2 of 3 seasons, and pooled
   coefficient signs stable across seasons.
3. **Positive OOS improvement:** Mean OOS R² > 0 (not just in-sample).
4. **Economically signed:** Predicted direction aligns with profitable
   early-betting direction (sign check, not magnitude threshold).

**Report** effect magnitude with uncertainty (week-clustered CIs). Do NOT
knock out on an arbitrary point threshold.

## Stage 2 (only if Stage 1 survives)
Executable economics: simulate betting at window t in predicted direction.
- **CLV:** Do early bets beat the t+1 consensus?
- **Viability bar:** Positive mean CLV after accounting for typical spread
  (-110) and a 0.5-point execution slippage assumption.
- If viable → H1-finalist for 2025 confirmation.

## Failure (abandon H1 if)
- Primary permutation p > 0.05 (adjusted), OR
- OOS R² ≤ 0 in ≥2 seasons, OR
- Coefficient signs flip across seasons (unstable).

If H1 fails, inter-window movement is unpredictable from window-t info.
We do not try new features, targets, or windows. H1 is done.

---
*Stage 1 predicts movement. Stage 2 (only on survival) tests executability.
One family, one primary test.*
