# H3: Football-Information Residual Model (Preregistration)

**Family:** H3. **Date:** 2026-09-15 (revised). **Stage:** 1 (predict residual).
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**2025:** sealed, not used.

## Hypothesis
Opponent-adjusted team strength predicts the **residual** — the component
of game outcomes that the market line does not explain. We are not building
a football model; we are testing whether football information adds anything
**beyond the market**.

## Elo mechanics (FROZEN before evaluation)

**Rating system:** Offensive/defensive Elo (existing `challenger/elo.py`).
- `off[t]`, `deff[t]`: points above/below average (zero-centered).
- Update: `update_ratings(off, deff, home, away, hs, aws, k=0.15, hfa=1.2)`.
- Prediction: `expected_scores` → `pred_margin = exp_h − exp_a`,
  `pred_total = exp_h + exp_a`.

**These mechanics are frozen.** k, hfa, league_avg, update rule, and
recentering are fixed constants, not tuned.

**2019–21 burn-in (initialization only):**
- 2019–2021 games are used SOLELY to initialize entering-2022 ratings.
- 2019–21 are NOT used to: select k/hfa, choose the model family, define
  features, set thresholds, or judge whether H3 works.
- Ratings enter 2022-01-01 with three seasons of history instead of zero.
- **Sensitivity:** Re-run H3 with (a) zero-initialized 2022 ratings and
  (b) 2020–21 only burn-in. Conclusions must not depend on initialization.

**Point-in-time:** Elo for a game uses only games completed before that
game's date. Never future information.

## Universe
FBS-vs-FBS games, 2022–2024. Spread and total models separately.

## Features
1. `market_line` — consensus spread (signed, CFBD oracle) or total.
2. `elo_edge` — `pred_margin − (−market_spread_signed)` (spread) or
   `pred_total − market_total` (total). The Elo-implied deviation from market.

Using `elo_edge` (not raw `elo_diff`) directly tests incremental information:
if the market already prices Elo, `elo_edge` has no residual predictive power.

## Target (the residual)
- **Spread:** `residual = home_margin − (−market_spread_signed)`.
- **Total:** `residual = total_points − market_total`.

## Model (Stage 1)
**Primary:** OLS: `residual ~ elo_edge` (spread). One coefficient.
**Secondary:** OLS: `residual ~ elo_edge` (total).
**Null:** Elo adds nothing — coefficient on `elo_edge` = 0.

## Validation: rolling week-level origins
Same as H1: train on weeks 1..k, predict weeks k+1..k+2, k=4..12.
Week-clustered SEs. Report 2022/2023/2024 stability separately.
Elo ratings are strictly point-in-time within each fold.

## Statistical test (primary)
t-test on the `elo_edge` coefficient in the spread model (two-sided),
with week-clustered SEs. **This is H3's single primary test.**

## Stage-1 gate (revised — no arbitrary magnitude knockout)
Advance to Stage 2 IFF ALL hold:
1. **Statistically credible:** Clustered t-test p < 0.05 (Holm-adjusted
   across H1/H3/H4/H5).
2. **Directionally stable:** Positive coefficient in ≥2 of 3 seasons, and
   positive OOS R² in ≥2 of 3 seasons.
3. **Positive OOS improvement:** Mean OOS R² > 0 vs market-only null.
4. **Economically signed:** Coefficient sign implies betting WITH the
   Elo edge (not against it).

**Report** effect magnitude with uncertainty. No arbitrary knockout.

## Stage 2 (only if Stage 1 survives)
Executable economics: bet with Elo edge at window-t prices.
- **CLV:** Do Elo-edge bets beat the closing line?
- **Viability:** Positive mean CLV after -110 and 0.5pt slippage.
- If viable → H3-finalist for 2025 confirmation.

## Failure (abandon H3 if)
- Primary t-test p (adjusted) > 0.05, OR
- OOS R² ≤ 0 in ≥2 seasons, OR
- Coefficient sign flips across seasons.

If H3 fails, opponent-adjusted Elo adds nothing beyond the market. We do
not try SRS, FPI, or other ratings. H3 is done.

---
*Stage 1 only. 2019–21 = burn-in, not research data. Elo mechanics frozen.*
