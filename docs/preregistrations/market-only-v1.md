# Preregistration: market-only-v1

Date: 2026-09-12. Status: committed BEFORE any real-data execution.
Role: SHADOW research only. A positive result selects the model as a
FORWARD-SHADOW candidate only; historical results never promote anything.
The promotion gate remains positive CLV against actual closing lines on
forward data.

## Hypothesis

Early-week market prices predict the closing consensus no-vig fair
probability better than naive carry-forward. If true, the model's predicted
close is a better fair-value estimate than the currently available price,
which is the necessary (not sufficient) condition for a CLV-capturing
shadow strategy.

## Data

Frozen historical dataset, fingerprint `43f853a44bf93937d85149ca5fd7241b`
(162 snapshots, 291,586 quotes). Canonical game identity from the
event-identity audit: (home_team, away_team) + 7-day kickoff clustering.
Zero API credits. Multiplicative de-vig (tournament winner) for all fair
probabilities. All snapshots used are pre-kickoff; the close is an ex-post
benchmark only, never a feature.

## Unit of analysis

(game, market) for market in {FULL_GAME_SPREAD, FULL_GAME_TOTAL}.

Snapshot slots per game (kickoff K), all strictly pre-kickoff:
- close = latest snapshot with observed_at < K. Require K - close <= 72h,
  else the game is excluded (no valid close).
- sat = latest snapshot with observed_at <= close - 24h. Required.
- wed = latest snapshot with observed_at <= close - 96h. Required.

Analysis line L: modal line across books at the close snapshot (ties broken
deterministically: closest to the cross-book median line, then smallest).
At each of wed/sat/close, the market consensus f is the median fair prob
across books quoting exactly L (multiplicative de-vig per book). Require
>= 2 books at L in EACH of the three snapshots (complete-case); otherwise
the unit is excluded. No target book is evaluated, so no LOBO is needed:
the consensus is simply the market's.

Season label for validation: season = kickoff.year, except kickoff in
January -> season = kickoff.year - 1 (playoffs belong to the prior season's
campaign; the 9 Wild Card listings count as 2024).

## Features (wed/sat only — never the close)

1. f_wed — Wed consensus fair prob at L
2. f_sat — Sat consensus fair prob at L
3. move = f_sat - f_wed (early-week momentum)
4. pinn_dev — Pinnacle fair prob at L on Wed minus f_wed (0 if Pinnacle
   absent that snapshot)
5. disp_wed — cross-book std of fair probs at L on Wed
6. n_books_wed — number of books at L on Wed
7. is_total — 1 for FULL_GAME_TOTAL, 0 for FULL_GAME_SPREAD

Target: f_close.

## Model

Ridge regression, alpha = 1.0 (fixed, no tuning), features standardized
using training-fold mean/std. Intercept fit. One model, pooled across
markets (is_total carries market differences).

## Baselines

- B_wed: predict f_close = f_wed (early carry-forward)
- B_sat: predict f_close = f_sat (late carry-forward; the stronger baseline)

## Validation

Leave-one-season-out over {2022, 2023, 2024}: train on two seasons,
predict the held-out season (3 folds). NO random k-fold across time.

## Primary inference

One-sided paired t-test (Diebold-Mariano form, quadratic loss) on
(SE_model - SE_Bsat) pooled over all held-out units, H1: mean < 0,
alpha = 0.05. This is the single confirmatory test: no multiple-testing
correction is needed. A null simulation (martingale fair-prob evolution,
no predictability) MUST show the test rejects at ~nominal rate before
real-data execution; if miscalibrated, amend pre-results per Amendment A1
template.

## Decision rule

"market-only-v1 advances to forward-shadow candidacy" iff ALL of:
(a) primary test rejects at alpha = 0.05 (one-sided);
(b) RMSE_model <= RMSE_Bsat - 0.003 (practical bar: 0.3 probability points);
(c) RMSE_model < RMSE_Bsat in at least 2 of the 3 held-out seasons.
Otherwise the verdict is "no_edge". Fewer than 200 complete units total
-> "infeasible".

## Secondary / exploratory (non-decisive, reported for context)

- RMSE vs B_wed; per-season RMSE table for model/B_wed/B_sat.
- Calibration: mean predicted vs mean actual f_close by predicted decile.
- Exploratory CLV sketch (labeled exploratory): at sat, hypothetical
  position toward predicted close when |pred - f_sat| >= 0.01; score
  sign(pred - f_sat) * (f_close - f_sat). Descriptive only.

## What a positive result does NOT do

It does not create picks, does not change the picks engine, and does not
qualify any strategy. It only authorizes building a forward-shadow
evaluation harness for the model. "No edge found" is an expected and
acceptable outcome.
