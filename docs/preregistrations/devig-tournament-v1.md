# Preregistration: de-vig / calibration tournament v1

**Status: PREREGISTERED — no results viewed. Analysis code does not yet exist.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Dataset (frozen): `docs/dataset-freeze.md`
- Dataset fingerprint: `43f853a44bf93937d85149ca5fd7241b`
- Scope: seasons 2022–2024, weeks 1–18, markets FULL_GAME_SPREAD and
  FULL_GAME_TOTAL, regions us,eu — 162/162 snapshots, 291,586 paired quotes.
- Question: which de-vig method best estimates fair (no-vig) probabilities
  on NFL full-game two-sided markets?

## Candidate methods (exactly three)

Applied to a two-sided implied-probability pair (p1, p2) with
overround R = p1 + p2 > 1, returning fair probabilities (q1, q2):

1. **multiplicative** (baseline, industry standard): q_i = p_i / R.
   Included because it is the default assumption embedded in the existing
   pipeline (`fair_probability` column, `novig_prob` in market_features.py);
   the tournament tests whether alternatives beat the status quo.

2. **additive**: q_i = p_i − (R − 1) / 2.
   Included because it allocates the margin equally in probability space
   rather than proportionally — the natural alternative margin-allocation
   hypothesis.

3. **power** (Lopez): q_i = p_i^k / (p_1^k + p_2^k), where k is solved
   per market from the normalization constraint p_1^k + p_2^k = 1
   (unique root k > 1 when R > 1; bisection on k in (1e-6, 100]).
   Included because it is the standard favorite-longshot-bias correction:
   it reallocates vig share away from the longshot relative to
   multiplicative, without fitting any parameter to outcomes.

Deliberately excluded: **Shin**. On two-outcome markets Shin's method is
algebraically identical to additive (verified numerically to ~1e-12 in the
unit test suite against the published Shin formula); including it would
double-count one method in the multiple-testing budget. The set is kept
small (3) to preserve power under family-wise error control.

Validity guard (all methods): if a method returns q_i ∉ (0, 1) for a pair,
that cell is excluded for ALL methods (complete-case rule, see below).
The stored `fair_probability` column is NOT used; all three methods are
recomputed from raw `american_odds`.

## Evaluation protocol (LOBO)

For each event (provider_event_id) and market:

- Snapshots for the event = distinct `observed_at` values among its quotes,
  sorted ascending. **Closing snapshot** = the latest one (all snapshots are
  pre-kickoff per the frozen audit). All earlier snapshots are
  **prediction snapshots**.
- Reference selection: spreads → the home-team side
  (`selection = home_team`); totals → `Over`.
- For each prediction snapshot s and each book b with a two-sided pair at s:
  take b's pair at its modal line within s (ties → line closest to the
  cross-book median line at s; deterministic). Compute each method's fair
  probability f for the reference selection from b's own pair.
- **Closing consensus** (per method M, per held-out book b): median of M's
  fair probabilities for the reference selection across books b' ≠ b with
  a valid pair at the closing snapshot. **The held-out book never
  contributes to the consensus used to evaluate it.** Require ≥ 2 other
  books, else the cell is excluded.
- A cell = (event, market, prediction snapshot s, held-out book b) is
  included only if all three methods yield valid probabilities for b's
  pair at s AND for every consensus-contributing pair at the close
  (complete-case across methods, so methods are compared on identical cells).

Point-in-time safety: the prediction f uses only quotes with
`observed_at` equal to s (the snapshot's own quotes). The closing
consensus uses only closing-snapshot quotes; it is the ex-post benchmark
(the standard CLV target), never an input to f. No future information
enters any prediction.

## Metrics

### Primary metric (ideal) — DECLARED UNTESTABLE THIS ROUND

**Mean log-loss of the LOBO-consensus fair probability against the
settled game outcome** (reference selection: 1 if it covered/went over,
0 otherwise; pushes excluded).

Justification for the choice: log-loss is the strictly proper scoring
rule for probability estimation — it is minimized only by the true
probabilities and penalizes confident errors superlinearly, which is the
correct objective when the downstream use is sizing wagers off fair
probabilities. Brier score is a proper alternative but is less sensitive
in the tails where mispricing matters most.

Why untestable now: **settled game outcomes are not stored in Neon for
the historical dataset** (the 007 tables contain odds only; the backfill
never extracted scores). This is stated explicitly per protocol — no
substitute metric is silently promoted in its place. A future round will
run this primary metric once outcomes are available; the push convention
above is preregistered for that round.

### Decision metric for this round (confirmatory)

**Closing-convergence MSE**: mean squared error of each method's
per-book fair probability f against the same method's closing LOBO
consensus, averaged over all cells.

Justification: with outcomes unavailable, the closest preregistered
price-based analogue is squared error against the best available ex-post
fair-price estimate — the closing consensus, which aggregates the most
information. This is the standard convergence/CLV-style criterion: the
better de-vig method's early fair prices should track the eventual
consensus more closely. MSE (not MAE) is chosen for symmetry with the
Brier/log-loss family and differentiability.

### Exploratory / descriptive only (no significance claims)

- Per-market MSE splits (spread vs total), per-book MSE.
- Calibration table: deciles of f vs mean closing consensus within decile.
- MAE vs closing consensus.
- Cross-method dispersion of fair probabilities.

## Multiple-testing correction and decision rule

- Exactly **3 pairwise comparisons** (multiplicative–additive,
  multiplicative–power, additive–power).
- Unit of analysis for inference: the **event**. Per event and method,
  compute the mean SE over that event's cells; run paired t-tests on the
  event-level mean differences (this absorbs within-event correlation
  across snapshots/books/markets).
- **Holm-Bonferroni** correction at family-wise α = 0.05.
- **Decision rule**: method M is declared the winner iff its event-level
  mean SE is significantly lower than BOTH other methods' after
  correction. Otherwise the recorded outcome is
  **"no method significantly better"** — a valid and expected outcome.
- Practical magnitude is reported alongside (mean SE differences and
  implied RMSE differences in probability points); the decision rule
  itself is purely the preregistered statistical one.

## What winning does and does not mean

**Winning this tournament does NOT promote anything from SHADOW.** It
selects the de-vig method used for subsequent forward-shadow evaluation
only. The mandatory promotion gate is unchanged: **positive CLV against
actual closing lines over forward (post-freeze) data**, plus calibration
and risk criteria. A tournament win is a method-selection step, not
evidence of edge. "No method significantly better" is a complete,
publishable outcome.

## Confirmatory vs exploratory

- **Confirmatory**: the closing-convergence MSE tournament with the
  decision rule above, run once against the frozen fingerprint.
- **Exploratory**: all per-market/per-book splits, calibration tables,
  MAE, dispersion diagnostics, and any follow-up slices conceived after
  seeing results. These are descriptive; they cannot declare a winner
  and cannot feed the decision rule.

## Execution constraints (preregistered)

- Read-only: the tournament issues SELECTs against
  `nfl_edge_historical_quotes` only. **Zero API credits** are spent;
  no HTTP calls to any odds provider.
- Freeze verification before running: row count must equal 291,586 and
  distinct snapshot count 162; a sha256 content hash over ordered
  (quote_id, american_odds, line, observed_at) is recomputed and recorded
  for comparison with the stated fingerprint. The run proceeds only if
  counts match.
- Results are written to the append-only experiment ledger
  (`nfl-edge/model/research/results/experiments.jsonl`) with the
  preregistration path, dataset fingerprint, per-method metrics,
  corrected p-values, calibration tables, and the winner /
  "no method significantly better" determination.
