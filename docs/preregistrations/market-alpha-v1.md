# Preregistration: market-alpha v1 (Pinnacle lead/lag + stale prices)

**Status: PREREGISTERED — no results viewed. Analysis code does not yet exist.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Dataset (frozen): `docs/dataset-freeze.md`
- Dataset fingerprint: `43f853a44bf93937d85149ca5fd7241b`
- Scope: seasons 2022–2024, weeks 1–18, markets FULL_GAME_SPREAD and
  FULL_GAME_TOTAL, regions us,eu — 162/162 snapshots, 291,586 paired quotes.
- Question: do two classical market-microstructure edges exist in NFL
  full-game spread/total markets — (A) Pinnacle leading other books, and
  (B) stale cross-book prices?

## Shared setup (both tracks)

- **Game identity:** canonical game keys from the event-identity audit —
  `(home_team, away_team)` + kickoff-proximity clustering (7-day gap),
  reusing `canonical_game_keys` in `nfl-edge/research/devig_tournament.py`.
  Provider event ids are NOT used as game identity.
- **Pair selection:** per (game, market, snapshot, book), the book's
  two-sided pair at its modal line within the snapshot; ties broken by
  closeness to the cross-book median line at that snapshot; deterministic
  (same rule as the de-vig tournament).
- **Reference selection:** spreads → home-team side; totals → Over.
- **Fair probabilities:** multiplicative de-vig (tournament v1 winner),
  recomputed from raw `american_odds` on the book's own pair.
- **Books:** `pinnacle` is the candidate leader. Confirmatory follower
  family (Track A) = draftkings, fanduel, betmgm, betrivers,
  williamhill_us — the five US majors present in all snapshots.
- **Point-in-time safety:** a snapshot's quotes are compared only against
  information available at or before that snapshot's `observed_at`.
  Closing snapshots are used only as ex-post benchmarks (the standard CLV
  target), never as inputs to any signal.
- **Target-book exclusion:** a book never contributes to a consensus used
  to evaluate that same book (strict LOBO wherever a consensus appears).
- **Freeze verification before running:** quote count = 291,586 and
  distinct snapshots = 162, else the run does not proceed.
- **Execution:** read-only SELECTs against `nfl_edge_historical_quotes`
  only. Zero Odds API credits; no provider HTTP calls.

## Track A — Pinnacle lead/lag

**Hypothesis (confirmatory):** Pinnacle moves first; other books converge
toward Pinnacle's earlier price in later snapshots.

### Definitions (pre-declared)

- For canonical game g, market m, book b: let the game's snapshot
  sequence be the sorted distinct `observed_at` values with a valid pair
  for b at its selected line. f_b(t) = multiplicative no-vig fair
  probability of the reference selection from b's pair at snapshot t.
- **Pinnacle move** at step t ≥ 1: |f_pin(t) − f_pin(t−1)| ≥ X with
  **X = 0.01** (one probability point). Direction d = sign of the move.
  Rationale for X: a half-point spread move or a −110→−115 juice move
  shifts fair probability by roughly 0.01; smaller changes are dominated
  by quote noise.
- **Follower response:** for follower book B with valid pairs at t and
  t+1 (same game/market): Δ_B = f_B(t+1) − f_B(t).
  - **Agreement event:** sign(Δ_B) == d, counted only when |Δ_B| ≥ 0.002
    (moves smaller than 0.2 probability points are "no response" and are
    excluded from numerator AND denominator; the excluded count is
    reported).
- A Pinnacle-move event requires Pinnacle pairs at t−1 and t AND the
  follower's pairs at t and t+1 — all four from the same canonical game
  and market. No future information enters the signal: the "move" is
  known at t, the response is measured at t+1.

### Primary test (confirmatory)

- Per follower book B: agreement rate = agreements / eligible events.
  **One-sided binomial test** of agreement rate > 0.5.
- **Minimum 30 eligible events per book**; a book with fewer is reported
  as infeasible (not as a null result).
- **Multiple testing: Holm-Bonferroni across the 5 follower books**,
  family-wise α = 0.05.

### Effect size (pre-declared)

- Mean and median **catch-up fraction**
  c = 1 − |f_B(t+1) − f_pin(t)| / |f_B(t) − f_pin(t)|,
  computed only where the denominator ≥ 0.005 (else the event is
  excluded from c and reported). c = 1 means B fully converged to
  Pinnacle's t-price by t+1; c = 0 means no convergence; c < 0 means
  divergence. Reported per book and pooled.
- Plain-language translation: "when Pinnacle moved, followers moved the
  same direction next snapshot Z% of the time (chance = 50%), closing on
  average c×100% of the gap."

### Secondary confirmatory check

- **Pooled test:** all follower books' agreement events pooled, one-sided
  binomial vs 0.5 at α = 0.05 (single test).

### Falsification check (pre-declared)

- **Reverse test:** for each follower book B, define B's moves
  (|Δ_B| ≥ 0.01 between B's consecutive snapshots) and test whether
  Pinnacle's subsequent change (t → t+1) agrees in direction — one-sided
  binomial vs 0.5, Holm across the 5 books at α = 0.05. If ANY reverse
  test is significant, the "Pinnacle leads" interpretation FAILS (moves
  are mutually correlated, not Pinnacle-led). This check is binding on
  the verdict.

### Decision rule (Track A)

"Lead/lag edge exists" iff ALL of:
1. ≥ 3 of the 5 follower books are Holm-significant with agreement
   rate > 0.5;
2. the pooled test is significant at α = 0.05;
3. NO reverse test is significant (falsification passes).

Otherwise the verdict is **"no lead/lag signal"** (if nothing is
significant) or **"mixed/inconclusive"** (partial significance or
failed falsification) — both are complete, publishable outcomes.

### Exploratory (Track A, no significance claims)

- Sensitivity: X = 0.02 move threshold.
- Per-market splits (spread vs total).
- Catch-up fraction distributions; time-to-convergence (how many
  snapshots until |f_B − f_pin| < 0.005).
- Cross-follower lead/lag (e.g. does DraftKings lead FanDuel?).
- Other books (beyond the 5 majors) as followers/leaders.

## Track B — stale prices

**Hypothesis (confirmatory):** at a given snapshot, at least one book's
quote is stale relative to the cross-book consensus, and betting the
stale side at that quote would have beaten the closing consensus.

### Definitions (pre-declared)

- For canonical game g, market m, snapshot s, book b with a valid pair
  at its selected line: f_b(s) = multiplicative no-vig fair probability
  of the reference selection from b's own pair.
- **Same-snapshot LOBO consensus:** C_{≠b}(s) = median of f over books
  b' ≠ b with valid pairs at s; require ≥ 2 other books. **The evaluated
  book never enters its own consensus.**
- **Stale flag:** |f_b(s) − C_{≠b}(s)| ≥ Y with **Y = 0.02** (two
  probability points). Rationale for Y: twice the Track A move
  threshold — large enough to be economically interesting if real, small
  enough to occur with usable frequency.
- **Stale side (hypothetical bet):** if f_b(s) < C_{≠b}(s), the book is
  generous on the reference selection → bet the reference selection at
  book b. Otherwise bet the other selection (away side / Under) at
  book b.
- **Closing benchmark:** the game's latest (pre-kickoff) snapshot.
  F_close = median multiplicative no-vig fair probability for the *bet
  selection* across books ≠ b at the closing snapshot (≥ 2 required;
  complete-case: the stale cell counts only if the closing benchmark is
  available). The closing consensus is the ex-post benchmark, never an
  input to the stale flag.
- **Edge per stale cell:** e = F_close − p_bet_b, where p_bet_b is book
  b's no-vig probability for the bet selection at s. e > 0 ⟺ the
  stale-side price beat the closing consensus (got a better price than
  the eventual fair price).

### Primary test (confirmatory)

- **Unit of analysis: the game.** Per game g: e_g = mean of e over the
  game's stale cells (a cell = (market, snapshot, book) triple flagged
  stale). Games with no stale cells are excluded.
- **One-sided one-sample t-test** of mean(e_g) > 0 at α = 0.05 (single
  primary test; no multiplicity adjustment needed).
- Report mean, 95% CI, and n games.

### Secondary (confirmatory, reported alongside)

- **Hit rate:** fraction of stale cells with e > 0; one-sided binomial
  vs 0.5 at α = 0.05.

### Practical bar (pre-declared)

- Mean edge **≥ 0.01 (one probability point)** to be called practically
  meaningful — roughly the scale of a half-point line move, the minimum
  that could survive execution friction.
- Significant but < 0.01 → verdict **"statistically significant but
  practically negligible."**

### Decision rule (Track B)

"Stale-price edge exists" iff the primary test is significant at
α = 0.05 AND mean(e_g) ≥ 0.01. Otherwise **"no stale-price signal"**
(not significant) or **"significant but negligible"** (significant and
< 0.01).

### Key limitation (pre-declared, binding on interpretation)

- **Counterfactual execution assumption:** the analysis assumes the
  stale quote was actually available for betting at `observed_at`.
  Snapshot quotes are observed at a point in time; the quote may have
  been pulled or moved between the book's update and the snapshot, and
  availability at size is unverified. A positive finding therefore
  measures *apparent* staleness, and forward-shadow validation must
  verify executability (freshness ≤ threshold, fill confirmation).
- Books that are slow to update may also shade or limit; not modeled.

### Falsification / alternative explanations (pre-declared)

- The "pure noise" alternative (deviations are just book noise, equally
  likely to beat or lose to the close) is exactly the null of the
  primary test.
- **Persistence check (exploratory):** is the same book stale on the
  same game at consecutive snapshots, or are flags one-off noise?

### Exploratory (Track B, no significance claims)

- Per-book stale rates and per-book mean edge.
- Per-market splits (spread vs total).
- Sensitivity: Y = 0.03.
- Edge vs time-to-kickoff (are stale quotes more common early week?).

## What a positive finding does and does not mean

**A positive finding on either track does NOT constitute a betting edge,
a model promotion, or a publishable recommendation.** It selects the
hypothesis for **forward-shadow validation only**. The mandatory
promotion gate is unchanged: **positive CLV against actual closing
lines on forward (post-freeze) data**, plus calibration and risk
criteria. "No signal" and "mixed/inconclusive" are complete,
publishable outcomes.

## Confirmatory vs exploratory

- **Confirmatory:** Track A primary tests (5 books, Holm), pooled check,
  reverse falsification, and the Track A decision rule; Track B primary
  t-test, hit-rate check, practical bar, and the Track B decision rule.
  Each run once against the frozen fingerprint.
- **Exploratory:** all sensitivity thresholds, per-market/per-book
  splits, persistence checks, cross-follower lead/lag, and any follow-up
  slice conceived after seeing results. Descriptive only; cannot declare
  an edge and cannot feed the decision rules.

## Execution constraints (preregistered)

- Read-only: SELECTs against `nfl_edge_historical_quotes` only. **Zero
  API credits**; no HTTP calls to any odds provider.
- Freeze verification before running: 291,586 quotes, 162 snapshots.
- Results appended to the experiment ledger
  (`nfl-edge/model/research/results/experiments.jsonl`) with the
  preregistration path, dataset fingerprint, per-book test statistics,
  corrected p-values, effect sizes, confidence intervals, falsification
  results, and the per-track verdict.
- Registry updated: `nfl-edge/research/experiments.json`
  (`market-alpha-v1`, `completed_shadow_only` or per-track outcome).
