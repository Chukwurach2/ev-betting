# Preregistration (DRAFT — user freezes): nfl-h-n3-rest-travel-h1

**Status: DRAFT. Not frozen. No outcomes inspected; no test run.**
**All work under this preregistration is SHADOW (research only).**
**Prior: LOW** (rest edges are well-studied; any remainder likely lives
in less-scrutinized derivatives).

- Drafted: 2026-09-15
- Scope: NFL regular season, **2023–2024 only** (period markets have no
  provider history before 2023-05-03; 2022 cannot supply H1 quotes).
- Question: does a material rest differential between the teams predict
  the first-half margin beyond what the full-game and first-half market
  lines already imply?

## Eligibility (mechanical, from free data — no outcome peeking)

- Regular-season game, 2023–2024, week ≥ 2 (week 1 has no defined rest).
- **Material rest differential:** |rest_home − rest_away| ≥ 3 days, where
  rest = calendar days between a team's consecutive kickoffs,
  computed **within-season** from nflverse schedules (preseason-known,
  leak-proof). Rationale for 3 days, pre-specified: the smallest
  threshold mapping to a real scheduling structure (mini-bye 10 vs 7;
  short-week 4 vs 7). No outcome data was consulted in choosing it.
- Deterministic identity match (nflverse game_id ↔ Odds event id)
  required — pending NFL identity validation.
- ≥1 book quoting **both** `spreads_h1` and `totals_h1` at the decision
  timestamp (coverage unknown until the pull — reported as missingness).
- Realized 1H margin available from nflverse play-by-play (availability
  known-complete for 2023–2024; values sealed until the test runs).
- Exclusions: postseason; week 1; games with missing H1 quotes at the
  decision timestamp (missingness reported, never imputed); pushes on
  the H1 line (void in the economic endpoint, excluded from the primary
  statistic).

## Snapshot timing (mechanical, point-in-time safe)

- **Decision timestamp per game:** the provider's 5-minute historical
  snapshot nearest to **24 hours before kickoff** (one timestamp per
  game). The rest information is preseason-known, so intra-week timing
  is not load-bearing; 24h-before is strictly pre-kickoff and
  mechanical.
- Markets pulled at that timestamp only: `spreads`, `spreads_h1`,
  `totals_h1` (narrow pull — nothing else).

## Feature construction

- Signed rest differential d = rest_home − rest_away (days). Continuous;
  no binning (binning choices would be outcome-adjacent).
- Travel/altitude/international indicators are recorded as covariates
  for the exploratory (non-confirmatory) specification only.

## Target / settlement

- Realized 1H margin M = home 1H points − away 1H points (nflverse PBP;
  sealed until test).
- Controls (from the decision timestamp): full-game spread consensus and
  1H spread consensus (median across quoting books, de-vigged). The
  full-game spread is the required control — this lane does not reuse
  the frozen dataset.
- Primary scale: residual of M after the spread controls (linear).

## Primary statistic

- **Design: 2023 → 2024 walk-forward** (2 seasons cannot do 3-fold
  LOSO). Fit residual-on-d on 2023 eligible games; test the
  pre-specified directional hypothesis on 2024 eligible games only.
- One-sided t-test of the rest-differential coefficient β > 0 on the
  2024 test set (more home rest advantage → higher home 1H margin),
  α = 0.05, HC1 robust SEs. Single pre-specified coefficient; no
  specification search.

## Season folds / clustering

- Train: 2023 eligible games. Test: 2024 eligible games. One shot.
- Game-level observations; team repetition across games noted as a
  limitation (HC1 SEs; no team clustering pre-specified to avoid
  post-hoc choices).

## Economic endpoint (pre-specified, secondary)

- Using β̂ from the 2023 fit: lean on the 1H spread in the direction of
  the rest-advantaged team when |β̂ × d| ≥ 1.0 point, else no bet.
  Enter at the decision-timestamp H1 consensus price; settle vs M at
  −110, 1u fixed. Report mean per-game P&L with 95% CI.
- Practical bar: mean per-game P&L ≥ +0.005u.

## Exclusions / missing-data treatment

- Complete-case at the game level. Attrition reported at each factor:
  eligible rest differential → identity match → H1 quote availability
  at the decision timestamp → outcome availability → non-push
  settlement. No imputation at any step.

## Power / feasibility (computed 2026-09-15, free data only)

- Raw eligible (|d| ≥ 3), 2023–2024: **121 games** (56 in 2023, 65 in
  2024). SD of d in the eligible sample: 4.97 days.
- Intersection (expected): 121 × ~0.978 (identity, pending validation)
  × H1-availability (unknown until pull) × ~1.0 (PBP outcome
  availability) ≈ **59–100 pooled**; 2024 test set ≈ **30–51**.
- Minimum detectable effect (80% power, α=0.05 two-sided) for β in
  points of 1H margin per day of rest advantage, assuming residual SD
  σ=9 (conservative; σ to be estimated from the 2023 training fit at
  implementation):
  MDE ≈ 2.8·σ/(s_d·√N) → **0.6–0.9 pts/day** on the 2024 test set
  (N≈30–51), ≈ 0.5 pts/day pooled (N≈100).
- A 7-day differential (bye vs normal week) at MDE 0.6–0.9/day implies
  **4–6 points of first-half margin** — 3–6× the plausible mechanism
  magnitude (published full-game rest effects are ~1–2 points total,
  i.e. ≤0.3 pts/day; the 1H share is smaller). The test cannot detect
  realistic effect sizes at any attainable N in the 2023–2024 H1 window.

## Exact pass / fail / retirement rule

- **PASS:** β significant one-sided at α=0.05 on the 2024 test set AND
  sign correct AND economic endpoint ≥ +0.005u/game.
- **FAIL (retire):** β not significant, or significant with the wrong
  sign. Family retired; no variants, no re-pull.
- **INFEASIBLE → RETIRE (pre-pull gate):** if, after the pull, final
  test-set N < 30 (MDE > 0.9 pts/day) → retire without running the test.
- **Feasibility verdict (2026-09-15): RETIRE WITHOUT SPENDING.** The
  power analysis above shows the design is underpowered for plausible
  effects even before H1-availability attrition. Do not authorize the
  ~3,630-credit pull (121 × 3 markets × 1 ts × 10, us region).

*DRAFT — frozen only by user approval. No outcomes inspected.*
