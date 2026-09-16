# Preregistration (DRAFT — user freezes): nfl-h-n6-officiating-crew-totals

**Status: DRAFT. Not frozen. No outcomes inspected; no test run.**
**All work under this preregistration is SHADOW (research only).**
**Prior: LOW** (crew effects are small and may be fully priced or unstable
year to year).

- Drafted: 2026-09-15
- Dataset (frozen, read-only): fingerprint
  `43f853a44bf93937d85149ca5fd7241b`, 162/162 snapshots, 291,586 paired
  spread/total quotes. Freeze verification before running: quote count =
  291,586 and distinct snapshots = 162, else the run stops.
- Scope: FULL_GAME_TOTAL, NFL regular season 2022–2024. Postseason
  excluded (playoff crews are all-star selections, not regular crews).
- Question: does a referee crew's lagged penalty tendency carry
  INCREMENTAL information about the game total beyond a reasonable
  market/team baseline? This is not a test of raw referee-total
  correlation.

## Decision window (derived from the information set, not from snapshots)

- NFL officiating assignments for a game week are publicly knowable by
  **Tuesday** of game week (Football Zebras Tuesday-morning assignment
  posts; e.g. Week 1 2026 assignments published Tue 2026-09-08 for the
  Sun 2026-09-13 slate; team-beat media republishes the same day).
- Frozen snapshot cadence: Wed 12:00 UTC (= Wed 08:00 ET), Sat 12:00 UTC,
  Sun 15:30 UTC — every snapshot type of a game week falls strictly
  AFTER the Tuesday announcement and strictly BEFORE that week's
  earliest kickoff (Thu 20:15 ET). All 162 snapshots satisfy the
  post-crew-announcement decision window for their week's games.
- **Decision snapshot per game (mechanical):** the Wed 12:00 UTC snapshot
  of the game's week (earliest post-announcement = most decision-like).
  Fallback order if the game has no totals quotes at Wed: Sat 12:00 UTC,
  then Sun 15:30 UTC. Fallback usage is reported as missingness; it does
  not redefine the window.
- **Coverage gate (read-only, before any analysis):** count eligible games
  with ≥1 totals quote at a post-announcement snapshot. If any game lacks
  coverage at all three snapshot types, the missing games are listed and
  the minimum missing-data pull is sized at 10 credits × 1 region ×
  1 market × 1 timestamp × (games missing). Expected ≈ 0 (162/162
  snapshots received). No pull is authorized by this draft.

## Eligibility

- Game is NFL regular season, 2022–2024, present in the frozen dataset
  with a decision-snapshot totals consensus (≥3 books incl. Pinnacle at
  the consensus line, same construction as the frozen de-vig work).
- nflverse officials record exists for the game with exactly one referee
  (verified coverage: 272/272 per season 2022–2024; zero multi-referee
  anomalies).
- Realized total available from nflverse (sealed until the test runs).
- Exclusions: postseason; games voided/pushed on the consensus total
  (settled as void in the economic endpoint, excluded from the primary
  statistic); any game whose crew is a mid-season replacement
  designation that cannot be mapped to a referee identifier (reported, not
  imputed).

## Feature construction (strictly lagged; no test-season information)

- Crew identity = referee (crew chief) from nflverse `load_officials()`.
  Mid-season personnel substitutions are absorbed in the referee
  identifier (documented limitation).
- **Primary feature:** crew's lagged accepted-penalties-per-game rate,
  computed over seasons strictly before the test season
  (2015–2021 for the 2022 fold; 2015–2022 for 2023; 2015–2023 for 2024),
  shrunk toward the league mean by empirical Bayes with weight
  n_games/(n_games + 16) (k=16 = one season, pre-specified).
- Penalty counts come from lagged-season gamebooks/PBP (information-set
  data, not test outcomes).
- Exploratory only (reported descriptively, never confirmatory):
  offensive-holding and defensive-PI sub-rates; automatic-first-down rate.

## Baseline (the "reasonable market/team baseline" the feature must beat)

Per game, fitted on training folds only:
- Consensus totals line L = median line across books at the decision
  snapshot (multiplicative de-vig from raw American odds; Pinnacle
  included — it is part of the market).
- Controls: home-team fixed effect, away-team fixed effect, season and
  week effects, stadium roof type. The market line already embeds team
  strength, primetime/marquee assignment effects, and weather
  expectations; team FEs absorb persistent team penalty discipline and
  pace; week/season effects absorb rule-emphasis changes.
- Documented confounders and absorption: (1) crew assignment is not
  random — senior crews get marquee games with higher totals → absorbed
  by L; (2) dome vs outdoor → absorbed by L + roof control; (3) team
  penalty discipline → team FEs; (4) crew turnover year to year →
  referee-level shrinkage, never current-season data.

## Target / settlement

- Realized total points R (nflverse; sealed until test). Pushes on L are
  void.
- Primary scale: residual of R after the baseline (linear).
- Economic endpoint (pre-specified, secondary): crew-adjusted total
  L* = L + β̂_train × (pen_rate_c − league mean), β̂ from training folds
  only. Lean Over if L* − L ≥ 0.5, Under if ≤ −0.5, else no bet. Enter at
  the decision-snapshot consensus price; settle vs R at −110, 1u fixed.
  Report mean per-game P&L with 95% CI (clustered by referee).

## Primary statistic

- 3-fold leave-one-season-out. Per fold: fit baseline and
  baseline+feature on the other two seasons; compute OOS squared-error
  loss difference per game (baseline − augmented; positive = feature
  helps) on the held-out season.
- Combine the 3 fold-level mean differences by inverse-variance
  weighting. One-sided test of combined improvement > 0, α = 0.05,
  with SEs clustered by referee (17 clusters).
- Directional requirement: pooled training-fit coefficient on the
  penalty-rate feature must be > 0 (more flags → higher totals) AND the
  fold-level improvement must be positive in ≥ 2 of 3 folds. A
  significant improvement with the wrong sign is a FAIL on mechanism.

## Season folds / clustering

- Folds: {train 2023+2024 → test 2022}, {train 2022+2024 → test 2023},
  {train 2022+2023 → test 2024}. Tendency features always use seasons
  strictly before the test season.
- Clustering: game-level observations; inference clustered by referee
  (17 clusters); fold-consistency rule as above.

## Economic endpoint

- As defined under Target/settlement. Practical bar (pre-declared):
  mean per-game P&L ≥ +0.005u (0.5% of stake).

## Exclusions / missing-data treatment

- Complete-case at the game level. Every attrition step is reported:
  frozen-dataset games → with decision-snapshot totals quotes → with
  crew assignment → with realized total → with non-push settlement.
- No imputation of crews, lines, or totals. Ambiguous referee mappings
  (none observed in 2022–2024) would be listed and excluded, never
  fuzzy-matched.

## Power caveat (pre-registered honestly)

- The coefficient is identified off cross-crew variation: 17 referee
  clusters. Effective independent units ≈ 17, not ~800. Cluster-robust
  inference with 17 clusters has limited power for small effects; the
  fold-consistency rule and shrinkage are guardrails, not cures.
- This test can only detect crew effects large enough to move through
  17-cluster inference. A null result retires the family; it does not
  prove crews don't matter.

## Exact pass / fail / retirement rule

- **PASS:** OOS improvement significant one-sided at α=0.05 AND
  directional sign correct AND positive in ≥2 of 3 folds AND economic
  endpoint ≥ +0.005u/game. → Promote to the paid-test candidate list
  (no pull needed; data already frozen).
- **INCONCLUSIVE (parked, not failed):** statistically significant but
  economic endpoint < +0.005u/game.
- **FAIL (retire):** no significant OOS improvement, or significant with
  wrong sign, or positive in <2 folds. Family retired; no variants.
- **INFEASIBLE:** < 700 eligible games with complete data → retire
  without running.

## Feasibility gate result (2026-09-15)

- Officials coverage: 272/272 regular-season games × 3 seasons, one
  referee each, 17 crews/season, lagged seasons 2015–2021 available.
- Timing: all 162 frozen snapshots are post-crew-announcement for their
  week's games. **Zero API credits required; the 8,160-credit pull is
  unnecessary.**
- Exact game-level eligible count = (frozen-dataset events) ∩ (272×3
  nflverse games with crews); the intersection is computed read-only at
  implementation after NFL identity validation. Expected ≥ 790.
- API credits authorized by this draft: **0**.

*DRAFT — frozen only by user approval. No outcomes inspected.*
