# Preregistration (DRAFT — NOT FROZEN): nfl-h-n8-redzone-conversion-totals

**Status: DRAFT 2026-09-16. Not frozen. No outcomes inspected; no test
run; no statistical capital spent. This document becomes binding only
after (a) the exact eligible-N gate returns PROCEED-TO-FREEZE and (b)
explicit user approval to freeze. Until then every "frozen" below means
"proposed frozen content".**
**All work under this preregistration is SHADOW (research only).**
**Prior: MODERATE** (red-zone finishing is a widely cited driver of
scoring, which cuts both ways: the mechanism is real, but totals markets
are the most efficient market in sports and "finishing drives" is exactly
the kind of narrative sharps price; the test adjudicates strictly
*incremental* information beyond the market total, not whether red-zone
efficiency matters).

- Drafted: 2026-09-16
- Family id: **H-N8** (next free NFL family id; H-N7 was never assigned).
- Dataset (frozen, read-only): manifest v1, fingerprint
  `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`,
  scope `nfl-2022-2024-spread-total`, 162/162 snapshots, 291,586 paired
  spread/total quotes. Freeze verification before running: quote count =
  291,586 and distinct snapshots = 162, else the run stops. v1 is never
  edited; a scope change mints v2.
- Scope: FULL_GAME_TOTAL, NFL regular seasons 2022–2024. Postseason
  excluded (not in the frozen dataset).
- Question: does timestamp-safe *lagged situational conversion
  efficiency* contain INCREMENTAL information about the game total beyond
  the contemporaneous betting-market total? This is not a test of raw
  conversion–scoring correlation.

## Explicit distinction from retired H-N4 (pace/tempo)

Retired H-N4 tested **pace/tempo**: plays per minute, seconds per play,
no-huddle rates — *how fast* teams play. H-N8 tests **conversion
efficiency in high-leverage situations**: red-zone TD rate, third-down
conversion rate, goal-to-go TD rate, fourth-down conversion rate — *how
often* teams finish drives once they reach scoring position. These are
disjoint feature families (a team can play fast and finish poorly, or
slow and finish well). H-N8 is not a re-labeling of H-N4 and inherits
none of its analysis; H-N4's FAIL verdict stands untouched and its
features are not inputs here.

## Information set (frozen)

Only information available by the prediction cutoff (kickoff − 24h):

- **Conversion features:** nflverse play-by-play, seasons 2021–2024
  regular season only (2021 is a *feature-history source only*, never a
  target season), aggregated into trailing team histories strictly
  before each game's kickoff (§Lag structure). Play-level inputs only;
  game scores, game totals, and anything derived from them are never
  read (the gate loads a strict column allowlist that excludes every
  score column).
- **Market input:** the frozen totals consensus at the decision snapshot
  (defined below). No other market data enters the information set.
- **Disqualified, permanently:** realized game totals, closing totals
  beyond the decision snapshot, any outcome-derived residual or its
  variance, re-aggregated features after seeing results, alternate
  feature definitions.

## Closed conversion-metric list (frozen) and the single primary

Exactly four lagged conversion metrics are defined. The DRAFT commits to
**exactly one** of them as the primary feature (single pre-named
primary, not a composite, not best-of-four). The other three are
documented as considered-and-rejected for primary status; they are
**not** secondary endpoints and **not** rescue variants.

All four are constructed at the **drive level** from nflverse
play-by-play with these frozen play rules:

- A **counted snap** is a play with `play_type` in {`pass`, `run`}.
  (Special-teams plays, kneels, and spikes never create situational
  opportunities by construction.)
- A **red-zone trip** (offense, team T, game G, drive D): drive D
  contains ≥1 counted snap with `posteam == T` and `yardline_100 <= 20`.
- A **goal-to-go situation** (offense, team T, game G, drive D): drive D
  contains ≥1 counted snap with `posteam == T` and `goal_to_go == 1`.
- **Third-down attempt**: a play with `third_down_converted == 1` or
  `third_down_failed == 1` (nflverse's own accounting), attributed to
  `posteam`. Conversion ⟺ `third_down_converted == 1`.
- **Fourth-down attempt**: a play with `fourth_down_converted == 1` or
  `fourth_down_failed == 1`, attributed to `posteam`. Conversion ⟺
  `fourth_down_converted == 1`.
- A drive's **offensive TD**: ≥1 play in the drive with `touchdown == 1`
  and `td_team == T` (return touchdowns credited to the opponent do not
  count as offensive conversion).

The four metrics (per team, per trailing window):

1. **RZ-TD%** = offensive-TD drives among red-zone trips
   (numerator: trips with an offensive TD; denominator: red-zone trips).
2. **3D%** = third-down conversions / third-down attempts.
3. **GTG-TD%** = offensive-TD drives among goal-to-go situations
   (numerator: goal-to-go drives with an offensive TD; denominator:
   goal-to-go drives).
4. **4D%** = fourth-down conversions / fourth-down attempts.

**PRIMARY (frozen): metric 1, RZ-TD%, as the home-minus-away offensive
differential** `x = RZ-TD%_off(home) − RZ-TD%_off(away)`. Defensive
red-zone rates are a distinct, untested family and are not inputs here.
No weighted composite is formed; no metric is substituted after the
feasibility gate.

**Pre-specified direction: positive.** Better red-zone finishing
relative to the opponent → more points than the market total implies →
positive slope on the residual (realized − market).

## Lag structure (frozen)

- Per team, per game: the **8 most recent regular-season games** with
  `(season, week)` strictly before the game's own `(season, week)`,
  drawn from nflverse 2021–2024 REG (postseason excluded everywhere).
  Week-ordering is exact because a team plays at most once per week; no
  within-week kickoff comparisons are needed and no within-game updates
  ever enter.
- 2021 games are eligible history for 2022 early-season games (point-in-
  time safe: full completed seasons, free nflverse data).
- **Minimum sample (frozen):** each team must have **≥12 offensive
  red-zone trips** inside its trailing-8 window. Same rule for both
  teams; a game failing the minimum for either team is **ineligible**
  (never imputed, never carried forward).
- All trailing features are fixed at kickoff − 24h by construction
  (they use only completed prior games).

## Decision window (mechanical, game-specific)

- Prediction cutoff = kickoff − 24h (kickoff = nflverse gameday+gametime
  parsed as America/New_York → UTC; the schedules CSV loader reads only
  gameday/gametime/stadium columns — scores never read).
- **Decision snapshot per game:** the latest frozen cadence snapshot
  (Wed 12:00 UTC / Sat 12:00 UTC / Sun 15:30 UTC of the game's week)
  with snapshot instant **strictly before** kickoff − 24h. Same
  mechanical rule as H-N2.
- **Consensus construction** (identical to the frozen de-vig work and
  the H-N2/H-N6 gates): per-book median Over line at the decision
  snapshot, then median across books = consensus line L; books "at the
  consensus line" are those within 0.01. Eligible iff ≥3 books at the
  line **including Pinnacle**.
- **No fallbacks.** A game without an eligible consensus at its own
  decision snapshot is OUT. Missingness is reported by season; the
  window is never widened to absorb gaps.

## Eligibility (mechanical funnel; attrition reported at every step)

1. NFL regular season 2022–2024 game in the bundled nflverse schedules.
2. Canonical identity match → ≥1 Odds event id (deterministic layer;
   `docs/nfl-identity-match-report.md`; the 2022 W17 BUF@CIN suspended
   game is `missing_from_source` and can never be eligible).
3. Eligible totals consensus at the decision snapshot (§Decision
   window).
4. Feature history: both teams satisfy the trailing-8 / ≥12-trip
   minimum (§Lag structure).

The exact count is produced by `nfl-edge/ops/nfl_hn8_eligibility.py`
(read-only, zero credits, zero outcomes; dispatch
`nfl-hn8-eligibility.yml`). **Exact N is PENDING** until that run
completes.

## Baseline and model form (frozen)

- L = consensus totals line at the decision snapshot. R = realized total
  points (nflverse; sealed until the test runs).
- Target: residual `R − L` (the market-adjusted quantity — this asks
  whether conversion efficiency adds information the market total has
  not incorporated). Raw scoring is never the primary endpoint.
- Per season-fold (see below), on training folds fit by OLS:
  - **Baseline:** `R − L ~ season FE + decision-slot FE`
    (slot ∈ {wed, sat, sun} absorbs consensus staleness).
  - **Augmented:** baseline `+ x`, with `x` centered at the
    **training-fold** mean.
  - No team fixed effects: they would absorb the between-team
    conversion variation that is the legitimate identifying variation
    (conversion efficiency is a team attribute measured pre-cutoff, not
    a post-cutoff assignment).
- Per-game OOS loss difference on the held-out season:
  `d_i = (e_baseline,i)² − (e_augmented,i)²` (positive = conversion
  features help).

## Primary statistic (frozen)

- 3-fold leave-one-season-out: {train 2023+2024 → test 2022},
  {train 2022+2024 → test 2023}, {train 2022+2023 → test 2024}.
- Pool all held-out games (each game appears exactly once as held-out).
  **One-sided t-test of mean(d) > 0, α = 0.05, SEs clustered by
  season-week** (54 clusters; absorbs within-week correlation across
  games).
- **Directional gate:** pooled OLS coefficient on `x` (same spec, all
  data) must be **> 0** AND fold-level mean(d) > 0 in **≥ 2 of 3 folds**.
  A significant improvement with the wrong sign is a FAIL on mechanism.

## Season folds / clustering

- Folds as above; conversion features use only pre-cutoff prior games
  (no test-season or post-cutoff information anywhere).
- Clustering unit: season-week (3 seasons × 18 weeks = 54 clusters).

## Economic endpoint — Stage 2 (frozen, secondary)

- Conversion-adjusted total `L* = L + β̂_train·(x − x̄_train)`, β̂ from
  training folds only, applied to the held-out fold.
- **Betting rule:** Over 1u iff `L* ≥ L + 1.5` (conversion implies ≥1.5
  pts of upward adjustment — a pre-specified meaningful line move).
- **Execution:** enter at the best available Over price (max payout per
  unit; ties → alphabetical book_key) among consensus books quoting the
  Over at the consensus line at the decision snapshot; if none, no bet
  (reported). Settle vs R at the taken price; pushes return stake.
- Report mean per-game P&L with 95% CI (season-week-clustered).
- **Practical bar (pre-declared): +0.005u/game** (H-N2/H-N6 precedent).
  Statistical significance alone does not promote H-N8.

## Exclusions / missing-data treatment

- Complete-case at the game level. Every attrition step reported:
  bundle games → identity-matched → decision consensus →
  minimum-sample satisfied → eligible.
- No imputation of lines, features, or history. Pushes on L contribute
  d_i mechanically (no special-casing) in the primary statistic and
  settle as stake-returned in the economic endpoint.

## Power / MDE and the feasibility rule (frozen)

- Planning MDE for the slope (one-sided α=0.05, power=0.8):
  `MDE = 2.4865 · 13.5 / (sd_x · √N)` pts per 1.0 of `x`, with σ_res =
  13.5 pts (planning constant from the known residual scale of NFL
  totals; not re-estimated from outcomes) and sd_x = the **observed**
  SD of the primary feature `x` across eligible games (a feature
  summary, not an outcome).
- A slope of 10 pts/unit = 1.0 pt per 10pp of RZ-TD% differential. The
  test is worth running only if it can detect effects at or below that
  scale.
- **Feasibility rule (frozen):** the eligibility gate returns exact N
  and observed sd_x.
  - N < 250 → **INFEASIBLE**: retire without running.
  - MDE at exact N > **10.0 pts/unit** → **RETIRE-ON-POWER**: retire
    without outcome exposure (same standard as H-N2/H-N3/H-N6).
  - Else → PROCEED-TO-FREEZE (freeze still requires explicit user
    approval; approval does not re-open any frozen content).

## Exact pass / fail / retirement rule

- **PASS:** OOS mean(d) > 0 significant one-sided at α=0.05 AND pooled
  β̂ > 0 AND fold-level mean(d) > 0 in ≥2/3 folds AND economic endpoint
  ≥ +0.005u/game. → H-N8 advances to prospective confirmation (no
  further historical testing).
- **INCONCLUSIVE (parked, not failed):** statistically significant but
  economic endpoint < +0.005u/game.
- **FAIL (retire):** no significant OOS improvement, or significant with
  wrong sign, or positive in <2 folds. Family retired permanently under
  this specification. No variants: no nearby metrics (3D%, GTG-TD%,
  4D%), no composite, no nearby trip minimums, no nearby lags, no
  defensive-rate reframing, no alternate endpoints presented as
  independent confirmation.
- **INFEASIBLE / RETIRE-ON-POWER:** per the feasibility rule above;
  no test run, no outcome exposure.

## Outcome-blindness (standing implementation integrity requirement)

The feasibility gate may inspect ONLY non-outcome quantities: eligible
N, the conversion-feature SDs/distributions (all four closed metrics
may be summarized as features), coverage/missingness (identity,
consensus snapshot availability, minimum-sample attrition), and the
frozen planning constant σ_res = 13.5. It must NEVER read: actual game
scores or totals, realized residuals, empirical residual variance,
Stage 1 coefficients, or anything derived from game outcomes. The gate
loads nflverse play-by-play through a strict column allowlist that
excludes every score column, and aggregates only play-level features
from strictly-prior games (the frozen information set — not outcome
inspection of the target sample). The gate certifies
`outcome_rows_read = 0` alongside N, feature SDs, MDE, and attrition.

## Power caveat (pre-registered honestly)

- The slope is identified off between-team conversion differences that
  persist over 8-game windows; to the extent the market already prices
  recent red-zone form into the total, the residual slope is small and
  a null result cannot distinguish "fully priced" from "never useful".
- 12 trips is a small denominator per team (binomial noise ≈
  √(.5·.5/12) ≈ 14pp per team); the differential inherits that noise,
  which attenuates the slope and costs power vs the iid calculation —
  the MDE above is mildly optimistic.
- A null retires the family; it does not prove conversion efficiency
  doesn't matter.

## What is frozen in this draft vs what remains pending

**Proposed frozen (binding only after user-approved freeze):**
hypothesis and its explicit distinction from H-N4, information set,
the closed 4-metric list with drive-level construction rules, the
single pre-named primary (RZ-TD% offensive differential, positive
direction), trailing-8 lag with (season,week) ordering and the ≥12-trip
minimum per team, decision window (latest pre-cutoff slot, no
fallbacks), consensus construction, baseline, model form, controls,
LOSO OOS primary statistic with season-week clustering and the ≥2/3-
fold sign gate, economic endpoint and the +0.005u/game bar, N floor
250, MDE cap 10.0 pts/unit, and the full pass/fail/retirement rule.

**Pending:**
1. Exact eligible N and observed feature SDs — produced by
   `nfl-edge/ops/nfl_hn8_eligibility.py` via `nfl-hn8-eligibility.yml`
   (read-only, zero credits, zero outcomes). Status: **PENDING**.
2. User approval to freeze. The freeze changes nothing in the text
   above; it only flips this document's status.

## Statistical capital

Zero consumed to date. If frozen and run, H-N8 spends one family-wise
slot (α=0.05, Holm across active families). H-N2's retirement consumed
none (retired on feasibility); H-N8 is likewise zero-cost unless it
reaches a frozen test.

## Files

- `nfl-edge/ops/nfl_hn8_eligibility.py` — eligible-N gate (read-only)
- `nfl-edge/tests/test_nfl_hn8_eligibility.py` — unit tests
- `.github/workflows/nfl-hn8-eligibility.yml` — dispatch workflow

*DRAFT 2026-09-16. Not frozen. No outcomes inspected. No test run.*
