# Preregistration (DRAFT — NOT FROZEN): nfl-h-n7-special-teams-totals

**Status: DRAFT 2026-09-16. Not frozen. No outcomes inspected; no test
run; no statistical capital spent. This document becomes binding only
after (a) the exact eligible-N gate returns PROCEED-TO-FREEZE and (b)
explicit user approval to freeze. Until then every "frozen" below means
"proposed frozen content".**
**All work under this preregistration is SHADOW (research only).**
**Prior: MODERATE-LOW** (kickers and punters are among the most visible
specialists in football; the base rate of ST quality being fully priced
in totals is high. The test adjudicates strictly *incremental*
information beyond the closing market total, not whether special teams
matter for scoring.)

- Drafted: 2026-09-16
- Dataset (frozen, read-only): manifest v1, fingerprint
  `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`,
  scope `nfl-2022-2024-spread-total`, 162/162 snapshots, 291,586 paired
  spread/total quotes. Freeze verification before running: quote count =
  291,586 and distinct snapshots = 162, else the run stops. v1 is never
  edited; a scope change mints v2.
- Scope: FULL_GAME_TOTAL, NFL regular seasons 2022–2024. Postseason
  excluded (not in the frozen dataset).
- Question: does timestamp-safe *lagged special-teams efficiency*,
  expressed in total-points space, contain INCREMENTAL information about
  the game total beyond the contemporaneous closing betting-market
  total? This is not a test of raw ST–scoring correlation.
- Distinctness: this family uses only special-teams plays (kicks, punts,
  returns, FG/XP). It shares no features, no information set, and no
  endpoint with the retired pace/tempo family (H-N4) or any other
  retired family.

## Information set (frozen)

Only information available by the decision snapshot (Wednesday 12:00
UTC of the game's week):

- **ST input:** nflverse play-by-play, seasons 2021–2024, REG games only,
  plays strictly before the game's kickoff. Play identification by
  `play_type` (`field_goal`, `extra_point`, `kickoff`, `punt`) — NOT by
  the `special_teams_play` flag, which is 0 on FG plays in this nflverse
  build (verified 2026-09-16; using the flag would silently drop all
  field goals). `no_play` (penalty-negated) ST plays are excluded.
- **Market input:** the frozen totals consensuses at the decision and
  closing snapshots (defined below). No other market data enters the
  information set.
- **Disqualified, permanently:** realized scores, realized totals,
  any post-decision roster or injury move, gameday inactives, weather,
  officiating crews, and any ST metric computed from the game itself
  (no within-game updates, ever).

## Decision and closing snapshots (mechanical, game-specific)

- **Decision snapshot:** the Wednesday 12:00 UTC cadence slot of the
  game's week, for every game. The ST feature is fully determined by
  prior-week games, so one common decision point is used. No fallbacks:
  a game without an eligible decision consensus is OUT.
- **Closing snapshot:** the latest frozen cadence slot (Wed 12:00 /
  Sat 12:00 / Sun 15:30 UTC) with instant strictly before kickoff
  (kickoff = nflverse gameday+gametime parsed as America/New_York → UTC;
  same parser as the H-N2 gate). For Thursday games this is the Wed
  slot (decision = close); for London 09:30 ET games it is the Sat slot.
  No fallbacks: a game without an eligible closing consensus is OUT.
- **Consensus construction** (identical to the frozen de-vig work and
  the H-N2/H-N6 gates): per-book median Over line at the snapshot, then
  median across books = consensus line L; books "at the consensus line"
  are those within 0.01. Eligible iff ≥3 books at the line **including
  Pinnacle**. The Pinnacle requirement is never relaxed for sample size.

## Eligibility (mechanical funnel; attrition reported at every step)

1. NFL regular season 2022–2024 game in the bundled nflverse schedules.
2. Canonical identity match → ≥1 Odds event id (deterministic layer;
   `docs/nfl-identity-match-report.md`; the 2022 W17 BUF@CIN suspended
   game is `missing_from_source` and can never be eligible).
3. Week ≥ 2. Week-1 games are excluded by mechanical rule: no
   point-in-time-safe pre-decision kicker baseline exists for week 1
   under the weekly-trailing rule below (2021 data supplies the trailing
   window, but kicker availability cannot be established before any
   week-1 decision snapshot without look-ahead).
4. Eligible totals consensus at the decision (Wed) snapshot.
5. Eligible totals consensus at the closing snapshot.
6. Trailing ST data: both teams have 8 prior REG games (spanning into
   2021 as needed), of which ≥4 contain ≥1 ST play (sanity filter).
7. Kicker determinable for both teams (§Kicker treatment): unambiguous
   (≥75% of team FG+XP attempts), no in-window kicker change, and the
   primary kicker attempted ≥1 kick in the team's most recent trailing
   game.

The exact count is produced by `nfl-edge/ops/nfl_hn7_eligibility.py`
(read-only, zero credits, zero outcomes; dispatch
`nfl-hn7-eligibility.yml`). **Exact N is PENDING** until that run
completes.

## Lag structure (frozen)

- Trailing window: each team's **8 most recent REG games strictly
  before the game's kickoff**, spanning the season boundary into 2021
  as needed. Postseason and preseason never enter the window. Bye weeks
  are skipped mechanically (most-recent-games, not most-recent-weeks).
- Identical window for both teams. No within-game updates: the feature
  for game g uses only games kicked off before g.
- Shrinkage: trailing mean shrunk toward 0 with 4 pseudo-games,
  `V̄ = (8·mean)/(8+4)`. Shrinkage toward 0 is principled because every
  component is constructed as a deviation from a league baseline
  (grand mean ≈ 0 by construction).

## ST value construction (frozen; total-points space)

Per team T and game g, `V(T,g)` is the sum of the following components,
each expressed as a deviation from a league baseline so the unit is
**expected total points above/below average**:

1. **Field goals:** +3 per made FG by T (`play_type='field_goal'`,
   `field_goal_result='made'`), minus baseline `E[FG pts]` per
   team-game. Blocked FG returned for TD: +6 to the returning team
   instead (no FG points); the FP terms below are skipped for that play.
2. **Extra points:** +1 per good XP by T
   (`extra_point_result='good'`), minus baseline `E[XP pts]`.
3. **Return TDs:** +6 per TD on a kickoff/punt play to `td_team`
   (covers kick/punt return TDs and blocked-kick recovery TDs), minus
   baseline `E[return-TD pts]`. The FP term for that play is skipped
   (no double counting).
4. **Kickoff field position** (kicking team K; note nflverse sets
   `posteam` = the *receiving* team on kickoffs, so K = `defteam`):
   for each kickoff by K without a TD: opponent start
   `s = 25` on touchback (2021–2023) or `30` (2024 dynamic-kickoff
   rule), `40` on kickoff out of bounds, else
   `s = clip(100 − L − kick_distance + return_yards, 1, 99)` where
   `L = yardline_100` (the kick spot, measured from the kicking team's
   own goal — verified 2026-09-16). Contribution
   `−κ·(s − E[kickoff start])` summed over K's kickoffs.
5. **Punt field position** (punting team K = `posteam` on punts):
   for each punt by K: skip if TD, safety (+2 to the receiving team
   instead), or `punt_blocked`. Else opponent start `s = 20` on
   touchback, else `s = clip(L − kick_distance + return_yards, 1, 99)`
   where `L = yardline_100` (the line of scrimmage, measured from the
   receiving team's own goal — verified 2026-09-16). Contribution
   `−κ·(s − E[punt start])` summed over K's punts. Penalty enforcement
   spots after the play are ignored (mechanical; documented limitation).
6. **Yard-line orientation guard:** the gate asserts on touchback plays
   that the orientation identities hold (punt touchback ⇒ s=20 by rule;
   kickoff frame identity `100 − L − kick_distance` on non-touchback
   kicks cross-checked against `desc` on a fixed 20-play sample). Any
   failure is a hard error, never a silent default.

- **κ = 0.06 points/yard** (frozen literature constant: linearization of
  published expected-points models, ≈15–17 yards ≈ 1 point near
  midfield; documented approximation, never re-estimated).
- **Baselines** `E[·]`: league averages over REG games of the current
  season played strictly before game g's week; if fewer than 64
  team-games, the full prior REG season is used instead. Baselines are
  computed from the same pbp (pre-decision only), never from outcomes
  of game g.
- **Sign convention (pre-specified):** better kicking (more FG/XP
  points, return TDs) raises the expected total (+); better punting /
  kickoff coverage (deeper opponent starts) lowers the expected total
  (−). The primary feature is the **sum**, not the difference, of the
  two teams' ST values: both teams' kicking raises the total and both
  teams' punting lowers it.

## Primary ST feature (frozen; exactly one)

- `F_g = V̄_home + V̄_away`, where `V̄_T` is team T's shrunk trailing-8
  mean of `V(T,g)`.
- Pre-specified direction: higher F → higher total → **positive** slope
  on the residual (realized − market).
- Empirical feature summary (outcome-blind, computed 2026-09-16 from
  trailing ST values only, never from scores or residuals):
  per-team-game V: mean 0.00, SD 4.51; shrunk trailing-8 team mean SD
  1.15; implied game-level F SD ≈ 1.63. The frozen planning constant is
  **sd_F = 1.60**.

## Kicker-availability treatment (frozen)

From `kicker_player_name` on FG/XP plays in the trailing window only
(point-in-time-safe by construction; no roster or injury feed):

- **Identification:** the kicker is the player with the most FG+XP
  attempts for T in the trailing 8 games. Share = his attempts / T's
  total FG+XP attempts; require share ≥ 0.75, else `ambiguous_kicker`.
- **Change detection:** the kicker with the most FG+XP attempts in T's
  *most recent* trailing game must equal the window primary, else
  `kicker_change_in_window`.
- **Availability:** the primary kicker must have ≥1 FG/XP attempt in
  T's most recent trailing game, else `kicker_unknown_last_game`.
- Any of the three failures on either team → the game is OUT
  (complete-case). Missingness is reported by reason and season.
- **Documented blind spot:** a kicker injured *during* the last
  trailing game after attempting a kick is (conservatively) treated as
  available. Mid-week practice injuries after the last game are outside
  the information set by design.

## Baseline and model form (frozen)

- `L_close` = consensus totals line at the closing snapshot.
  `R` = realized total points (nflverse; sealed until the test runs).
- Target: residual `R − L_close` (the market-adjusted quantity — this
  asks whether lagged ST efficiency adds information the closing total
  has not incorporated).
- Per season-fold (see below), on training folds fit by OLS:
  - **Baseline:** `R − L_close ~ season FE + close-slot FE`
    (slot ∈ {wed, sat, sun} absorbs which cadence slot served as close).
  - **Augmented:** baseline `+ F`, with `F` centered at the
    **training-fold** mean.
- Per-game OOS loss difference on the held-out season:
  `d_i = (e_baseline,i)² − (e_augmented,i)²` (positive = ST helps).

## Primary statistic (frozen)

- 3-fold leave-one-season-out: {train 2023+2024 → test 2022},
  {train 2022+2024 → test 2023}, {train 2022+2023 → test 2024}.
- Pool all held-out games (each game appears exactly once as held-out).
  **One-sided t-test of mean(d) > 0, α = 0.05, SEs clustered by
  season-week** (54 clusters; absorbs within-week correlation from
  shared weather/scheduling).
- **Directional gate:** pooled OLS coefficient on `F` (same spec, all
  data) must be **> 0** AND fold-level mean(d) > 0 in **≥ 2 of 3 folds**.
  A significant improvement with the wrong sign is a FAIL on mechanism.

## Season folds / clustering

- Folds as above; ST features use only pre-kickoff plays
  (no test-season or post-kickoff information anywhere).
- Clustering unit: season-week (3 seasons × 18 weeks = 54 clusters).

## Economic endpoint — Stage 2 (frozen, secondary)

- ST-implied residual `r̂ = β̂_train·(F − F̄_train)`, β̂ from training
  folds only, applied to the held-out fold.
- **Betting rule:** Over 1u iff `r̂ ≥ +1.5`; Under 1u iff `r̂ ≤ −1.5`
  (ST implies ≥1.5 pts of value vs the close — a pre-specified
  meaningful move); else no bet.
- **Execution:** enter at the decision (Wed) consensus line, at the best
  available price (max payout per unit; ties → alphabetical book_key)
  among consensus books quoting the chosen side at the consensus line;
  if none, no bet (reported). Settle vs R at the taken price; pushes
  return stake.
- Report mean P&L per eligible game with 95% CI (season-week-clustered).
- **Practical bar (pre-declared): +0.005u/game** (H-N2/H-N6 precedent).
  Statistical significance alone does not promote H-N7.

## Exclusions / missing-data treatment

- Complete-case at the game level. Every attrition step reported:
  bundle games → identity-matched → week ≥ 2 → decision consensus →
  closing consensus → trailing ST data → kicker determinable →
  eligible.
- No imputation of lines, ST values, kickers, or baselines. Pushes on
  `L_close` contribute d_i mechanically in the primary statistic and
  settle as stake-returned in the economic endpoint.

## Power / MDE and the feasibility rule (frozen)

- Planning MDE for the slope (one-sided α=0.05, power=0.8):
  `MDE = 2.4865 · 13.5 / (1.60 · √N)` pts per ST-point, with σ_res =
  13.5 pts (planning constant from the known residual scale of NFL
  totals, same as H-N2; not re-estimated from outcomes) and sd_F = 1.60
  pts (frozen planning assumption from the outcome-blind feature
  summary above).
- At N=290: MDE ≈ **1.23**. Full pass-through of trailing ST value is
  β=1; the test is therefore powered only for near-complete
  non-pricing of ST — a deliberately hard bar, because kickers and
  punters are highly visible and the plausible alternative is partial
  mispricing at best.
- **MDE cap = 0.50** (frozen): the minimum interesting effect is half
  pass-through (a 2-pt trailing ST edge leaving 1 pt unpriced ≈ a real,
  bettable edge). A smaller mispricing, while possibly real, is
  declared out of scope: it is not distinguishable from noise at any
  feasible N on this dataset.
- **Feasibility rule (frozen):** the eligibility gate returns exact N
  and the empirical feature SD.
  - N < 200 → **INFEASIBLE**: retire without running.
  - MDE at exact N (frozen planning sd_F=1.60) > 0.50 → **RETIRE-ON-POWER**:
    retire without outcome exposure.
  - Else → PROCEED-TO-FREEZE (freeze still requires explicit user
    approval; approval does not re-open any frozen content).
- **Discordance governance:** the empirical sd_F is an outcome-blind
  sensitivity diagnostic, never silently substituted for the frozen
  planning constant. If empirical sd_F differs from 1.60 by more than
  25%, the DRAFT is amended pre-freeze recording the change and date
  (a design correction consuming zero statistical capital), and the
  verdict is recomputed — never cherry-picked toward PROCEED.

## Exact pass / fail / retirement rule

- **PASS:** OOS mean(d) > 0 significant one-sided at α=0.05 AND pooled
  β̂ > 0 AND fold-level mean(d) > 0 in ≥2/3 folds AND economic endpoint
  ≥ +0.005u/game. → H-N7 advances to prospective confirmation (no
  further historical testing).
- **INCONCLUSIVE (parked, not failed):** statistically significant but
  economic endpoint < +0.005u/game.
- **FAIL (retire):** no significant OOS improvement, or significant with
  wrong sign, or positive in <2 folds. Family retired permanently under
  this specification. No variants: no nearby lags (4/6/12 games), no
  κ re-tuning, no component re-weighting, no per-component features,
  no kicker-only reframing, no alternate endpoints presented as
  independent confirmation.
- **INFEASIBLE / RETIRE-ON-POWER:** per the feasibility rule above;
  no test run, no outcome exposure.

## Power caveat (pre-registered honestly)

- The slope is identified off between-team ST quality differences that
  persist over 8-game windows; measurement noise in the trailing mean
  attenuates the true pass-through, so the MDE above is mildly
  optimistic about the detectable *structural* mispricing.
- Week-clustered inference with 54 clusters is honest about correlated
  scheduling/weather but costs power vs the iid calculation.
- A null retires the family; it does not prove special teams are fully
  priced — only that no incremental edge of the pre-specified minimum
  size is detectable on this dataset.

## Outcome-blindness (standing implementation integrity requirement)

- The eligibility gate may inspect ONLY non-outcome quantities:
  eligible N, the ST feature distribution (V, V̄, F summaries),
  coverage/missingness (kicker availability, nflverse ST coverage,
  snapshot availability), and the frozen planning constants.
- NEVER: actual scores, realized totals, realized residuals, empirical
  residual variance, Stage 1 coefficients, or anything derived from
  game outcomes. The gate script selects only non-score pbp columns and
  never reads `total_home_score`, `total_away_score`, `spread_line`,
  `total_line`, or any score-derived column.
- The gate certifies `outcome_rows_read = 0` alongside N, feature SD,
  MDE, and attrition. Any outcome read aborts the run.

## What is frozen in this draft vs what remains pending

**Proposed frozen (binding only after user-approved freeze):**
hypothesis, information set, play_type-based ST identification (not the
`special_teams_play` flag), decision snapshot (Wed 12:00 UTC, no
fallbacks), closing snapshot (latest pre-kickoff slot, no fallbacks),
consensus construction (≥3 books incl. Pinnacle, never relaxed),
week≥2 exclusion, trailing-8-REG-game lag with cross-season spanning,
shrinkage (k=4 toward 0), the six-component V(T,g) construction with
κ=0.06 and the touchback/out-of-bounds spots, baseline rule (64
team-game fallback to prior season), kicker identification/change/
availability rule (75% share), the single summed feature F with
pre-specified positive direction, baseline/augmented model forms,
LOSO OOS primary statistic with season-week clustering and the ≥2/3-fold
sign gate, economic endpoint and the +0.005u/game bar, N floor 200,
MDE cap 0.50 with planning sd_F=1.60, discordance governance, and the
full pass/fail/retirement rule.

**Pending:**
1. Exact eligible N and empirical sd_F — produced by
   `nfl-edge/ops/nfl_hn7_eligibility.py` via `nfl-hn7-eligibility.yml`
   (read-only, zero credits, zero outcomes). Status: **PENDING**.
2. User approval to freeze. The freeze changes nothing in the text
   above; it only flips this document's status.

## Statistical capital

Zero consumed to date (all six prior families retired on feasibility or
analysis consumed none). If frozen and run, H-N7 spends the first
family-wise slot (α=0.05, Holm across active families).

## Files

- `nfl-edge/ops/nfl_hn7_eligibility.py` — eligible-N gate (read-only)
- `nfl-edge/tests/test_nfl_hn7_eligibility.py` — unit tests
- `.github/workflows/nfl-hn7-eligibility.yml` — dispatch workflow

*DRAFT 2026-09-16. Not frozen. No outcomes inspected. No test run.*
