# NFL H-N6 / H-N3 feasibility summary (free pass, 2026-09-15)

**Zero API credits spent. Zero outcomes inspected.** Coverage, counts,
timing, and availability only. Realized totals, 1H margins, and P&L stay
sealed until the user authorizes a test.

Draft preregistrations: [nfl-h-n6-officiating-crew-totals-DRAFT](preregistrations/nfl-h-n6-officiating-crew-totals-DRAFT.md),
[nfl-h-n3-rest-travel-h1-DRAFT](preregistrations/nfl-h-n3-rest-travel-h1-DRAFT.md).
Both are DRAFT — frozen only by user approval.

## H-N6 (officiating-crew tendency vs totals) — SURVIVE, $0-credit test

**Zero-credit verdict: CONFIRMED.** The 8,160-credit totals pull is
unnecessary.

- **Timing:** NFL officiating assignments are publicly knowable by
  Tuesday of game week (Football Zebras Tuesday-morning assignment
  posts; e.g. Week 1 2026 assignments published Tue 2026-09-08 ahead of
  the Sun 2026-09-13 slate). The frozen dataset's cadence — Wed 12:00
  UTC, Sat 12:00 UTC, Sun 15:30 UTC — puts **every one of the 162
  snapshots strictly after the Tuesday announcement and strictly before
  that week's earliest kickoff** (Thu 20:15 ET). The decision window was
  defined from the information set (post-crew-announcement), not fitted
  to convenient snapshots; the snapshots happen to satisfy it.
- **Coverage:** nflverse officials give exactly one referee per game for
  272/272 regular-season games in each of 2022, 2023, 2024 (17 crews per
  season; zero multi-referee anomalies). Lagged seasons 2015–2021 are
  available for tendency estimation. Postseason excluded (all-star
  crews).
- **Decision snapshot (mechanical):** Wed 12:00 UTC of the game's week;
  fallback Sat then Sun with missingness reported. A read-only coverage
  gate at implementation counts games with ≥1 post-announcement totals
  quote; any missing game is listed and the minimum missing-data pull is
  sized at 10 cr × 1 region × 1 market × 1 ts × (games missing).
  Expected ≈ 0 given 162/162 snapshots received.
- **Design guardrails:** the prereg tests INCREMENTAL information beyond
  a market/team baseline (consensus totals line + team/season/week/roof
  controls), not raw referee-total correlation. Feature = lagged,
  shrunk penalties-per-game (mechanism: more flags → extended drives →
  more points), directional sign pre-specified. 3-fold leave-one-season-out.
- **Power caveat:** the coefficient is identified off 17 referee
  clusters — cluster-robust inference with 17 clusters has limited power
  for small effects. Fold-consistency (positive in ≥2 of 3 folds) and
  shrinkage are guardrails. A null retires the family; it does not prove
  crews don't matter.
- **Exact eligible count:** (frozen-dataset events) ∩ (272×3 games with
  crews), computed read-only at implementation after NFL identity
  validation. Expected ≥ 790.
- **Credits authorized: 0.**

## H-N3 (rest/travel → 1H derivatives) — RETIRE WITHOUT SPENDING

**The free count did not collapse — the power analysis did.**

- **Eligibility definition (frozen without outcome peeking):**
  |rest_home − rest_away| ≥ 3 days, rest = within-season calendar days
  between consecutive kickoffs (nflverse schedules). 3 days = smallest
  threshold mapping to real scheduling structure (mini-bye 10 vs 7;
  short-week 4 vs 7).
- **Raw eligible, 2023–2024 (H1 market frame): 121 games** (56 in 2023,
  65 in 2024). Differential distribution: ±3 (65), ±7 (37), ±4/±6/±8
  (19). SD of differential in eligible sample: 4.97 days.
  (2022 adds 57 more eligible games, but H1 markets have no 2022
  provider history — unusable.)
- **Intersection attrition:**
  1. Eligible rest differential: **121**
  2. × deterministic identity: **pending** NFL identity validation
     (NCAAF precedent ~97.8% → ~118 expected)
  3. × H1 market availability (`spreads_h1` + `totals_h1` at the
     decision timestamp): **unknown until pull** — scenarios 50/70/85%
  4. × valid pregame snapshot: folded into (3) — one timestamp per game
     (nearest 5-min snapshot to 24h before kickoff)
  5. × outcome availability (realized 1H margins, nflverse PBP):
     **~100%** (availability known; values sealed)
  - **Final N scenarios: 59–100 pooled; 2024 test set ≈ 30–51.**
- **Minimum detectable effect** (80% power, α=0.05, residual SD σ=9,
  s_d=4.97): MDE ≈ 2.8·σ/(s_d·√N).
  - Test set N=30→**0.93**, N=42→**0.78**, N=51→**0.71** pts/day.
  - Pooled N=59→0.66, N=83→0.56, N=100→0.51 pts/day.
- **Plausibility gap:** a 7-day differential (bye vs normal week) at
  MDE 0.6–0.9/day implies **4–6 points of first-half margin** — 3–6× the
  plausible mechanism magnitude (published full-game rest effects are
  ~1–2 points total, i.e. ≤0.3 pts/day; the 1H share is smaller). The
  design cannot detect realistic effects at any attainable N in the
  2023–2024 H1 window, before or after H1-availability attrition.
- **Verdict: RETIRE without spending.** The pull, if it proceeded, would
  cost exactly **3,630 credits** (121 × 3 markets × 1 ts × 10, us
  region) — below the ~4,500 estimate but still unjustified.
- Possible redesign (not authorized): continuous treatment on all games
  instead of the |d|≥3 filter — a new prereg, not a variant of this one.

## Decision tree

| Family | Gate result | Next step |
|---|---|---|
| H-N6 | Existing data sufficient | User freezes prereg → identity validation → run $0-credit test |
| H-N3 | Insufficient final N / detectable effect | **Retire, no spend** |

**Sequencing implication:** H-N6 first was the right order, and it now
costs nothing. The ~12,660 ceiling is not reached: H-N3's ~3,630 is
declined on power grounds. If H-N6 gives a useful answer about whether
richer NFL information carries incremental market information, the lane
has its signal on what to do next.

## Data exposure record (standing rule)

- nflverse `load_schedules()` 2022–2024 (local bundle
  `nfl-edge/ops/nflverse_schedules_2022_2024.json`; date/team fields
  only — no scores read).
- nflverse `load_officials()` 2015–2026 (crew identity/coverage counts
  only — no outcomes).
- Two web searches (crew-announcement timing). Zero Odds API calls.

*Feasibility only. No picks, no model changes, no forward-shadow contact.*
