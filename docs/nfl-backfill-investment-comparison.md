# NFL backfill investment comparison

**Date:** 2026-09-15. **Status:** ANALYSIS ONLY — zero API credits spent.
**Inputs:** `docs/nfl-research-lane-inventory.md` only (plus free, public
facts: NFL season structure). No hypothesis was tested, no model was fit,
nothing was preregistered. Frozen NFL v1.3, the forward shadow experiment,
and frozen dataset `43f853a44bf93937d85149ca5fd7241b` were not touched.

**Question answered:** which of the six hypothesis sketches earn a paid
historical market pull, and what is the minimum-credit pull each needs?
The user will **not** approve a blanket ~16k-credit pull.

## Pricing reference (verified in the inventory, §B)

- Historical per-event endpoint: **10 credits / region / market /
  timestamp** (one event per request).
- `/v4/sports` and `/v4/events` (event-ID enumeration): **free**.
- Alt/prop/period markets: **no history before 2023-05-03** → at most
  2023–2024 (2 seasons).
- Featured markets (`h2h`, `spreads`, `totals`): history from 2020-06-06
  → 2022–2024 (3 seasons) available.
- Event counts (standard NFL structure; verify at implementation):
  272 regular-season games/season → **544** (2023–2024), **816**
  (2022–2024). Playoffs (+13/season) excluded from all pulls below
  unless noted.

All estimates are **us region only** (EU region would double cost and is
not recommended for the initial pull).

## Per-family comparison

| Family | Verdict | Min credits (us) | Events × Markets × Ts |
|---|---|---|---|
| H-N6 officiating-crew tendency vs totals | **EARN (#1)** | **8,160** | 816 × `totals` × 1 |
| H-N3 rest/travel fatigue in 1H derivatives | **EARN (#2)** | **~4,500** | ~150 filtered × (`spreads`,`spreads_h1`,`totals_h1`) × 1 |
| H-N1 injury-report shock in correlated props | FAIL | 3,000 (filtered) – 32,640 (full) | 50–544 × (`team_totals`,`player_pass_yds`,`player_reception_yds`) × 2 |
| H-N2 forecast wind vs totals | BLOCKED | ~14,000 if unblocked | ~350 outdoor × (`totals`,`totals_h1`) × 2 |
| H-N4 lagged tempo vs 1H totals | FAIL | 10,880 | 544 × (`totals`,`totals_h1`) × 1 |
| H-N5 alt-line key-number consistency | FAIL | 16,320 – 32,640 | 544 × (`spreads`,`alternate_spreads`,`alternate_totals`) × 1–2 |

### H-N1. Injury-report shock in correlated player props — FAIL

- **Mechanism:** Books reprice quickly on QB news; any residual edge
  would have to live in the cross-market adjustment (prop totals lagging
  the headline move) against books' automated repricing. Plausible in
  principle, but it requires the book to be simultaneously fast and
  slow on the same news.
- **Sample:** 2023–2024 only (props have no 2022 history). Injury-filtered
  from free nflverse injuries, genuine shocks are rare — on the order of
  50 QB Out/Doubtful events over 2 seasons. Below one-shot test power.
- **Exact markets:** `team_totals`, `player_pass_yds`,
  `player_reception_yds`.
- **Credits:** 2 timestamps per event are required (pre-news baseline
  vs post-news; the endpoint is prop CLV). Full: 544 × 3 × 2 × 10 =
  32,640. Injury-filtered (~50 events): 3,000.
- **Decision unlocked:** whether prop markets show residual mispricing
  after injury news beyond the full-game total move — but the prereg
  must first define "shock" mechanically from free data, which itself
  narrows the sample further.
- **Why it fails:** thinnest book consensus (3–5 books for props), wide
  prop vig erodes any residual, and the filtered sample is too small
  for a single decisive test even at the 3,000-credit level.

### H-N2. Forecast wind vs totals in outdoor stadiums — BLOCKED

- **Mechanism:** Wind degrades passing efficiency and kicking; the most
  cited weather edge and therefore the most likely already priced.
- **Sample:** blocked — no verified point-in-time forecast archive
  exists. The weather spike must pass first.
- **If unblocked:** `totals` + `totals_h1`, outdoor stadiums only (~65%
  of 544 ≈ 350 events), 2 timestamps (forecast-as-of ≥12h before kickoff
  + near-close). 350 × 2 × 2 × 10 = ~14,000.
- **Decision unlocked:** whether a wind-adjusted total beats the market
  total OOS on high-wind games.
- **Why no spend now:** blocked on the spike by standing rule. If the
  spike fails, H-N2 is dropped — never approximated with realized
  weather (leakage).

### H-N3. Rest/travel fatigue in 1H derivatives — EARN (#2)

- **Mechanism:** Rest differentials (short week, post-bye,
  international travel) are known before the season — a leak-proof
  information set. Any unpriced remainder plausibly lives in
  less-scrutinized 1H derivatives rather than the efficient full-game
  line.
- **Sample:** 2023–2024 only for 1H markets. Pull is **filtered to games
  with material rest differential** (e.g. ≥3 days) — the exact count is
  computable **free** from nflverse schedules before any pull;
  estimate ~100–150 games over 2 seasons (~150 used below).
- **Exact markets:** `spreads`, `spreads_h1`, `totals_h1`. The full-game
  spread is the required control (this lane does not reuse the frozen
  dataset). No other markets.
- **Credits:** 150 × 3 × 1 × 10 = **~4,500** (1 timestamp per event —
  the information is preseason-known, so intra-week timing is not
  load-bearing). Price exactly once the free schedule filter returns the
  true event count.
- **Decision unlocked:** one-shot test of a stable rest-differential
  coefficient in 1H lines after full-game controls; 2023→2024
  walk-forward only (2 seasons cannot do 3-fold LOSO).
- **Caveats:** small treatment sample; 1H book consensus is 3–5 books;
  prereg must define the differential threshold from free data first.

### H-N4. Lagged tempo vs 1H totals — FAIL

- **Mechanism:** Neutral-script pace from strictly lagged play-by-play.
  Pace is publicly modeled everywhere (nflverse columns, public
  models), so the market 1H total most likely already spans it.
- **Sample:** 2023–2024 (544 games; tempo is continuous, so no
  filtering is possible).
- **Exact markets:** `totals`, `totals_h1` (full-game total required
  for the encompassing test).
- **Credits:** 544 × 2 × 1 × 10 = 10,880.
- **Decision unlocked:** OOS log-loss of a tempo-based 1H total vs the
  market 1H total.
- **Why it fails:** the inventory's own mechanism note says pace is
  "heavily modeled publicly" — the weakest justification relative to
  cost. At 10,880 credits for 2 seasons, the cost/mechanism ratio is
  worse than H-N3 on both dimensions.

### H-N5. Alternate-line internal consistency (key numbers 3/7) — FAIL

- **Mechanism:** Alt lines are algorithmically generated from the same
  distribution as the main line; a systematic deviation would be a
  pricing bug, not a persistent edge. The weakest mechanism of the six —
  there is no information edge at all, only a check against the
  market's own model.
- **Sample:** 2023–2024 only (alt markets).
- **Exact markets:** `spreads`, `alternate_spreads`,
  `alternate_totals`.
- **Credits:** CLV vs own close needs 2 timestamps: 544 × 3 × 2 × 10 =
  32,640. A 1-timestamp no-arb-band check: 16,320.
- **Decision unlocked:** whether alt-line implied probabilities deviate
  from the no-arbitrage band after vig and key-number mass modeling.
- **Why it fails:** highest cost tier, weakest mechanism, no
  information set — a bug hunt at 16k–33k credits.

### H-N6. Officiating-crew penalty tendency vs totals — EARN (#1)

- **Mechanism:** Crew assignments are published pregame (SAFE); crew
  *tendencies* (flags/game, automatic-first-down rate) computed from
  strictly lagged seasons are a genuinely orthogonal, low-salience
  information set the totals market may not fully price.
- **Sample:** **2022–2024, all 3 seasons** — `totals` is a featured
  market (816 regular-season games). The largest sample of any family
  and the only one that supports 3-fold leave-one-season-out.
- **Exact markets:** `totals`. One market, nothing else.
- **Credits:** 816 × 1 × 1 × 10 = **8,160** (1 pregame timestamp per
  event; the endpoint is residual correlation of realized total vs
  market total, and realized scores are free from nflverse).
- **Free data:** officials, schedules, realized scores — all free via
  nflverse; tendency computation costs nothing.
- **Decision unlocked:** one-shot test of a stable crew-effect
  coefficient across 2022/2023/2024 folds with multi-season shrinkage
  in the prereg.
- **Caveats:** crew effects are small by construction; the prereg must
  include shrinkage and a pre-specified effect-size gate, or a null
  result is uninformative.

## Recommendation

**Earn a paid backfill: H-N6 (~8,160 credits) and H-N3 (~4,500 credits),
total ≈ 12,660 — nothing else.**

Sequence before any pull:

1. Compute exact event counts from **free** data first: nflverse
   schedules → exact rest-differential game count for H-N3; nflverse
   officials → confirm crew coverage 2022–2024 for H-N6.
2. Write full preregistrations in `docs/preregistrations/` for both
   families (information set, as-of rules, market, endpoint,
   falsification, multi-season shrinkage for H-N6, differential
   threshold for H-N3). Test each **once**.
3. Approve only the minimum credits: 8,160 + the exactly-priced H-N3
   pull (~4,500).

**Do not fund:** H-N1 (thin consensus, wide vig, underpowered sample),
H-N4 (publicly modeled information at 2.4× the cost of H-N3), H-N5
(pricing-bug hunt at the highest cost), H-N2 (blocked on the weather
spike — revisit only if the spike verifies an archive).

*Analysis-only. Zero API credits spent. No picks, no model changes, no
forward-shadow contact.*
