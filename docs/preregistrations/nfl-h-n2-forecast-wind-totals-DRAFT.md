# Preregistration (DRAFT — NOT FROZEN): nfl-h-n2-forecast-wind-totals

**Status: DRAFT 2026-09-16. Not frozen. No outcomes inspected; no test
run; no statistical capital spent. This document becomes binding only
after (a) the exact eligible-N gate returns PROCEED-TO-FREEZE and (b)
explicit user approval to freeze. Until then every "frozen" below means
"proposed frozen content".**
**All work under this preregistration is SHADOW (research only).**
**Prior: MODERATE-LOW** (wind effects on scoring are real physics, but
this is the most-cited weather edge in betting literature — the base rate
of it being fully priced is high; the test adjudicates strictly
*incremental* information, not whether wind matters).

- Drafted: 2026-09-16
- Dataset (frozen, read-only): manifest v1, fingerprint
  `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`,
  scope `nfl-2022-2024-spread-total`, 162/162 snapshots, 291,586 paired
  spread/total quotes. Freeze verification before running: quote count =
  291,586 and distinct snapshots = 162, else the run stops. v1 is never
  edited; a scope change mints v2.
- Scope: FULL_GAME_TOTAL, NFL regular seasons 2022–2024. Postseason
  excluded (not in the frozen dataset).
- Question: does timestamp-safe *forecast* wind contain INCREMENTAL
  information about the game total beyond the contemporaneous
  betting-market total? This is not a test of raw wind–scoring
  correlation.

## Information set (frozen)

Only information available by the prediction cutoff (kickoff − 24h):

- **Weather input:** IEM MOS GFS archive, mechanically the latest cycle
  with `runtime + 4h ≤ kickoff − 24h` (frozen archive rule v1; 4h =
  assumed GFS MOS dissemination lag; timestamps only, never chosen by
  fit). Cycles live on the 6-hour GFS MOS grid.
- **Disqualified, permanently:** realized weather, reanalysis (ERA5),
  current-only APIs, later forecast revisions, retrospective cycle
  re-selection, alternate weather-source shopping (HRRR is a future
  upgrade path, never a retrospective comparator).
- **Market input:** the frozen totals consensus at the decision snapshot
  (defined below). No other market data enters the information set.

## Decision window (mechanical, game-specific)

- Prediction cutoff = kickoff − 24h (kickoff = nflverse gameday+gametime
  parsed as America/New_York → UTC).
- **Decision snapshot per game:** the latest frozen cadence snapshot
  (Wed 12:00 UTC / Sat 12:00 UTC / Sun 15:30 UTC of the game's week)
  with snapshot instant **strictly before** kickoff − 24h. Examples: a
  Sunday 13:00 ET kickoff → Sat 12:00 UTC; Thursday 20:15 ET → Wed
  12:00 UTC; Monday 20:15 ET → Sun 15:30 UTC.
- **Consensus construction** (identical to the frozen de-vig work and the
  H-N6 gate): per-book median Over line at the decision snapshot, then
  median across books = consensus line L; books "at the consensus line"
  are those within 0.01. Eligible iff ≥3 books at the line **including
  Pinnacle**.
- **No fallbacks.** Unlike H-N6, there is no Wed→Sat→Sun fallback chain:
  a game without an eligible consensus at its own decision snapshot is
  OUT. Missingness is reported by season; the window is never widened to
  absorb gaps.

## Eligibility (mechanical funnel; attrition reported at every step)

1. NFL regular season 2022–2024 game in the bundled nflverse schedules.
2. Canonical identity match → ≥1 Odds event id (deterministic layer;
   `docs/nfl-identity-match-report.md`; the 2022 W17 BUF@CIN suspended
   game is `missing_from_source` and can never be eligible).
3. Stadium resolves via `nfl_stadium_mos.json` (+ 2022–2024 name aliases
   in `nfl_stadium_aliases_2022_2024.json`). Unresolvable →
   `unmapped_stadium`, excluded (fail loud, never guessed).
4. Venue classification INCLUDE (structural rule, frozen table):
   - **Include:** `outdoor`, `canopy_open_air` (SoFi Stadium: fixed
     translucent canopy with fully open sides — pre-specified judgment
     that wind reaches the field; documented here, not revisited).
   - **Exclude:** `dome` (Allegiant, Caesars Superdome, Ford Field,
     U.S. Bank), `retractable` (AT&T, Lucas Oil, Mercedes-Benz/ATL,
     NRG/Reliant, State Farm). Retractable-roof venues are excluded
     **wholesale by structural rule**: gameday roof position is
     post-cutoff information and can never be used to include them.
   - **Exclude:** `unmatched` — international venues with no NWS MOS
     coverage (2022–2024: Allianz Arena, Deutsche Bank Park, Wembley,
     Tottenham Stadium, Azteca Stadium, Arena Corinthians).
   - Unknown roof class → hard error, never defaulted.
5. Eligible totals consensus at the decision snapshot (§Decision window).
6. MOS availability: the mechanically selected cycle is retrievable from
   the IEM archive with a numeric `wsp` at the ftime nearest kickoff
   (failures/absences are explicit missingness, never backfilled).

The exact count is produced by `nfl-edge/ops/nfl_hn2_eligibility.py`
(read-only, zero credits, zero outcomes; dispatch
`nfl-hn2-eligibility.yml`). **Exact N is PENDING** until that run
completes. Planning estimate ≈ 400–450.

## Primary wind feature (frozen; exactly one)

- `w` = sustained wind speed in **statute mph** =
  (IEM MOS GFS `wsp`, knots, at the selected cycle's ftime row nearest
  kickoff) × 1.15078.
- Selected cycle = max 6h-grid runtime with `runtime + 4h ≤ kickoff − 24h`.
- ftime selection: among the cycle's ftime rows, the valid time nearest
  kickoff (UTC); tie → earlier ftime. `wsp` must be present and numeric.
- **Direction (wdr), gusts, temperature, precipitation, thresholds,
  interactions, and stadium subsets are NOT part of the primary
  specification** and must not become post-result rescue variants.
- Pre-specified direction: higher forecast wind → lower scoring →
  **negative** slope on the residual (realized − market).

## Baseline and model form (frozen)

- L = consensus totals line at the decision snapshot. R = realized total
  points (nflverse; sealed until the test runs).
- Target: residual `R − L` (the market-adjusted quantity — this asks
  whether wind adds information the market total has not incorporated).
- Per season-fold (see below), on training folds fit by OLS:
  - **Baseline:** `R − L ~ season FE + decision-slot FE`
    (slot ∈ {wed, sat, sun} absorbs consensus staleness).
  - **Augmented:** baseline `+ w`, with `w` centered at the
    **training-fold** mean.
  - No stadium/team fixed effects: they would absorb the between-venue
    wind variation that is legitimate identifying variation (wind is
    exogenous weather, not an assigned treatment).
- Per-game OOS loss difference on the held-out season:
  `d_i = (e_baseline,i)² − (e_augmented,i)²` (positive = wind helps).

## Primary statistic (frozen)

- 3-fold leave-one-season-out: {train 2023+2024 → test 2022},
  {train 2022+2024 → test 2023}, {train 2022+2023 → test 2024}.
- Pool all held-out games (each game appears exactly once as held-out).
  **One-sided t-test of mean(d) > 0, α = 0.05, SEs clustered by
  season-week** (54 clusters; absorbs within-week weather correlation
  across games).
- **Directional gate:** pooled OLS coefficient on `w` (same spec, all
  data) must be **< 0** AND fold-level mean(d) > 0 in **≥ 2 of 3 folds**.
  A significant improvement with the wrong sign is a FAIL on mechanism.

## Season folds / clustering

- Folds as above; wind features use only pre-cutoff forecast cycles
  (no test-season or post-cutoff information anywhere).
- Clustering unit: season-week (3 seasons × 18 weeks = 54 clusters).

## Economic endpoint — Stage 2 (frozen, secondary)

- Wind-adjusted total `L* = L + β̂_train·(w − w̄_train)`, β̂ from
  training folds only, applied to the held-out fold.
- **Betting rule:** Under 1u iff `L* ≤ L − 1.5` (wind implies ≥1.5 pts
  of downward adjustment — a pre-specified meaningful line move).
- **Execution:** enter at the best available Under price (max payout per
  unit; ties → alphabetical book_key) among consensus books quoting the
  Under at the consensus line at the decision snapshot; if none, no bet
  (reported). Settle vs R at the taken price; pushes return stake.
- Report mean per-game P&L with 95% CI (season-week-clustered).
- **Practical bar (pre-declared): +0.005u/game** (H-N6 precedent).
  Statistical significance alone does not promote H-N2.

## Exclusions / missing-data treatment

- Complete-case at the game level. Every attrition step reported:
  bundle games → identity-matched → venue resolved → venue included →
  decision consensus → MOS available → eligible.
- No imputation of venues, lines, cycles, or wind values. Pushes on L
  contribute d_i mechanically (no special-casing) in the primary
  statistic and settle as stake-returned in the economic endpoint.

## Power / MDE and the feasibility rule (frozen)

- Planning MDE for the slope (one-sided α=0.05, power=0.8):
  `MDE = 2.4865 · 13.5 / (5.0 · √N)` pts/mph, with σ_res = 13.5 pts
  (planning constant from the known residual scale of NFL totals; not
  re-estimated from outcomes) and sd_w = 5 mph (planning assumption;
  replaced at freeze by the observed feature SD — a feature summary,
  not an outcome).
- At N=425: MDE ≈ **0.33 pts/mph**. A 15-mph forecast (≈7 mph above a
  ~8 mph mean) then implies ~2.3 pts — inside the published wind-effect
  range (1–4 pts for high-wind games) but at its upper end: **the test
  is powered for large effects only.**
- **Feasibility rule (frozen):** the eligibility gate returns exact N.
  - N < 250 → **INFEASIBLE**: retire without running.
  - MDE at exact N > 0.40 pts/mph → **RETIRE-ON-POWER**: retire without
    outcome exposure (same standard as H-N3/H-N6).
  - Else → PROCEED-TO-FREEZE (freeze still requires explicit user
    approval; approval does not re-open any frozen content).

## Exact pass / fail / retirement rule

- **PASS:** OOS mean(d) > 0 significant one-sided at α=0.05 AND pooled
  β̂ < 0 AND fold-level mean(d) > 0 in ≥2/3 folds AND economic endpoint
  ≥ +0.005u/game. → H-N2 advances to prospective confirmation on the
  accumulating 2026 MOS archive (no further historical testing).
- **INCONCLUSIVE (parked, not failed):** statistically significant but
  economic endpoint < +0.005u/game.
- **FAIL (retire):** no significant OOS improvement, or significant with
  wrong sign, or positive in <2 folds. Family retired permanently under
  this specification. No variants: no nearby wind cutoffs, no cycle
  re-selection, no other weather variables, no stadium subsets, no
  alternate endpoints presented as independent confirmation.
- **INFEASIBLE / RETIRE-ON-POWER:** per the feasibility rule above;
  no test run, no outcome exposure.

## Power caveat (pre-registered honestly)

- The slope is identified substantially off between-venue climate
  variation (Buffalo vs Miami) as well as within-venue game-to-game
  variation; both are legitimate under the exogeneity of weather, but a
  null result cannot distinguish "wind is fully priced" from "wind was
  never forecastable-useful at this horizon".
- Week-clustered inference with 54 clusters is honest about correlated
  weather systems but costs power vs the iid calculation; the MDE above
  is therefore mildly optimistic.
- A null retires the family; it does not prove wind doesn't matter.

## What is frozen in this draft vs what remains pending

**Proposed frozen (binding only after user-approved freeze):**
hypothesis, information set, MOS selection rule (`runtime + 4h ≤
kickoff − 24h`, max eligible runtime), venue classification table,
decision window (latest pre-cutoff slot, no fallbacks), consensus
construction, the single wind feature `w` (wsp knots → mph, nearest
ftime, tie-break), model form, baseline, controls, pre-specified
negative direction, LOSO OOS primary statistic with season-week
clustering and the ≥2/3-fold sign gate, economic endpoint and the
+0.005u/game bar, N floor 250, MDE cap 0.40, and the full
pass/fail/retirement rule.

**Pending:**
1. Exact eligible N — produced by `nfl-edge/ops/nfl_hn2_eligibility.py`
   via `nfl-hn2-eligibility.yml` (read-only, zero credits, zero
   outcomes). Status: **PENDING** (no DB access in the drafting
   environment; the script runs end-to-end on the runner).
2. User approval to freeze. The freeze changes nothing in the text
   above; it only flips this document's status.

## Statistical capital

Zero consumed to date (H-N1/H-N3/H-N4/H-N5/H-N6 retired on feasibility
or analysis consumed none). If frozen and run, H-N2 spends the first
family-wise slot (α=0.05, Holm across active families).

## Files

- `nfl-edge/ops/nfl_hn2_eligibility.py` — eligible-N gate (read-only)
- `nfl-edge/ops/nfl_stadium_aliases_2022_2024.json` — 2022–2024 stadium
  name → venue_key aliases + explicit unmatched internationals
- `nfl-edge/tests/test_nfl_hn2_eligibility.py` — 26 unit tests
- `.github/workflows/nfl-hn2-eligibility.yml` — dispatch workflow
- `nfl-edge/ops/nfl_stadium_mos.json` — stadium→MOS mapping (archive
  agent; 30 US venues verified 2026-09-16, 8 international unmatched)

*DRAFT 2026-09-16. Not frozen. No outcomes inspected. No test run.*
