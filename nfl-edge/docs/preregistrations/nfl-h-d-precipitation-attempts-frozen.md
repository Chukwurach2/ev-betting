# Preregistration (FROZEN): nfl-h-d-precipitation-attempts

**Status: FROZEN 2026-09-16T17:45:00Z. Binding.**
**Frozen by explicit user authorization 2026-09-16 13:33 EDT** ("Proceed
with lane D only... freeze the preregistration and execute the minimum
viable historical pull for D under a hard 1,200-credit maximum").
**All work under this preregistration is SHADOW (research only).**
**Prior: MODERATE-LOW** (heavy-rain → run-heavy play mix is a
widely-cited weather edge; the base rate of it being fully priced in
the attempt-prop market is high. The test adjudicates strictly
*incremental* information in the market price, not whether rain affects
play-calling.)

- Frozen: 2026-09-16
- Parent documents: `docs/nfl-cd-priced-pull-spec-2026-09-16.md` (master
  spec; D = §2.x, frozen precipitation feature = §2.0);
  `nfl-edge/docs/nfl-h-d-feasibility.md` (free MOS count + minimum-cost
  pull proposal; §4a manifest correction addendum applied at freeze);
  `nfl-edge/ops/nfl_hd_pull_manifest.json` (the exact 60-event pull
  manifest, corrected provider ids, frozen).
- This document supersedes
  `nfl-edge/docs/preregistrations/nfl-h-d-precipitation-attempts-DRAFT.md`
  (deleted at freeze). Content is unchanged from the DRAFT except: the
  five pending items are resolved below per the user's freeze defaults,
  the manifest transcription errors are corrected, and implementation
  specifications (§Freeze receipt) are recorded.
- Market data: the priced pull authorized only after this freeze —
  immutable append-only pull log (`nfl_hd_pull.py` → JSON artifact);
  failed calls recorded as explicit gaps, never retried into a different
  timestamp.
- Settlement data: nflverse weekly `player_stats` (free).
- Scope: PLAYER PROP ATTEMPTS, NFL regular seasons 2023–2024 only
  (prop history begins 2023-05-03 per the provider; there is no 2022).
  Postseason excluded.
- Question: does timestamp-safe *forecast* precipitation contain
  INCREMENTAL information about QB pass attempts and RB rush attempts
  beyond the contemporaneous betting-market prop lines? This is not a
  test of raw rain–play-mix correlation.

## Precipitation feature (adopted verbatim from the frozen spec §2.0,
plus the feasibility gate's pin-downs)

This is the entire precipitation specification. It is adopted verbatim;
any amendment requires cause and a versioned record. PoP is a
descriptor, never the primary.

- **Source:** IEM MOS **GFS** archive only. No NAM/NBE substitution, no
  HRRR upgrade, no retrospective source shopping.
- **Cycle rule:** mechanically the latest 6-hour-grid GFS MOS `runtime`
  with **`runtime + 4h ≤ kickoff − 24h`** (= T_dec). 4h = assumed
  dissemination lag. No cycle re-selection by fit, ever.
- **Window:** the `q06` (6-hour QPF, inches) valid intervals overlapping
  **[kickoff, kickoff + 3.5h]**.
- **Feature:** `F = max q06` over those periods.
- **Treatment (predeclared, from the forecast distribution — never from
  outcomes): treated iff F ≥ 0.25 inches.**
- **Pin-down 1 (feasibility §5.1): q06 valid-period convention is
  [F−6h, F]** (the NWS MOS ending convention). Sensitivity under the
  forward convention: 48/61 treated games stay treated — verdict robust,
  but the convention is pinned regardless.
- **Pin-down 2 (feasibility §5.2): q06 values in the archive interface
  are integer-quantized.** The rule F ≥ 0.25" is applied exactly as
  written, but no game fell in [0.25, 1.0): the **effective treatment
  margin in practice is F ≥ 1.0"** — genuinely heavy-rain forecasts
  (treated-set median PoP 65%).
- **Venues:** `roof ∈ {outdoors, open}` only (nflverse schedules `roof`
  column). `dome`/`closed` excluded wholesale.
- **Pin-down 3 (feasibility §5.3): retractable-roof venues are excluded
  wholesale by structural rule** (frozen default 4). Gameday roof
  position is decided ~90 min before kickoff, i.e. after T_dec, and can
  never be used to include them. (The 11 verbatim-`open` games never
  reached treatment, so the treated set is unaffected; the rule is
  structural, not data-driven.)
- **Pin-down 4 (feasibility §5.4): SoFi Stadium** is marked `dome` in
  nflverse → excluded wholesale; no canopy judgment was needed.
- **Missingness:** cycle unretrievable or non-numeric `q06` → unit
  censored (explicit missingness, never backfilled, never imputed).
- **PoP (`p06`):** recorded at the same ftime rows as a descriptor; it
  is **not** part of the treatment rule and must not become a
  post-result rescue variable.
- **Disqualified, permanently:** realized weather, reanalysis (ERA5),
  current-only APIs, later forecast revisions, retrospective cycle
  re-selection, alternate weather-source shopping, temperature, wind,
  and PoP as rescue variables.

## Snapshot timing (mechanical, game-specific)

- Prediction cutoff **T_dec = kickoff − 24h**, floored to the 5-minute
  grid, per game. Kickoff parsed from nflverse gameday+gametime as
  America/New_York → UTC.
- **Quote call:** `GET
  /v4/historical/sports/americanfootball_nfl/events/{eventId}/odds`
  with `markets=player_pass_attempts,player_rush_attempts`,
  `regions=us`, `date=<T_dec>`, `oddsFormat=american`. Returns the
  closest snapshot ≤ T_dec — by construction, known at T_dec. Both
  markets in **one call per event**.
- **Event-ID multiplicity:** the frozen identity layer carries 1–2
  re-issued ids per game here (40 games × 1 id, 20 × 2 ids). Pull
  execution tries candidate ids in manifest order (by first-seen
  odds_date) and uses the first returning non-empty bookmakers at
  T_dec — never assumes one id spans time. Empty responses are free.
- No window widening, no carry-forward, no reconstruction from other
  timestamps. A game with no qualifying quotes at T_dec is censored
  (see missing-data treatment).

## Eligibility (mechanical funnel; attrition reported at every step, per
endpoint)

1. NFL regular season 2023–2024 game in nflverse schedules (544).
2. `roof ∈ {outdoors, open}` per nflverse, retractable-roof venues
   excluded wholesale (369).
3. Stadium → MOS station resolved (359; the 10 international games are
   censored — never nearest-guessed).
4. MOS cycle retrievable with numeric `q06` in the game window (359;
   zero fetch failures in the feasibility gate).
5. **F ≥ 0.25"** → 61 treated games (2023: 34, 2024: 27). Controls are
   **not** purchased (frozen default 3 — treated-only design; buying
   controls would ~6× the cost for a parked sensitivity, not required
   by the preregistered question).
6. Canonical identity match → ≥1 Odds event id → **60 pull-eligible
   events** (1 treated game, `2024_18_HOU_TEN`, has no Odds event id →
   censored, never imputed).
7. **Paid (at pull time):** ≥2 US books quoting the market at T_dec.
8. Designated player quoted at T_dec (see designation); otherwise
   censored.
9. Designated player active per final injury designations (~48h
   pre-kickoff, i.e. before T_dec); report_status `Out` → censored.

The exact pull manifest is `nfl-edge/ops/nfl_hd_pull_manifest.json`
(60 events: game, T_dec, F, provider event id(s) in try-order) and the
corrected §4 table in `nfl-edge/docs/nfl-h-d-feasibility.md`.
**60 events × 2 markets × 10 = 1,200-credit hard ceiling**; expected
actual ~1,020–1,080 at 85–90% market presence (empty responses are
free). Expected usable N: **51–54 units per endpoint** after
~10–15% coverage/inactive attrition.

## Player designation (frozen; strictly pre-T_dec)

- **Pass-attempts unit:** (game, designated QB1). **Rush-attempts unit:**
  (game, designated RB1). One unit per endpoint per game, maximum.
- **Designation rule:** the player with the highest lagged offensive
  snap share **at that position on that team** (lagged `snap_counts`,
  weeks < t, all weeks including postseason). Most recent week with data
  first (typically t−1); tie-break chain: week t−2 snaps → week t−3
  snaps → most pass attempts (QB) / rush attempts (RB) in the most
  recent week (lagged `player_stats`, weeks < t only) → `player_id`
  alphabetical. Deterministic, frozen pre-pull, never re-picked post
  hoc (a wrong RB1 in a committee backfield is noise, not a design
  change). The attempts tie-break is verified to bind nowhere in the
  test receipt; the integrity gate uses the snap+player_id chain only
  (it must not read outcome columns).
- **Mid-game QB change:** settle on the designated starter's actual;
  documented limitation, never re-designated post hoc.

## Consensus construction (frozen)

- Qualifying books: US books quoting the designated player at T_dec.
  Eligible iff **≥2 books**.
- **L = consensus line** = median line across qualifying books (a book
  with multiple lines for the player contributes its median line).
- Fair probabilities by same-book de-vig of the Over/Under prices
  (informational; the primary statistic uses lines).
- Books "at the consensus line" are those with any line within 0.01 of
  L — used for Stage 2 entry.

## Feature construction (strictly pre-T_dec)

- Precipitation feature exactly as in §Precipitation feature above —
  a single binary treatment indicator (treated iff F ≥ 0.25"; effective
  margin F ≥ 1.0" in practice). No intensity slope, no PoP interaction,
  no multi-level dose in the primary specification.
- No other features enter the primary test. The market line L is the
  baseline against which incrementality is judged.

## Target / settlement

- **R = realized attempts** from nflverse weekly `player_stats` (free).
  Column map (recorded): pass attempts ← `attempts`; rush attempts ←
  `carries`.
- **Target:** residual `R − L` (the market-adjusted quantity — this asks
  whether precipitation adds information the market price has not
  incorporated).
- **Pushes:** actual == line → residual 0; the unit stays in the primary
  test (contributes nothing) and is a push in Stage-2 P&L accounting
  (stake returned). (Frozen default 5.)
- **Voids:** designated player DNP/inactive at kickoff → unit excluded,
  recorded. (Designation-time inactives are censored at step 9 above;
  voids cover only post-T_dec scratches.)

## Baseline (the market price itself)

- No fitted baseline model: the test is one-sample on the treated-unit
  residual. The consensus line L already embeds team strength,
  game-script expectations, and any weather shading the books apply —
  which is exactly why the residual is the right target.
- **Season FE (demeaning):** within each season, residuals are demeaned
  (`r̃_i = r_i − season-mean(r)`) before the test. This absorbs
  year-level market or rule shifts without consuming the
  weather-exogenous variation.
- No team FEs: they would absorb between-team variation the market line
  already prices; precipitation is exogenous weather, not an assigned
  treatment. Documented choice; per-team residual means reported
  descriptively only.

## Primary statistic (frozen)

- Pooled treated units, 2023+2024, with season demeaning as above.
- **Rush attempts:** one-sided one-sample t-test of mean(r̃) **> 0**
  (heavy rain → more rush attempts). **Pass attempts:** one-sided
  one-sample t-test of mean(r̃) **< 0** (heavy rain → fewer pass
  attempts). α = 0.05 per test, **Holm step-down across the two
  endpoints** (within-family multiple-testing correction).
- **SEs clustered by season-week** (absorbs correlated weather systems
  across games in the same week). t = mean(r̃)/SE_clu with
  SE_clu = sqrt(Σ_c (Σ_{i∈c} r̃_i)²)/n, df = G−1 (G = season-week
  clusters).
- **Directional gate:** a significant result with the wrong sign is a
  FAIL on mechanism, never a PASS.
- **Stop rule:** the confirmatory tests run only if usable N ≥ 25 per
  endpoint; below that the family is parked/retired on power without
  outcome exposure (the feasibility sensitivity rule, frozen).

### Season folds (pre-freeze design decision, recorded)

Pooled with season FE is frozen; walk-forward rejected pre-freeze on
the outcome-blind count (2024 test fold would hold only ≈22–23 usable
units → MDE_rush ≈ 1.8 attempts, underpowered against the mechanism's
central prediction). Pre-freeze design correction with zero outcome
exposure — consumes no statistical capital.

### Clustering

Season-week clusters. The iid MDE below is therefore mildly optimistic;
the cluster count is reported with the result.

## Power / MDE (frozen constants — planning assumptions, never
estimated from outcomes)

One-sided α=0.05, power=0.8 → z-sum **2.4865**. σ_res = 3.5 attempts
(rush) / 4.5 attempts (pass) — planning assumptions, never re-estimated
from the sample.

| Scenario | N | MDE rush | MDE pass |
|---|---|---|---|
| Pull-eligible (free count) | 60 | 1.12 attempts | 1.45 attempts |
| Expected usable after attrition | 51–54 | 1.18–1.22 | 1.52–1.57 |

The mechanism's central prediction (+1.5–2.0 rush attempts for the
lead back in heavy rain) clears the rush MDE at every scenario. Power
is viable; the binding constraint is paid-pull economics, not power.

## Economic endpoint — Stage 2 (frozen, secondary)

- **Betting rule (predeclared, no fitted magnitude):** on every usable
  treated unit, take the mechanism direction at the consensus line —
  **Over 1u on rush attempts, Under 1u on pass attempts.**
- **Execution (frozen default 2):** enter at the best available price
  among books quoting at the consensus line (within 0.01) at T_dec;
  ties → alphabetical `book_key`; if none, no bet (reported). Settle vs
  R at the taken price; pushes return stake; voids excluded. Flat 1u,
  no Kelly, no selection.
- Report mean per-unit P&L with 95% CI (season-week-clustered SEs,
  t_{G−1}).
- **Practical bar (pre-declared): +0.005u per unit** (H-N2/H-N6
  precedent). Statistical significance alone does not promote H-D.

## Exclusions

- Postseason (outside the prop-history window by construction).
- Domes, closed venues, retractable-roof venues (wholesale structural
  rule), international venues without MOS coverage (censored, never
  guessed).
- The 2022 W17 BUF@CIN suspended-game convention is not applicable
  (2023–2024 scope).
- No game, team, or player subset is ever added post hoc.

## Missing-data treatment

- Complete-case at the unit level. Attrition reported at every funnel
  step, per endpoint: schedule games → outdoor/open → station-resolved →
  MOS-available → treated → identity-matched → ≥2-book coverage →
  designated-player quoted → active at designation → usable.
- **Censored (never backfilled, never imputed):** 10 international
  games; 1 treated game without an Odds event id; MOS fetch failures;
  games without ≥2-book coverage at T_dec; designated player unquoted
  or designation-inactive.
- **Voids at settlement:** designated player DNP/inactive at kickoff —
  excluded, recorded.
- No imputation of venues, lines, cycles, precipitation values, or
  player designations.

## Exact pass / fail / retirement rule (frozen default 1)

- **PASS:** both endpoints Holm-significant with the correct sign AND
  the economic endpoint ≥ +0.005u/unit. → H-D advances to prospective
  confirmation on future heavy-rain games (no further historical
  testing on this question).
- **INCONCLUSIVE (parked, not failed):** statistically significant
  correct-sign result(s) but economic endpoint < +0.005u/unit.
- **FAIL (retire):** no Holm-significant correct-sign endpoint, or any
  endpoint significant with the wrong sign, or usable N < 25 per
  endpoint at the stop rule. Family retired permanently under this
  specification.
- **No variants, ever:** no nearby precipitation cutoffs, no PoP, no
  temperature/wind rescue variables, no re-designation of players, no
  pooling of loosely related weather effects, no control-group
  comparisons presented as independent confirmation. A null retires the
  family; it does not prove rain doesn't matter.

## Power caveat (pre-registered honestly)

- The primary is identified off treated games only — a deliberate
  minimum-cost design (treated-vs-control would ~6× the pull cost for
  a second-order refinement; parked, not purchased).
- Season-week-clustered inference is honest about correlated weather
  systems but costs power vs the iid MDE; the MDE table above is mildly
  optimistic.
- A null cannot distinguish "precipitation is fully priced" from
  "precipitation was never forecastable-useful at this horizon".

## Freeze receipt (2026-09-16)

**Final free validation** (zero credits, zero outcomes;
`nfl-edge/ops/` validation script, 2026-09-16):
- Pull universe == the 60 preregistered eligible events: 60 unique
  games, no duplicates, 40 × 1 id / 20 × 2 ids, no id shared across
  games, `2024_18_HOU_TEN` correctly excluded.
- T_dec == floor5(kickoff−24h) verified against live nflverse schedules
  for all 60 games.
- Credit math: 60 × 2 × 10 = **1,200 ceiling**; expected actual
  1,020–1,080 at 85–90% market presence (empty responses free).
- **2 transcription errors corrected** against the frozen
  canonical-identity artifact (run 35041420559): `2023_15_PHI_SEA`
  truncated id → `f51da6d224ba8d719fe220e807cd0442`; `2023_18_PIT_BAL`
  one hex digit → `f5160997517aa6f8f1876ee7e84f227e`. Mechanical data
  correction, not a specification change (see feasibility §4a).
- Internal consistency check of the five pending items against the
  user's freeze defaults: **no contradictions found** — resolved as
  frozen above (both-endpoint PASS; best-available T_dec price;
  treated-only; retractable-roof wholesale exclusion; pushes residual
  0 / stake-returned).

**Implementation specifications** (computational details fixed at
freeze, before any outcome inspection):
- Cluster t: SE_clu = sqrt(Σ_c (Σ_{i∈c} r̃_i)²)/n, df = G−1.
- Alternate lines: a book with multiple lines for the player
  contributes its median line to the consensus; Stage-2 entry considers
  each of the book's lines within 0.01 of L.
- Lagged designation weeks: all weeks (incl. postseason) < t, most
  recent first.
- Step-9 inactives: final injury report `report_status == 'Out'` only.
- Player-name matching: case/punctuation/suffix-insensitive
  normalization (`nfl_hd_common.norm_name`).
- Scripts: `nfl-edge/ops/nfl_hd_pull.py` (paid pull, hard 1,200-credit
  abort), `nfl-edge/ops/nfl_hd_integrity.py` (Phase 4 gate, no outcome
  access), `nfl-edge/ops/nfl_hd_test.py` (Phase 5 frozen test),
  `nfl-edge/ops/nfl_hd_common.py` (shared frozen logic),
  `.github/workflows/nfl-hd-pull.yml` (paid-pull vehicle).

## Statistical capital

Zero consumed to date. All prior NFL families retired on feasibility or
FAIL without spending family-wise alpha. On this freeze, H-D spends
**one family-wise slot** (α=0.05, Holm across active families); within
the family, Holm step-down across the two endpoints.

## Outcome-blindness certification

- `outcome_rows_read = 0` through freeze. The feasibility gate
  (`nfl_h_d_precip_feasibility.py`) whitelisted only non-outcome
  schedule columns; score and realized-weather columns were never
  accessed.
- The identity join used the frozen canonical-identity layer only.
- σ_res values are planning assumptions, never estimated from the
  sample; the empirical residual variance was never computed.
- No actual attempts, realized scores, empirical residuals, or anything
  derived from game outcomes entered any count, distribution, MDE, or
  this document. The pull (`nfl_hd_pull.py`) and integrity gate
  (`nfl_hd_integrity.py`) are structurally barred from outcome data;
  outcomes are first read by `nfl_hd_test.py`, which runs only after
  the integrity gate passes.

## Files

- `nfl-edge/ops/nfl_hd_pull_manifest.json` — the exact 60-event pull
  manifest (frozen)
- `nfl-edge/ops/nfl_hd_pull.py` — paid pull execution (hard 1,200 cap)
- `nfl-edge/ops/nfl_hd_integrity.py` — Phase 4 integrity gate
- `nfl-edge/ops/nfl_hd_test.py` — Phase 5 frozen test
- `nfl-edge/ops/nfl_hd_common.py` — shared frozen logic
- `nfl-edge/ops/nfl_h_d_precip_feasibility.py` — MOS count + pull
  manifest gate (read-only, zero credits)
- `nfl-edge/ops/nfl_stadium_mos.json` — stadium→MOS station mapping
  (30 US venues verified; 10 internationals explicit `unmatched`)
- `nfl-edge/docs/nfl-h-d-feasibility.md` — feasibility report + the
  exact 60-event pull manifest (§4, corrected §4a)
- `docs/nfl-cd-priced-pull-spec-2026-09-16.md` — master spec (§2.x for D)
- `.github/workflows/nfl-hd-pull.yml` — paid-pull workflow

*FROZEN 2026-09-16T17:45:00Z. User-authorized 2026-09-16 13:33 EDT.
No outcomes inspected through freeze. One family-wise alpha slot spent
on this freeze.*
