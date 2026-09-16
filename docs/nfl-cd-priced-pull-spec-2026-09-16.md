# NFL hypotheses C & D — priced-pull / infrastructure specification

**Date:** 2026-09-16. **Status:** SPECIFICATION ONLY — no pull authorized, no credits spent, no outcomes inspected.
**Scope of this document:** cost every paid input needed to test hypotheses C (OL/DL pressure → QB props)
and D (forecast precipitation → rush/pass-mix props), so the user can approve or deny spend with exact numbers.
Nothing here authorizes a pull. The gate order from the user's credit directive still applies:
counts → feasibility → frozen preregs → identity → authorization.

**Credits spent producing this document: 0.** All counts below are free schedule facts
(nflverse `games.parquet`, outcome columns dropped before reading), provider documentation,
or explicitly labeled planning assumptions. Nothing that required a paid probe was guessed —
probe costs are separate line items.

---

## 0. Shared infrastructure facts (verified for this document)

### 0.1 The paid endpoint and its cost

- Player props are **additional markets**. Per the provider docs
  (`GET historical event odds`): *"Historical data for additional markets (player props,
  alternate lines, period markets) are available after 2023-05-03T05:30:00Z."*
  Consequence: **2023→2024 walk-forward is the maximum historical window. There is no 2022
  prop history.** Any C/D family is two seasons by construction.
- Cost: **10 credits per market per region per event per timestamp** (docs-verified).
  Multiple markets may be requested in one call; cost is computed on the **unique markets
  present in the response** — requesting a market that returns no data for that event costs
  nothing for that market, and *"responses with empty data do not count towards the usage quota."*
- The **bulk** historical odds endpoint (`/v4/historical/sports/{sport}/odds`) mirrors the
  `/odds` endpoint's parameters, which accept only featured markets (`h2h`, `spreads`, `totals`,
  `outrights`). Player props are documented as available on the **per-event** historical endpoint
  (*"Accepts all available market keys"*). **Pricing in this document therefore assumes
  per-event pulls: 10 credits × markets × events.** Probe step 0 (§4) tests whether bulk
  historical happens to return prop markets; if it does, C's cost collapses and this document
  is revised before any spend.
- Snapshots exist at 5-minute intervals (since Sept 2022). The `date` parameter returns the
  closest snapshot **equal to or earlier than** the supplied timestamp — so a per-game
  `date = kickoff − 24h` (floored to the 5-minute grid) is always well-defined.
- `regions=us` only (1 region). The `bookmakers` filter (≤10 bookmakers = 1 region) costs the
  same as all-US-books; it may be used to restrict to prop-posting books without changing cost.

### 0.2 Market keys (provider-documented; historical per-book availability = probe item)

From the provider's market documentation and the 2026-09-15 coverage probe
(`docs/nfl-new-lane-inventory.md` §2):

| Hypothesis | Market key | Meaning | Coverage notes (current, US) |
|---|---|---|---|
| C | `player_pass_yds` | QB passing yards O/U | DK/FD/BetRivers post; moderate depth |
| C | `player_sacks` | **Defensive-player** sacks O/U | Thin; listed at Pinnacle EU + some US books |
| D | `player_pass_attempts` | QB pass attempts O/U | Standard QB prop menu |
| D | `player_rush_attempts` | Rusher rush-attempts O/U | Standard RB prop menu |

Whether each key was actually quoted **in 2023–2024** and by which books at kickoff−24h
is NOT assumed — it is probe step 1 (§4).

### 0.3 Free schedule facts (no outcomes; outcome columns dropped before read)

From nflverse `games.parquet`, regular season only:

- 2023: **272** REG games; 2024: **272** REG games → **544** total candidate games.
- Distinct kickoff−24h decision slots (5-min grid): 123 (2023) + 125 (2024) = 248.
  (Irrelevant to per-event cost, but bounds any future bulk design.)
- Roof classes: `outdoors` 358, `dome` 101, `closed` 74, `open` 11 →
  **369 weather-relevant games** (outdoors + open) across 2023–2024 for D's denominator.
- Event IDs: the frozen identity layer already maps `odds_event_id → nflverse_game_id`
  (`docs/nfl-identity-match-report.md`, 1,478/1,482 matched). The priced pull reuses those
  IDs — **no paid `historical/events` calls are needed** for matched games.

### 0.4 Shared decision-timestamp rule

- **T_dec = kickoff − 24h**, floored to the 5-minute grid, per game. Kickoff parsed from
  nflverse gameday+gametime as America/New_York → UTC (same convention as the H-N2 DRAFT).
- **Quote call:** `GET /v4/historical/sports/americanfootball_nfl/events/{eventId}/odds`
  with `markets=<keys>`, `regions=us`, `date=<T_dec>`, `oddsFormat=american`.
  Returns the closest snapshot ≤ T_dec. Both markets for a hypothesis go in **one** call
  (20 credits/event when both present, 10 when one present, 0 when neither).
- **Missingness:** a game with no qualifying prop quotes at T_dec is **censored** —
  excluded from that endpoint's analysis and recorded in the attrition table. Quotes are
  never inferred, carried forward, or reconstructed from other timestamps.
- **Consensus (proposed; frozen at prereg):** median line across qualifying US books with
  ≥2 books quoting the designated player; fair probabilities by same-book de-vig of the
  Over/Under prices. The exact consensus rule is analysis design (free) and is frozen in
  the preregistration, not here.

---

## 1. Hypothesis C — OL/DL pressure mismatch → QB props

### 1.1 What must be bought

| Item | Detail |
|---|---|
| Endpoint | `GET /v4/historical/sports/{sport}/events/{eventId}/odds` (per-event historical) |
| Markets | `player_pass_yds`, `player_sacks` (one call, `markets=player_pass_yds,player_sacks`) |
| Regions | `us` |
| Events | all 544 REG games 2023–2024 (walk-forward needs both seasons' quotes) |
| Timestamp | `date` = per-game kickoff − 24h (floored 5-min) |

**Credit math: 544 events × 2 markets × 10 = 10,880 hard ceiling.**
Expected actual: empty-data responses are free; assuming ~90% of games have ≥1 prop market
quoted at T_dec, ≈ 9,800. The ceiling is the number to budget.

Reduced-scope option (see §1.7): `player_pass_yds` only → **5,440 ceiling**.

### 1.2 Eligible N (mechanical funnel; free inputs except the quotes)

Unit definitions (proposed; frozen at prereg):

- **Pass-yards unit:** (game, designated starting QB). Designation rule (proposed):
  the QB with the highest offensive snap share for that team in week t−1
  (lagged `snap_counts`; tie-break week t−2). Strictly pre-T_dec information.
- **Sacks unit:** (game, team, primary edge rusher of the *defense facing the designated QB*).
  `player_sacks` is a defensive-player prop. Designation rule (proposed): the EDGE/DE/OLB
  with the highest lagged pass-rush snap count on that defense (lagged `snap_counts` ×
  FTN charting; weeks < t). Two units per game maximum (one per defense).

Funnel (attrition reported at every step; all steps except (4) are free):

1. 544 REG games 2023–2024.
2. Canonical identity match → ≥1 Odds event id (frozen layer).
3. Feature availability: FTN charting + NextGen + snap_counts for weeks < t
   (verified present 2022–2024 in the lane inventory; per-game completeness counted free).
4. **Paid:** ≥2 US books quoting the market at T_dec (consensus rule).
5. Designated player quoted: the designated QB (pass_yds) / designated edge rusher (sacks)
   has a listed line at T_dec; otherwise the unit is censored.
6. Designated player active per final injury designations (~48h pre-kickoff, i.e. before
   T_dec); inactive → censored.

**Planning estimates (assumptions, not counts):** pass_yds 2024 test set ≈ 235–245 units;
sacks 2024 test set ≈ 400–470 units (designated-rusher prop coverage is thinner — probe
step 1 quantifies this). Train (2023) ≈ same magnitudes. Exact N is produced by the
outcome-blind eligibility gate **after** the pull is authorized — never before.

### 1.3 Feature data (all free; the priced pull does not buy features)

Pressure-mismatch index (exact construction frozen in the future prereg DRAFT; the
information set is fixed here): offense lagged pressure-allowed rate vs defense lagged
pressure-generated rate from FTN charting (`n_pass_rushers`, `n_blitzers`,
`is_qb_out_of_pocket`) + NextGen `time_to_throw`, starter identification from lagged
`snap_counts` — **all strictly weeks < t** (charting is published post-game; lag is
structural). No injury-news timing (that mechanism is burned by H-N1).

### 1.4 Timestamp safety

| Quantity | Cutoff | Known by T_dec? |
|---|---|---|
| Pressure features (charting, NextGen, snaps) | weeks < t only | Yes — published days after each game |
| Final injury designations | ~kickoff − 48h | Yes — precedes T_dec |
| Prop quotes | snapshot ≤ T_dec via `date` param | Yes — by construction |
| Outcomes (actual yards/sacks) | post-game | Never in the information set |

No lookahead: the latest information used (quotes at T_dec, designations at −48h,
charting through week t−1) is all strictly pre-T_dec.

### 1.5 Settlement coverage

- **Source:** nflverse weekly `player_stats` (free): passing yards per QB-game;
  sacks per defensive-player-game. (Exact column mapping resolved at implementation;
  the statistical fields are passing yards, pass attempts, sacks — all standard
  nflverse player-stat outputs.)
- **Pushes:** actual == line → the residual is 0; the unit stays in the regression
  (contributes nothing) and is a push in any Stage-2 P&L accounting.
- **Voids:** designated player DNP/inactive at kickoff → unit excluded (void), recorded.
- **Mid-game QB change:** settle on the designated starter's actual; documented limitation,
  never re-designated post hoc.
- Team-level sacks are NOT the endpoint — `player_sacks` is per defender; the unit
  definition in §1.2 stands.

### 1.6 Minimum viable power

Conventions: one-sided α = 0.05, power = 0.8 → z-sum **2.4865**. Predictor standardized
(s_x = 1). σ_res values are **planning assumptions** (never estimated from outcomes;
the outcome-blind gate may not estimate residual variance from the sample).

- **Pass yards (slope on standardized mismatch index, 2024 test set):**
  MDE_β = 2.4865 × σ_res / √N, σ_res = 40 yds (assumption), N = 240 →
  **MDE ≈ 6.4 yards per 1-SD of mismatch.**
  Verdict: **viable** — a true effect of ≥ ~6–7 yards per SD (bad-OL-vs-elite-DL
  suppressing the QB's yardage by that much) is within the plausible mechanism range.
  Sensitivity: N = 200 → MDE ≈ 7.0 yds.
- **Sacks (slope, 2024 test set):**
  MDE_β = 2.4865 × 0.75 / √460 ≈ **0.087 sacks per SD** (σ_res = 0.75 assumption).
  Verdict: **viable on power**, but the binding constraint is **coverage**, not power —
  `player_sacks` is the thinnest prop in the set and the designated-rusher unit may fail
  the consensus rule often. Probe step 1 measures this before any spend decision.

### 1.7 What the frozen spec would contain (after authorization)

Eligibility funnel (§1.2) with exact designation rules; the single frozen
pressure-mismatch construction; consensus rule; baseline
(`R − L ~ season FE + decision-slot FE`, L = consensus prop line, R = realized);
Stage 1 one-sided incremental-information test, 2023→2024 walk-forward
(fit on 2023 residuals, test on 2024), SEs clustered by season-week, per-season reporting;
Stage 2 economic bar (+0.005u/game on a predeclared Under/Over rule at consensus prices —
significance alone never promotes); terminal retirement rule (fail of the frozen primary
spec retires the family; no nearby cutoffs, variables, or endpoints as rescue variants).

### 1.8 C: GO / NO-GO

**NO-GO on the full pull (10,880-credit ceiling).** The cost is disproportionate for a
first test of a new family at this evidence stage: it would consume the majority of any
plausible historical-market budget on a single hypothesis whose sacks endpoint has a
known coverage risk. **Do not approve 10,880.**

Recommended path, in order:
1. Approve the shared probe only (**≤160 credits**, §4) — it resolves the two unknowns
   that dominate this decision: (a) whether bulk historical returns prop markets
   (collapses cost if yes), (b) actual 2023–24 book coverage of `player_pass_yds` /
   `player_sacks` at T_dec.
2. Build the pressure index and run the **outcome-blind** eligibility gate on free data
   (zero credits) — exact feature-available N, no outcomes.
3. Revisit with a scoped request. The defensible reduced scope is
   **`player_pass_yds` only, both seasons: 5,440-credit ceiling** — the primary mechanism
   (pressure → throwaways/checkdowns → fewer yards, Under direction) with the deeper
   prop menu. `player_sacks` stays parked until pass_yds shows a signal or the probe
   shows unexpectedly deep sacks coverage.

**Exact number for the user to approve today: 160 (probe only).** The 5,440 reduced-scope
number returns for explicit approval only after steps 1–2.

---

## 2. Hypothesis D — forecast precipitation → rush/pass-mix props

### 2.0 The frozen precipitation feature (specified prospectively here, per task §7)

This is the **entire** precipitation specification. It is frozen as written; the future
prereg DRAFT adopts it verbatim or records an amendment with cause. PoP is a descriptor,
never the primary.

- **Source:** IEM MOS **GFS** archive only (spike-verified, `docs/nfl-weather-archive-spike.md`).
  No NAM/NBE substitution, no HRRR upgrade, no retrospective source shopping.
- **Cycle rule (the H-N2 gate's machinery, reused):** the mechanically latest 6-hour-grid
  GFS MOS `runtime` with **`runtime + 4h ≤ kickoff − 24h`** (= T_dec). 4h = assumed
  dissemination lag (spike-verified). No cycle re-selection by fit, ever.
- **Window:** the `q06` (6-hour QPF, inches) valid periods whose valid intervals overlap
  **[kickoff, kickoff + 3.5h]** (the game window; 3.5h covers essentially all game durations).
- **Feature:** `F = max q06` over those periods (inches of liquid-equivalent precipitation).
- **Treatment (predeclared, from the forecast distribution — never from outcomes):**
  **treated iff F ≥ 0.25 inches.**
- **Venues:** `roof ∈ {outdoors, open}` only (nflverse schedules `roof` column).
  `dome`/`closed` excluded wholesale; international venues with no MOS coverage excluded.
  (SoFi-style canopy judgment is not needed here — precipitation, unlike wind, is
  unambiguously blocked by any roof; `closed` is excluded by structural rule.)
- **Missingness:** cycle unretrievable or non-numeric `q06` → unit censored
  (explicit missingness, never backfilled, never imputed).
- **PoP (`p06`):** recorded at the same ftime rows as a descriptor for the feasibility
  report; it is **not** part of the treatment rule and must not become a post-result
  rescue variable.

### 2.1 What must be bought

| Item | Detail |
|---|---|
| Endpoint | `GET /v4/historical/sports/{sport}/events/{eventId}/odds` (per-event historical) |
| Markets | `player_pass_attempts`, `player_rush_attempts` (one call per event) |
| Regions | `us` |
| Events | **treated games only** (F ≥ 0.25"), 2023–2024 — identified by the free MOS gate |
| Timestamp | `date` = per-game kickoff − 24h (floored 5-min) |

**Credit math: G_precip × 2 markets × 10, where G_precip = number of treated games.**
- **Absolute hard ceiling:** all 369 outdoor/open games × 20 = **7,380** (unreachable in
  practice; stated so the authorization has a bound).
- **Planning estimate:** 10–15% of outdoor games reach 0.25" QPF in the game window →
  **≈ 37–55 games → 740–1,100 credits.** G_precip is counted exactly, outcome-blind and
  free, by the MOS feasibility gate — that count is the prerequisite this document does
  NOT perform (specification only).

Why treated-only: the primary test (§2.6) is a one-sided test of the treated-unit residual
against zero (the market residual should be mean-zero if efficient). Controls are not
purchased. A treated-vs-control comparison is a parked sensitivity, not the primary —
buying control quotes would roughly 7× the cost for a second-order refinement.

### 2.2 Eligible N (mechanical funnel; free inputs except the quotes)

Unit definitions (proposed; frozen at prereg):

- **Pass-attempts unit:** (game, designated QB1). **Rush-attempts unit:** (game, designated RB1).
  Designation (proposed): highest lagged snap share at the position on that team
  (lagged `snap_counts`, weeks < t). Strictly pre-T_dec.

Funnel:

1. 369 outdoor/open REG games 2023–2024.
2. MOS cycle retrievable with numeric `q06` (free; failures censored explicitly).
3. **F ≥ 0.25"** (free; this is G_precip — counted by the feasibility gate, zero credits).
4. **Paid:** ≥2 US books quoting the market at T_dec.
5. Designated QB1/RB1 quoted at T_dec; otherwise censored.
6. Designated player active per final injury designations (~48h pre-kickoff); inactive → censored.

**Planning estimate:** G_precip ≈ 37–55 games → ≈ 37–55 pass-attempt units and
≈ 37–55 rush-attempt units (some games lose one unit to coverage/inactives; attrition
reported per endpoint).

### 2.3 Feature data (all free)

IEM MOS GFS `q06`/`p06` via the archive interfaces in the spike doc — ~369 games × 1 HTTP
call each, anonymous, $0. The stadium→MOS-station map is built once (the MOS archive
agent's deliverable; until then the H-N2 gate's mapping work applies).

### 2.4 Timestamp safety

| Quantity | Cutoff | Known by T_dec? |
|---|---|---|
| MOS cycle | `runtime + 4h ≤ T_dec` (mechanically latest) | Yes — by construction |
| Prop quotes | snapshot ≤ T_dec | Yes — by construction |
| Final injury designations | ~kickoff − 48h | Yes — precedes T_dec |
| Outcomes | post-game | Never in the information set |

The forecast used is the one a bettor could actually have seen 24h before kickoff.
Realized weather is permanently disqualified (leakage — see inventory non-goals).

### 2.5 Settlement coverage

- **Source:** nflverse weekly `player_stats` (free): pass attempts per QB-game,
  rush attempts per rusher-game.
- **Pushes:** actual == line → residual 0; unit stays in the test.
- **Voids:** designated player DNP/inactive → excluded, recorded.
- **Committee backfields:** settle on the designated RB1's actual; the designation rule
  is frozen pre-pull and never re-picked post hoc (a wrong RB1 pick is noise, not a
  design change).

### 2.6 Minimum viable power

One-sided α = 0.05, power = 0.8 (z-sum 2.4865). Primary: one-sample one-sided test of the
treated-unit residual mean vs 0 (Over direction for rush attempts, Under for pass attempts).

- **Rush attempts:** MDE_mean = 2.4865 × σ_res / √N, σ_res = 3.5 attempts (assumption),
  N = 45 → **MDE ≈ 1.3 attempts.**
  Verdict: **viable** — a true rain effect of +1.5–2.0 rush attempts for the lead back
  is the mechanism's central prediction and clears the MDE.
- **Pass attempts:** σ_res = 4.5 (assumption), N = 45 → **MDE ≈ 1.7 attempts.**
  Verdict: **viable** at the same N.
- **Sensitivity (no threshold weakening):** if the free MOS count returns N < 25,
  MDE exceeds ~1.7–2.2 attempts and the test is underpowered for plausible effects —
  the family is then **retired or parked, not rescued** by lowering the 0.25" threshold
  or pooling loosely related weather effects. If N > 70, MDE ≈ 1.0 attempts.

### 2.7 What the frozen spec would contain (after authorization)

The precipitation feature verbatim from §2.0; eligibility funnel (§2.2) with exact
designation rules; consensus rule; Stage 1 one-sided treated-residual tests
(2023→2024 walk-forward: estimate on 2023 treated units, test on 2024 — or pooled with
season FE if N is small; frozen at prereg, not chosen post hoc); Stage 2 economic bar
(+0.005u/game on a predeclared Over/Under rule at consensus prices); terminal retirement
rule (fail of the frozen primary spec retires the family; PoP, alternate thresholds,
temperature, and wind are permanently excluded as rescue variants).

### 2.8 D: GO / NO-GO

**CONDITIONAL GO — but not yet.** The expected pull (740–1,100 credits) is modest and the
power arithmetic works at the planning N, but two free prerequisites must land first:

1. **The free MOS feasibility gate** (zero credits, outcome-blind): builds the stadium→MOS
   map, pulls `q06` for the 369 outdoor/open games, counts **G_precip exactly**, and reports
   the predictor-side distribution. This fixes the exact credit number:
   **G_precip × 20**.
2. **The shared probe (≤160 credits, §4):** confirms `player_pass_attempts` /
   `player_rush_attempts` were quoted historically at T_dec with ≥2-book depth.

After (1) and (2), the D pull returns for **explicit approval at the exact number**
(G_precip × 20, expected 740–1,100, hard ceiling 7,380). **No number is approved today
beyond the shared probe.**

---

## 3. What remains before any claim-bearing test (C and D)

1. Shared probe (§4) — ≤160 credits, resolves bulk-vs-per-event and historical key coverage.
2. Free feasibility gates (zero credits, outcome-blind): feature-side N counts, predictor
   distributions, MDE recomputation with the frozen σ_res planning constants.
   (D's MOS count is prerequisite to D's exact price; C's charting-based N is prerequisite
   to C's scope decision.)
3. Preregistration DRAFTs adopting this document's frozen elements (§2.0 for D; §1.2–§1.4
   proposed rules for C), then **user approval to freeze**.
4. Pull authorization at the exact credit number — a separate explicit approval.
5. Pull execution with per-call logging; immutable market table (append-only; failed calls
   recorded as explicit gaps, never retried into a different timestamp).
6. Settlement join (nflverse player_stats, free) → test run → disposition.
7. Statistical capital: none spent to date. C and D would each consume family-wise slots
   at freeze time (Holm accounting, as H-N2 would have) — recorded in the prereg, not here.

---

## 4. Verification probe — shared line item (priced separately)

| Step | Call | Max cost | Answers |
|---|---|---|---|
| 0 | Bulk historical odds, `markets=player_pass_yds,player_pass_attempts,player_rush_attempts,player_sacks`, `regions=us`, `date=2023-09-09T17:00:00Z` (Sat before 2023 W1) | 0–40 (0 if props unsupported → empty response is free) | Does bulk historical return prop markets? If yes, §1.1/§2.1 cost basis is revised before any further spend. |
| 1 (only if step 0 is empty) | 3 per-event historical calls × 4 markets: a 2023 W1 Sunday-1pm game, a 2024 W1 Sunday-1pm game, a 2023 Thursday-night game; `date` = each game's T_dec | 120 | Do all four keys exist historically? Which books quote at T_dec? Does the identity-layer event id join correctly? |
| — | **Probe ceiling** | **160** | |

Event IDs for the probe come free from the identity layer. No other paid calls are
needed before the feasibility gates.

---

## 5. Cost summary

| Scope | Credits (ceiling) | Recommendation |
|---|---|---|
| Shared probe (§4) | **160** | **Approve** — resolves the two unknowns dominating both decisions |
| C full (pass_yds + sacks, 544 events) | 10,880 | **NO-GO** — do not approve |
| C reduced (pass_yds only, 544 events) | 5,440 | Decide after probe + free gate; number returns for explicit approval |
| D pull (treated games only) | G_precip × 20; expected 740–1,100; hard ceiling 7,380 | **CONDITIONAL GO** after free MOS count + probe; exact number returns for explicit approval |
| D free MOS feasibility count | 0 | Proceed (outcome-blind) |

**Bottom line for the user:** approve **160 credits for the probe** and the zero-credit
feasibility work. Approve nothing else today. C's full 10,880 pull is not recommended at
any scope discussed here; D's pull (expected ~740–1,100) comes back with an exact number
once the free MOS count lands.

---

*Specification only. Zero API credits spent. Zero outcomes inspected. No pull authorized
by this document. All σ_res values are planning assumptions, never estimated from data.*
