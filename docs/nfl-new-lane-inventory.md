# NFL new-lane inventory v2 — genuinely new information sets, markets, and hypothesis-family sketches

**Date:** 2026-09-16. **Status:** INVENTORY + SKETCHES ONLY.
**API credits used: 0.** Zero outcome inspection (no scores, settled results,
or realized residuals touched at any step).
**Role:** opens the next NFL research lane after all six v1 families died
(H-N1/H-N4/H-N5 FAIL; H-N2/H-N3/H-N6 retired-infeasible). Nothing is
preregistered, nothing is tested, frozen NFL v1.3 / forward shadow / manifest
v1 (`0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`,
291,586 spread/total rows, 162 snapshots) are untouched.

**What changed since the 2026-09-15 inventory** (`docs/nfl-research-lane-inventory.md`):
- Weather spike **resolved**: IEM MOS GFS archive verified point-in-time-safe,
  free, 2003→realtime (`docs/nfl-weather-archive-spike.md`). Variables beyond
  wind now available: `tmp`, `p06`/`p12` (PoP), `q06`/`q12` (QPF), `sky`, `cig`, `vis`.
- NFL identity **validated**: 1,478/1,482 Odds events matched; 799/815 nflverse
  REG games (`docs/nfl-identity-match-report.md`). Odds re-issues event ids —
  joins must map any `odds_event_id` → `nflverse_game_id`.
- This document supersedes §C of the old inventory (all six sketches are dead)
  but keeps its §A/§B factual findings, extended below.

**Free verification performed for this document (no credits, no outcomes):**
- The Odds API free endpoints: `/v4/sports` + `/v4/sports/americanfootball_nfl/events`
  → 32 upcoming events enumerated 2026-09-16 (prospective coverage confirmed).
- nflverse-data release assets confirmed present for 2022–2024: `pbp`,
  `injuries`, `snap_counts`, `ftn_charting` (columns incl. `n_blitzers`,
  `n_pass_rushers`, `is_qb_out_of_pocket`, `is_throw_away`), `nextgen_stats`.
- Free eligible-N counts computed from the nflverse schedules CSV
  (schedule facts only — no scores read).

---

## 1. Candidate pregame information sets

Legend: **SAFE** = as-of the decision window with no look-ahead.
**SAFE-WITH-LAG** = safe only with strict lag (weeks < t for a week-t game).
**CONDITIONAL** = safe under a stated as-of rule. **BURNED** = consumed by a
dead family; do not reuse in a nearby framing.

| # | Information set | Free source (2022–24 verified) | Timestamp-safety verdict | New mechanism it could capture |
|---|---|---|---|---|
| 1 | Special-teams efficiency: lagged ST EPA (kickoff/punt/FG/return units), kicker/punter availability | nflverse pbp (`play_by_play_2022+`; play types + `epa`); injuries position-filtered K/P | **SAFE-WITH-LAG** (plays weeks < t; kicker status under injury as-of rules) | Market prices offensive/defensive efficiency; ST is high-variance and low-salience — kicker injuries and elite punt/return units move scoring and field position without moving the market proportionally |
| 2 | Situational conversion: red-zone TD%, early-down success rate, third-down distance profile | nflverse pbp, lagged | **SAFE-WITH-LAG** | Yardage efficiency ≠ scoring efficiency. Teams that sustain drives but stall kick FGs; the market total may overweight yardage-based ratings and underweight the TD/FG split |
| 3 | Pass-rush pressure matchup: offense pressure-allowed rate vs defense pressure-generated rate (FTN `n_pass_rushers`/`n_blitzers`, NextGen `time_to_throw`) | `ftn_charting` + `nextgen_stats` + `snap_counts` (starter identification), all lagged | **SAFE-WITH-LAG** (charting published post-game; `date_pulled` confirms) | Pressure degrades QB efficiency non-linearly; matchup extremes (bad OL vs elite rush) are not fully reflected in season-average-based prop lines |
| 4 | Forecast precipitation / temperature (NOT wind) | IEM MOS `p06`/`p12`, `q06`/`q12`, `tmp` — spike-verified archive | **SAFE** (issue-time vs valid-time semantics; mechanically latest cycle before kickoff−24h, same machinery as the H-N2 gate) | Rain shifts run/pass mix and raises fumble rates; extreme cold degrades kicking. Books set props/totals from neutral-weather projections; the forecast distribution at decision time is the information edge |
| 5 | Time-zone displacement (circadian), distinct from rest days | Schedules + static team time zones, preseason-known | **SAFE** | Body-clock disadvantage (e.g. PT/MT visitor at 1pm ET = 10–11am body time) reduces visitor offensive efficiency. The market prices travel distance into the spread; visitor scoring (team totals) may not reflect it |
| 6 | Roster/snap continuity for matchup features | `snap_counts`, weekly rosters, lagged | **SAFE-WITH-LAG** | Supporting info set: identifies actual starters for OL/DL matchup construction in #3 |
| 7 | Injury reports (non-QB, non-shock framing) | `injuries` 2009+ (practice DNP/Limited/Full + Out/Doubtful/Questionable) | **CONDITIONAL** (final designations ~48h pre-kickoff; early-week windows use only published practice reports) | Deliberately NOT sketched as a family here — too close to burned H-N1. Listed only as a supporting as-of source |
| — | Wind (any variable) → totals | — | **BURNED** (H-N2) | Do not touch |
| — | Rest days / bye / short week → any derivative | — | **BURNED** (H-N3) | Do not touch |
| — | Tempo / pace → any total | — | **BURNED** (H-N4) | Do not touch |
| — | Alt-line internal consistency | — | **BURNED** (H-N5) | Do not touch |
| — | Officiating crews → any endpoint | — | **BURNED** (H-N6) | Do not touch |

As-of rules carried forward: pbp features recomputed with strict week-t
cutoffs (never nflverse precomputed season aggregates for the current week);
injury designations only from reports published before the decision window;
FTN/NextGen lagged the same as pbp; MOS cycle = mechanically latest with
`runtime + dissemination lag ≤ kickoff − 24h`, no retrospective cycle shopping.

---

## 2. Alternative markets — coverage

Historical fact (provider-documented, unchanged): **featured markets
(`h2h`, `spreads`, `totals`) have history from 2020-06-06; every
additional market class below has NO history before 2023-05-03.**
Consequence: any family whose primary endpoint is an additional market can
train on 2023–2024 only (2 seasons → walk-forward, not 3-fold LOSO).

| Market class | Example keys | Historical (paid endpoint) | Prospective (free check 2026-09-16) | Book depth (2026-09-15 probe, cited) |
|---|---|---|---|---|
| Team totals | `team_totals`, `alternate_team_totals` | 2023-05-03 → present | 32 upcoming events enumerated; class offered provider-wide | Moderate: full-game team totals at most featured books; alternates DK/FD/BetRivers |
| 1H / 2H | `spreads_h1`, `totals_h1`, `h2h_h1`, `team_totals_h1` (+ alternates) | 2023-05-03 → present | same | 3–5 US books (DK 36 period markets, FD 31, BetRivers 14); Pinnacle EU 10 period markets |
| Quarters | `spreads_q1..q4`, `totals_q1..q4`, `h2h_q1` | 2023-05-03 → present | same | Thinnest of the period classes; DK deepest |
| Player props — passing | `player_pass_yds`, `player_pass_attempts`, `player_pass_completions`, `player_pass_tds`, `player_pass_interceptions` | 2023-05-03 → present | same | DK 33 props, FD 23, BetRivers 26; Pinnacle EU 10 props |
| Player props — rushing/receiving | `player_rush_yds`, `player_rush_attempts`, `player_reception_yds`, `player_receptions`, `player_anytime_td` | 2023-05-03 → present | same | Same books; cross-book overlap varies by prop — consensus weaker than featured |
| Player props — defensive | `player_sacks`, `player_defensive_interceptions`, `player_tackles_assists` | 2023-05-03 → present | same | Thin; Pinnacle lists `player_sacks` |

Notes: historical pulls cost 10 credits / region / market / timestamp
(one event per request) — **no pull is authorized by this document**.
Thin-book consensus (3–5 books) and wider prop vig are the structural
headwinds for any prop family; the feasibility plan must price consensus
requirements before any pull.

---

## 3. Hypothesis-family sketches (NOT preregistered, NOT tested)

Each sketch states mechanism, mispricing rationale, information cutoff,
primary endpoint, and a **free** feasibility plan. Power sketches use
one-sided α=0.05, power=0.8 (z-sum 2.4865); residual-SD planning constants
are literature/assumption values, never estimated from outcomes.

### Sketch A — Special-teams efficiency → totals

**Mechanism.** Special teams contribute ~15–20% of scoring variance through
FG accuracy, punt-driven field position, and return explosiveness, yet
receive a fraction of modeling and betting attention. A team losing its
starting kicker, or fielding a bottom-decile punt unit against a top-decile
return unit, plays a different scoring distribution than its
offensive/defensive ratings imply. The market total is set primarily from
offense/defense efficiency ratings; ST adjustments, if present, are coarse.

**Why mispriced.** Low salience: kicker news moves no handle; ST EPA is not
part of mainstream power ratings. The adjustment path (kicker injury →
market total) has no dedicated market maker the way QB news does.

**Information cutoff.** Lagged pbp ST EPA (plays from weeks < t only);
kicker/punter availability from injury reports published before the
decision window (final designations ~48h pre-kickoff → day-before/day-of window).

**Primary endpoint.** Market residual framing: `(actual total − market
total) ~ lagged ST EPA differential`, 3-fold leave-one-season-out over
2022–2024 (featured `totals`, N=816), one-sided, SEs clustered by
season-week, each season reported separately.

**Free feasibility plan.** Compute ST EPA differential per game from free
pbp (weeks < t); eligible N=816 fixed (featured market, 3 seasons);
empirical SD of the differential is outcome-blind (predictor-side) → slope
MDE = 2.4865 × 13.5 / (s_w × √816). Secondary descriptive: kicker-change
games identified free from injuries/transactions (count only, no modeling).

### Sketch B — Red-zone/situational conversion → totals

**Mechanism.** Two teams can have identical yardage efficiency and very
different scoring: one converts red-zone trips to TDs at 65%, the other at
45% and kicks field goals. Early-down success rate determines how often a
team reaches scoring position at all. Market totals are anchored to
yardage-based efficiency ratings and scoring averages; the TD/FG split is a
second moment the market may underweight.

**Why mispriced.** Public ratings (EPA/play, DVOA-style) blend all
downs; sportsbooks' totals models inherit the same yardage-centric inputs.
Situational splits are noisier, so books may rationally shrink them more
than the true signal warrants — the test is whether the residual signal
survives shrinkage.

**Information cutoff.** Lagged pbp only (weeks < t): red-zone TD%,
early-down success rate, third-down distance profile. Never current-week
aggregates.

**Primary endpoint.** `(actual total − market total) ~ lagged red-zone
efficiency differential`, 3-fold LOSO 2022–2024, `totals`, N=816,
one-sided, clustered SEs, per-season reporting. (Distinct from dead H-N4:
different characteristic — conversion, not pace — and different market.)

**Free feasibility plan.** Build the two differentials free from pbp;
report empirical predictor SDs (outcome-blind); slope-MDE formula as in A.
Falsification is predeclared: no stable coefficient across folds, or the
full-game spread/total already spans the information (encompassing check).

### Sketch C — OL/DL pressure matchup → QB props

**Mechanism.** Pressure degrades QB efficiency non-linearly: a bottom-5
pass-protection OL facing a top-5 pass-rush DL produces more throwaways,
sacks, and checkdowns than either unit's season average implies in
isolation. Prop lines (`player_pass_yds`, `player_sacks`) are set from
QB/season baselines with matchup adjustments; extreme matchups are where
a linear adjustment most plausibly fails.

**Why mispriced.** Prop market-making is heavily automated from player
baselines; OL/DL matchup granularity (which five linemen, which rushers,
blitz rate) is charting data most prop models ingest coarsely or not at
all. (Distinct from dead H-N1: no injury-news timing; the information is
lagged charting, the mechanism is matchup physics.)

**Information cutoff.** FTN charting (`n_pass_rushers`, `n_blitzers`,
`is_qb_out_of_pocket`) + NextGen `time_to_throw` + snap counts for starter
identification — all strictly lagged (weeks < t). Decision window:
day-before/day-of, after final injury designations (so the lined-up OL/DL
personnel are known).

**Primary endpoint.** Prop residual: `(actual − market line) ~ lagged
pressure-mismatch index` for `player_pass_yds` (under direction) and
`player_sacks` (over direction), 2023→2024 walk-forward (props have no 2022
history), books with qualifying consensus only.

**Free feasibility plan.** Build the mismatch index free from charting;
eligible N ≈ 544 QB-games 2023–24 minus missing charting/prop coverage
(count free before any pull); predictor-side SD is outcome-blind →
slope-MDE formula. Market-pull cost is NOT authorized here; the sketch
stops at the priced pull specification.

### Sketch D — Forecast precipitation → rush/pass-mix props

**Mechanism.** Heavy rain (high PoP / measurable QPF) shifts play-calling
toward runs, shortens the passing game, and raises fumble rates. Prop
lines for `player_rush_attempts` / `player_pass_attempts` are set from
neutral-weather projections days out; the forecast available 24h before
kickoff contains information the opening prop did not.

**Why mispriced.** Weather adjustments in props are concentrated on totals
and kicking; play-mix props are a quieter corner. The forecast itself
(IEM MOS, spike-verified) is free and timestamp-safe, so the information
was genuinely available pre-kickoff. (Distinct from dead H-N2: different
variable — precipitation, not wind — different market — props, not totals
— different mechanism — play-calling shift, not kicking/passing
efficiency.)

**Information cutoff.** IEM MOS GFS `p06`/`q06` at the kickoff-valid time,
from the mechanically latest cycle with `runtime + 4h ≤ kickoff − 24h`
(the H-N2 gate's timestamp machinery, reused for a different variable).
Outdoor stadiums only; domes excluded wholesale.

**Primary endpoint.** Prop residual on `player_rush_attempts` (over) and
`player_pass_attempts` (under) in high-precipitation games
(predeclared PoP/QPF threshold from the forecast distribution, not from
outcomes), 2023→2024 walk-forward.

**Free feasibility plan.** Pull MOS precipitation fields free for
2023–24 outdoor games (same IEM interface the H-N2 gate used); count games
above the predeclared PoP threshold → eligible N (outcome-blind);
predictor-side distribution reported. No market pull authorized; sketch
stops at the priced specification.

### Sketch E — West→east circadian displacement → visitor team totals

**Mechanism.** A team traveling from PT/MT to ET for a 1pm ET kickoff
plays at 10–11am body time. Reaction time, and plausibly offensive
execution, dip in the biological morning. The market prices travel
distance into the **spread**; visitor *scoring* (team total) is where a
circadian effect would appear if the spread adjustment already absorbed
the win-probability component.

**Why mispriced.** Circadian effects are well-documented in sports science
but are not an input to bookmaking models in any granular form; the
travel adjustment in the line is a distance heuristic, not a body-clock
model. (Distinct from dead H-N3: different information — time-zone
displacement, not rest days — different market — visitor team totals, not
1H derivatives.)

**Information cutoff.** Schedules only, preseason-known (SAFE): visitor
crosses ≥2 time zones AND kickoff 1pm ET.

**Primary endpoint.** `(visitor actual − visitor market team total) ~
circadian-displacement indicator`, 2022–2024.

**Free feasibility plan (computed, schedule facts only).** Eligible
treatment N = **70** (2022: 16, 2023: 28, 2024: 26); control ≈ 745.
Binary-treatment MDE ≈ 2.4865 × 9.5 × √(1/70 + 1/745) ≈ **2.9 points**
on the visitor team total — powered only for very large effects. Honest
read: likely underpowered as a standalone one-shot test; keep as a
sketch unless a pre-specified pooled specification (continuous displacement
hours, all 206 ≥2-zone trips) is justified at preregistration time. No
market pull needed for the feasibility verdict (team totals are
additional-market, but the N bound is already decisive).

---

## 4. Non-goals (deliberately not pursued)

- **Any QB-injury-news framing** — burned by H-N1; the news-repricing
  mechanism is consumed. Non-QB injury burden was considered and parked
  as too close to H-N1's market set (`team_totals` overlap).
- **Wind (any variable, any market)** — burned by H-N2. Precipitation and
  temperature are different physical mechanisms, not wind reformulations.
- **Rest days / bye / short week** — burned by H-N3. Time-zone
  displacement (Sketch E) is a different input with a different market.
- **Tempo / pace** — burned by H-N4. Red-zone conversion (Sketch B) is a
  different characteristic with a different market.
- **Alt-line arbitrage / internal consistency** — burned by H-N5; no
  information edge exists there by construction.
- **Officiating crews** — burned by H-N6; crew information is consumed.
- **Divisional "familiarity" unders, lookahead/letdown spots** — not
  mechanically preregistrable without subjective spot definitions;
  likely priced.
- **Realized weather as a pregame feature** — leakage, permanently closed
  (nflverse `temp`/`wind` columns, ERA5, METAR-as-forecast).
- **Prop/alt/period families on 2022 data** — no provider history before
  2023-05-03; any such family is 2023–2024 walk-forward by construction.
- **Paid historical market pulls** — no credit expenditure is authorized
  by this document; every sketch stops at a priced pull specification.
- **NCAAF crossover** — separate lane, separate firewall (2026-11-01);
  nothing here touches it.
- **Coaching/playcaller changes as a standalone family** — as-of
  semantics for "new scheme" are not mechanically definable from free
  data at the required precision; parked.

---

*Inventory + sketches only. Zero API credits spent. Zero outcomes
inspected. No preregistrations written, no tests run, no models changed.*
