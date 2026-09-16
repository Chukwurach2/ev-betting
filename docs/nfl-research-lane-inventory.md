# NFL new research lane — data & market inventory

**Date:** 2026-09-15. **Status:** INVENTORY ONLY.
**Role:** opens the new independent NFL research lane (richer pregame
information and/or different markets). No hypothesis was tested, no model
was fit, nothing was preregistered, and nothing in the frozen NFL v1.3 /
forward-shadow experiment was touched. Frozen dataset fingerprint
`43f853a44bf93937d85149ca5fd7241b` (spreads+totals, 2022–2024) is unchanged
and is NOT reused by this lane.

**API credits used: 2.** Both were 1-credit live `event markets` probes
(us + eu regions, one upcoming game). `/v4/sports` and `/v4/events` are
free. Zero historical calls were made; market-coverage history below is
from the provider's published documentation, not from paid probes.

**Sources:** nflverse via `nflreadpy` function list and nflverse-data
releases (public GitHub, free); The Odds API v4 docs
(`liveapi/guides/v4`, `sports-odds-data/betting-markets.html`); two live
coverage probes on 2026-09-15 (see §B).

---

## A. Pregame data inventory — point-in-time verdicts (NFL 2022–2024)

Legend: **SAFE** = reconstructable as-of the decision window with no
look-ahead. **SAFE-WITH-LAG** = safe only if features are strictly lagged
(weeks < t for a week-t game; never season aggregates that include the
game). **CONDITIONAL** = safe only under a stated as-of rule.
**UNVERIFIED** = not yet shown point-in-time safe; do not model until a
dedicated spike clears it.

| # | Source | What it gives | Fetch path | Coverage (approx; verify at implementation) | Verdict |
|---|---|---|---|---|---|
| 1 | nflverse play-by-play | Every play 1999+, EPA/WP/air yards/success rate, drive/pace fields | `load_pbp()` / parquet releases | 1999–present; some columns (air yards, WP) thinner pre-2011 | **SAFE-WITH-LAG** |
| 2 | nflverse schedules | Dates, kickoff times, stadium, roof, surface, home/away, results | `load_schedules()` | 1999–present | **SAFE** for schedule facts (released May, pre-season). **LEAKAGE TRAP:** `temp`/`wind` columns are *realized* game-time weather — never a pregame feature |
| 3 | nflverse injuries | Weekly practice participation (DNP/Limited/Full) + game status (Out/Doubtful/Questionable) | `load_injuries()` | weekly, ~2009–present | **CONDITIONAL** — see as-of rule below |
| 4 | nflverse rosters (weekly) + snap counts | Who was on the roster / snap share by week | `load_rosters_weekly()`, `load_snap_counts()` | weekly rosters ~2019+, snaps 2012+ | **SAFE-WITH-LAG** (week t−1 or earlier for a week-t game) |
| 5 | nflverse depth charts | Positional depth ordering | `load_depth_charts()` | recent seasons only | **CONDITIONAL** — as-of publication semantics must be verified before use |
| 6 | nflverse NextGen + FTN charting | Advanced tracking / charted stats | `load_nextgen_stats()`, `load_ftn_charting()` | NextGen 2016+, FTN 2022+ | **SAFE-WITH-LAG** (published post-game) |
| 7 | nflverse officials | Referee crew per game | `load_officials()` | 2016+ (verify) | **SAFE** — crews published pregame (typically Tue); tendencies computed from lagged seasons only |
| 8 | Rest / travel (derived) | Days rest, bye, Thursday-game short week, travel miles, altitude/London | schedules + static stadium coordinates | fully derivable | **SAFE** — all inputs known before the season |
| 9 | Weather **forecast** | Pre-kickoff forecast wind/temp/precip | **no verified free archive** | — | **UNVERIFIED — do not model yet** |
| 10 | Weather **realized** | Station/reanalysis temp/wind/precip | Open-Meteo ERA5, nflverse schedule columns | full history | Fetchable but **LEAKS** as a pregame feature (it is the outcome's weather, not the forecast) |
| 11 | Contracts / combine / trades | OTC contracts, combine measurables, trade log | `load_contracts()`, `load_combine()`, `load_trades()` | varies | **SAFE** but low expected value; offseason-timed, as-of ambiguous |

### As-of rules that make each conditional source safe

- **Injuries (#3):** NFL game-status reports are final ~48h before kickoff
  (Out/Doubtful/Questionable); practice reports land Wed/Thu/Fri. A
  decision window *after* the final status (e.g. day-before/day-of) may use
  the final designations; an early-week window (Tue/Wed) may use only
  practice reports published to date. Forbidden: season-long injury
  aggregates, retroactive IR designations, or using the final status in an
  early-week window.
- **Play-by-play (#1):** features for a week-t game may use plays from weeks
  < t only. Season-to-date aggregates must be recomputed with the cutoff;
  never use nflverse's precomputed season aggregates for the current week.
  Postseason games use regular-season-lagged features only.
- **Rosters/snaps (#4):** week t−1 (or earlier) snapshots for a week-t game.
- **Officials (#7):** crew identity is pregame-safe; crew *tendencies*
  (flags/game, home-bias proxies) must be computed from seasons strictly
  before the game.

### Weather: the one hard gap (dedicated spike required)

No free, verified archive of point-in-time *forecasts* was found. Facts:

- NWS/NDFD serves only the *current* forecast ("most recently issued
  forecast for that valid time"); there is no public forecast archive.
- OpenWeatherMap "historical weather" and Open-Meteo ERA5 are *observed /
  reanalysis* — realized weather, i.e. leakage if used pregame.
- Candidate paths for a spike (none verified): Iowa Environmental Mesonet
  archived NWS text/grid products; NOAA HRRR/GFS model-run archives on AWS
  (reconstruct the forecast valid at kickoff from archived runs — heavy
  GRIB engineering); commercial forecast-history products.
- Fallback that is safe today: prospectively snapshot day-before forecasts
  going forward (forward-only research; no 2022–2024 backtest).

**Verdict: weather is UNVERIFIED. Any weather hypothesis family is blocked
until the archive spike lands.** This is the single biggest data risk in
the new lane.

### What exists locally vs fetchable

- Locally in the workspace: no nflverse data files (only loader code that
  fetches from `github.com/nflverse/nflverse-data` releases at runtime).
- Fetchable free, no key: all feeds above via `nflreadpy`/`nflreadr` or
  direct parquet from nflverse-data releases.

---

## B. Market inventory — The Odds API, NFL (`americanfootball_nfl`)

### Market classes and where they live

| Class | Example keys | Live endpoint | Live cost | Historical availability | Historical cost |
|---|---|---|---|---|---|
| Featured | `h2h`, `spreads`, `totals` | `/odds` (all events) | 1 cr / region / market | From **2020-06-06** (5-min snapshots from Sep 2022) | 10 cr / region / market / timestamp |
| Team totals / alternates | `team_totals`, `alternate_spreads`, `alternate_totals`, `alternate_team_totals` | per-event `/events/{id}/odds` | 1 cr / region / market | **After 2023-05-03 only** — no 2022 coverage | 10 cr / region / market / timestamp |
| Period (Q1–Q4, H1/H2) | `spreads_h1`, `totals_q1`, `h2h_h1`, `team_totals_h1`, + alternates | per-event | 1 cr / region / market | **After 2023-05-03 only** — no 2022 coverage | 10 cr / region / market / timestamp |
| Player props | `player_pass_yds`, `player_rush_yds`, `player_receptions`, `player_anytime_td`, `player_sacks`, … (~34 keys + `_alternate` variants) | per-event | 1 cr / region / market | **After 2023-05-03 only** — no 2022 coverage | 10 cr / region / market / timestamp |
| Futures | `outrights` (Super Bowl winner is a separate sport key) | `/odds` | 1 cr / region / market | From 2020-06-06 | 10 cr / region / market / timestamp |

Consequences for a 2022–2024 backtest window:

- **2022 season: featured markets only.** Player props, alternates, team
  totals, and period markets have no historical coverage before
  2023-05-03 (provider's documented cutoff).
- **2023 and 2024 seasons: full coverage** (featured + additional) at
  5-minute snapshot granularity via the historical per-event endpoint
  (paid plan only).
- Historical per-event odds are one event per request; backfilling
  alt/prop markets across a season is credit-heavy (illustrative: one
  timestamp × 16 games × 5 markets × 2 regions × 10 cr ≈ **16,000
  credits** per snapshot time — budget and preregister before any pull).

### Live coverage probe — 2026-09-15 (Lions @ Bills, 2026-09-18; 1 cr each)

US region, 11 bookmakers carry the game. Featured `h2h/spreads/totals`
present at all 11.

| Bookmaker | Markets (n) | Player props | Period markets | Notes |
|---|---|---|---|---|
| DraftKings | 81 | 33 | 36 | deepest; props + Q1–Q4/H1/H2 + alternates + team totals |
| FanDuel | 63 | 23 | 31 | deep |
| BetRivers | 47 | 26 | 14 | deep |
| Bovada | 43 | — | — | broad |
| BetMGM / Fanatics | 29 | — | — | moderate |
| WilliamHill_us / BetOnline | 22–23 | — | — | moderate |
| BetUS | 8 | — | — | thin |
| LowVig / MyBookie | 3 | 0 | 0 | featured only |

EU region — **Pinnacle: 26 markets**, including 10 player props
(`player_anytime_td`, `player_defensive_interceptions`,
`player_pass_attempts`, `player_pass_completions`, `player_pass_tds`,
`player_pass_yds`, `player_reception_yds`, `player_receptions`,
`player_rush_attempts`, `player_rush_yds`) and 10 period markets
(`spreads_h1/q1`, `totals_h1/q1`, `h2h_h1/q1`, plus alternate
spreads/totals h1/q1), plus `team_totals`, `alternate_spreads`,
`alternate_totals`. Other EU books are featured-only (1–5 markets).

Coverage gaps (findings, not failures):

1. Thin books (LowVig, MyBookie, most EU books) list featured markets
   only — multi-book consensus for props/periods rests on 3–5 US books.
2. Player-prop bookmaker overlap varies by prop; cross-book consensus is
   weaker than for featured markets.
3. Additional-market coverage is "expanding" per the provider — verify
   per-event via the 1-credit `event markets` endpoint before budgeting a
   backfill.
4. Historical additional-market data starts 2023-05-03: any hypothesis
   using props/alternates/periods can only train on 2023–2024 (two
   seasons), which constrains validation design (no 3-fold LOSO; use
   2023→2024 walk-forward or pooled with season blocks).

---

## C. Candidate NEW hypothesis families (preregistration-ready sketches)

All six start with **LOW prior plausibility** (stated explicitly per
family). Each is genuinely new information or a genuinely new market —
none is a transform of the frozen spread/total series. Nothing below is
preregistered or tested; these are sketches for the user to promote.

### H-N1. Injury-report shock in correlated player-prop markets
- **Prior:** LOW. Books move fast on QB news; residual mispricing would
  have to live in the cross-market adjustment, not the headline.
- **Information set:** nflverse weekly injuries + depth charts, as-of rules
  from §A (#3): use only reports published before the decision window.
- **Market:** `team_totals` and QB-correlated player props
  (`player_pass_yds`, `player_reception_yds`) at books quoting both.
- **Endpoint:** CLV of a post-news prop position vs the prop's closing
  line; secondary: calibration of injury-implied total adjustment.
- **Falsification:** no positive mean CLV after transaction-cost-agnostic
  thresholds; or the full-game total move fully explains prop moves
  (no incremental residual).

### H-N2. Forecast wind vs totals in outdoor stadiums
- **Prior:** LOW. Wind is the most-cited weather edge and therefore the
  most likely to be priced.
- **Information set:** pre-kickoff *forecast* wind speed/gusts (BLOCKED
  until the §A weather spike verifies a point-in-time archive; realized
  wind is forbidden).
- **Market:** `totals` and `totals_h1` at outdoor stadiums.
- **Endpoint:** OOS log-loss / calibration of a wind-adjusted total vs the
  market total; directional accuracy on high-wind games only.
- **Falsification:** market-implied wind discount matches or exceeds the
  estimated physical effect; no OOS improvement over the market total.

### H-N3. Rest/travel fatigue in first-half derivatives
- **Prior:** LOW. Rest edges are well-studied; any remainder likely lives
  in less-scrutinized derivatives.
- **Information set:** §A #8 — days rest incl. bye/Thursday/short-week,
  travel miles, altitude, international games; all preseason-known.
- **Market:** `spreads_h1` / `totals_h1` vs the full-game line.
- **Endpoint:** calibration of 1H lines conditional on rest differential;
  residual 1H margin after controlling for the full-game spread.
- **Falsification:** rest differential has no stable coefficient across
  leave-one-season-out folds (2022/2023/2024).

### H-N4. Lagged tempo vs first-half totals
- **Prior:** LOW. Pace is observable and heavily modeled publicly.
- **Information set:** §A #1 — neutral-script seconds-per-play and pass
  rate from lagged play-by-play (weeks < t only).
- **Market:** `totals_h1` (less efficient than full-game totals).
- **Endpoint:** OOS log-loss of a tempo-based 1H total vs the market 1H
  total.
- **Falsification:** no OOS log-loss improvement; or the full-game total
  already spans the tempo information (encompassing test fails).

### H-N5. Alternate-line internal consistency (key numbers)
- **Prior:** LOW. Alt lines are algorithmically generated from the same
  distribution; systematic deviation would be a pricing bug, not a
  persistent edge.
- **Information set:** featured `spreads` line + empirical final-margin
  distribution estimated from lagged seasons (key numbers 3/7 handled
  explicitly).
- **Market:** `alternate_spreads` / `alternate_totals` at 3+ books.
- **Endpoint:** CLV of alt-line positions vs the alt line's own close;
  deviation of alt-line implied probabilities from the no-arbitrage band.
- **Falsification:** deviations are within the no-arb band after vig, or
  vanish once key-number mass is modeled correctly.

### H-N6. Officiating-crew penalty tendency vs totals
- **Prior:** LOW. Crew effects are small and may be fully priced or
  unstable year to year.
- **Information set:** §A #7 — crew assignments (pregame-safe) +
  lagged-seasons penalty-rate tendencies (flags/game, automatic-first-down
  rate).
- **Market:** `totals`.
- **Endpoint:** residual correlation between crew penalty tendency and
  (realized total − market total) after spread/total controls.
- **Falsification:** no stable effect in leave-one-season-out validation;
  effect shrinks to zero with multi-season shrinkage.

---

## D. Recommended sequencing (no approval needed to read; approval needed to spend)

1. **Weather archive spike** (blocks H-N2): verify one of IEM archived
   NWS products / NOAA HRRR-GFS archives / commercial forecast-history.
   Pass criterion: demonstrate a retrievable forecast issued ≥12h before
   kickoff for 2022–2024 games. If it fails, H-N2 is dropped, not
   approximated with realized weather.
2. **NFL identity join:** nflverse `game_id`/gsis ↔ Odds API event ids
   (simpler than the NCAAF alias work; deterministic on date+teams).
3. **Backfill budgeting:** any family using props/alternates/periods needs
   a credit budget approved against §B costs (2023–2024 only; 2022 has no
   alt-market history). Do not pull before preregistration.
4. **Exposure ledger:** record any new data exposure (nflverse pulls,
   forecast snapshots) per the standing rule.
5. **Then:** promote 1–2 families to full preregistrations in
   `docs/preregistrations/`; families stay LOW prior until evidence moves
   them.

---
*Inventory-only. No picks, no model changes, no forward-shadow contact.*
