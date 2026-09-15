# Preregistration — NCAAF market vs outcomes F(b)

- **Family:** market-outcomes-f2
- **Status:** preregistered 2026-09-15; amended 2026-09-15 (A2, before any
  aggregate outcome analysis was viewed — see disclosure below)
- **Scope:** frozen dataset fp `684c58410968c440a6d7500582ac9ecf`,
  kickoff-seasons **2022, 2023, 2024 only**. 2025 is sealed: the scores fetch
  requests CFBD years 2022–2024 only; no 2025 game is ever downloaded.
- **2026 season:** untouched, out of scope.

## Disclosure (2026-09-15)

After the CFBD credential was connected and before this preregistration was
written, a single `/games` record (one 2024 game, score included) was printed
to the terminal as a schema smoke test. No aggregate outcome analysis was
viewed before preregistration. The original status line's claim that no
outcomes were viewed beforehand is therefore amended here for accuracy.

## Purpose

Descriptive baseline (like F(a)). Measure how well the de-vigged consensus
market prices in the frozen odds data predicted realized game outcomes.
Selects nothing; feeds Phase G v0 design. No model, no thresholds, no picks,
no p-values used for selection.

## Data

- Odds: same read-only selection as F(a) (`ops/market_structure.py`
  `select_pairs`, prereg amendment A1): one selected pair per
  event×window×book×market, 2022–24 kickoff-seasons only.
- Scores: CFBD `/games` for years 2022–2024, seasonTypes regular+postseason,
  fetched fresh in the workflow run. Cached as a run artifact, not the DB.
- 2026 data and 2025 data are never fetched or viewed.

## Matching (point-in-time safe)

Outcomes are results, never inputs. Events match CFBD games on
identity only: normalized team names (lowercase, punctuation/whitespace
stripped) plus kickoff within ±36h of CFBD `startDate`. Deterministic
tiebreak: smallest |kickoff − startDate|. Matches on scores or outcomes are
forbidden. Conflicts (multiple CFBD candidates) are reported in integrity and
the event is excluded. Only `completed=true` games with non-null points count;
anything else is unmatched-with-reason.

## Consensus definition

Per event×window×market: median line across books; median de-vigged fair
probability of the home side covering (spreads) or the over hitting (totals).
Fair probabilities come from same-book de-vigging already in the frozen data.
Push = margin + median_line == 0 (spreads) or total == median_line (totals);
pushes are counted and excluded from rate denominators.

## Planned outputs

1. **Calibration:** de-vigged consensus home-cover/over probability in
   decile bins vs realized cover/over rate, per window (early/mid/late).
   Empirical rates with n only — no fitted models, no significance tests.
2. **Splits:** by season; by matchup class from CFBD classifications
   (FBS-vs-FBS, FBS-vs-FCS, other/unknown — never pooled); by conference
   group of the home team (P4, other FBS, FCS, unknown); by line bucket
   (spreads: |line| in [0,3), [3,7), [7,14), [14,∞); totals: line in
   <45, 45–55, 55–65, ≥65); by window.
3. **Closing efficiency:** mean log-loss and Brier score of consensus fair
   prob vs realized cover/over, early window vs late window. Descriptive
   comparison only (paired, same events); no hypothesis test for selection.
4. **Home-field / totals bias checks:** overall cover rate at pick'em-ish
   lines, over rate — reported as rates, not tested.
5. **Integrity section:** CFBD coverage (matched / unmatched-with-reason /
   conflicted events), null-score count, classification coverage, pushes,
   per-season matched event counts. Any integrity failure (e.g. <95% of
   in-scope events matched with a reason) is reported, not patched silently.

## Guardrails

- Descriptive only. If the market looks perfectly efficient, that is the
  finding — no threshold is moved to manufacture a lead.
- Leads for Phase H challengers are noted as leads, never as findings.
- The script fails closed: any unhandled exception or integrity breach
  produces no artifact and a failed run.

## Amendment A2 — identity matching (2026-09-15, before any aggregate
outcome analysis was viewed)

Three F2 workflow runs failed the 95% match-integrity gate at 94.5%
(2,334/2,470 events). Failure diagnostics (team-name pairs and kickoff
proximity only — no scores, no outcomes) showed the misses are deterministic
identity mismatches between the odds provider's team names and CFBD's, plus
a few provider-side data errors. This amendment preregisters the matching
repair. The 95% gate is unchanged; nothing is lowered to force success.

**Frozen school-name alias table** (provider-style → CFBD canonical,
applied by normalized-prefix substitution before matching; frozen as of this
amendment, any addition is a further amendment):

- `appalachianstate` → `appstate` (CFBD "App State")
- `albanystate` → `albanystate` (distinct DII school; shadows `albany` below)
- `albany` → `ualbany` (CFBD "UAlbany")
- `umass` → `massachusetts` (CFBD "Massachusetts")
- `citadel` → `thecitadel` (CFBD "The Citadel")
- `southeasternlouisiana` → `selouisiana` (CFBD "SE Louisiana")
- `houstonbaptist` → `houstonchristian` (school renamed; CFBD "Houston Christian")
- `youngstownst` → `youngstownstate` (CFBD "Youngstown State")
- `southernmississippi` → `southernmiss` (CFBD "Southern Miss")
- `texasamcommerce` → `easttexasam` (renamed Nov 2024; CFBD "East Texas A&M")
- `liu` → `longislanduniversity` (CFBD "Long Island University")
- `stfrancis` → `saintfrancis` (CFBD "Saint Francis", the PA school)

Normalization also maps `&` to `and` (CFBD "William & Mary" vs provider
"William and Mary"). Mascot-suffix matching is longest-prefix (see matching
rule below), which subsumes the old prefix fallback.

**Swapped home/away (neutral-site designation differences).** When the
provider and CFBD list the same two teams (after aliases) at the same
kickoff (±36h) but with home/away reversed (e.g. 2022 LSU–Florida State,
2022 Clemson–Georgia Tech, 2022 Texas Southern–Southern, 2023 Army–Navy),
the event matches with orientation recorded. Scores are realigned to the
provider's orientation: the provider's home team's points are used for the
home margin, and the provider's home team's conference/classification for
splits. The spread line is quoted on the provider's home team, so this keeps
cover determination consistent.

**Matching rule (final).** Each provider team name is resolved to the
longest CFBD school name in normalized-prefix relation (after aliasing).
Longest-prefix wins: e.g. provider "Texas A&M" (`texasam...`) resolves to
CFBD `texasam` (Texas A&M), never to the shorter `texas` — this was verified
to block a false match of the provider's phantom 2024-12-07 Georgia vs
Texas A&M listing against CFBD's real Texas vs Georgia game. A CFBD game
matches when both provider teams resolve to its two schools and kickoffs
agree within ±36h. The `albany` alias does not fire on `albanystate`
(Albany State is a distinct DII school and resolves to itself).

**Unmatched-with-reason codes** (reported in integrity, never patched):

- `phantom` — neither provider team resolves to any CFBD school in-season.
- `kickoff_miss` — both teams resolve to real schools, but no such pairing
  near the kickoff (±36h): postponed beyond tolerance (2022 UCF–SMU, moved
  78h by Hurricane Ian), or a game that never happened.
- `team_mislabel` — a CFBD game at a close kickoff shares one team but the
  pairing differs: provider team-name error (2023-09-16 "UCLA vs North
  Carolina Tar Heels" for the real UCLA vs North Carolina Central game) or
  speculative listings (wrong 2024-12-07/08 championship matchups).
  Excluded rather than guessed.
- `not_completed` / `null_scores` — pairing and kickoff match, but CFBD
  marks the game not completed or scores are null (e.g. 2024 App State vs
  Liberty, canceled for Hurricane Helene, `completed=false`).
- `conflict` — multiple candidates tie on |kickoff − startDate|; excluded.

**Bug fixes (same amendment):** the paired early/late efficiency comparison
keys on `(event_id, season)` (provider IDs can repeat across seasons);
`matchup_class` labels `FBSvFCS` only when exactly one team is FBS and one is
FCS (FCS-vs-FCS is `other/unknown`).

Limitation noted: prefix substitution is one-directional and could in
principle collide if the provider ever listed a school whose name extends an
alias key (e.g. "Albany State" vs the `albany`→`ualbany` alias). The provider
lists no such school in 2022–2024; the ±36h kickoff check is a second factor.

## Amendment A1 (2026-09-15, before any outcomes viewed)

Scores are NOT fetched in the workflow (the workflow cannot carry the CFBD
credential). Instead, CFBD /games for years 2022–2024 (regular+postseason)
were fetched once via the connected credential and frozen as an immutable
repo fixture:
`nfl-edge/model/research/fixtures/cfbd_games_{2022,2023,2024}.json`
(slimmed to identity/score fields, one file per season):
2022 sha256 `d700b9b6a3513abcabda0867b264dda739bdc8f0b16410463049051696b819b4`
(3,705 games),
2023 sha256 `870b48ad274f164bcf06ec700c19307a12920e7d28e1931bb7b7f2985c596849`
(3,734 games),
2024 sha256 `1ce6efa1f7001347d94ef0f421c1cdeeb1cd60c1ca1f9235a29192fa30eade1e`
(3,801 games). The analysis script additionally refuses any game whose
season is not in (2022, 2023, 2024), so 2025/2026 can never enter even if the
fixture were extended. The workflow reads the fixture from the repo.
