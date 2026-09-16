# NFL canonical identity — match report

**Date:** 2026-09-16
**Run:** `35041420559` (workflow `canonical-identity`, `sport=nfl`, seasons 2022–2024)
**Code commit:** `6f20168c601d`
**Result:** **PASS** — deterministic identity layer established at the NCAAF bar.

## Headline

| Metric | Value |
|---|---|
| Odds events observed (all) | 1,511 |
| In scope (commence year 2022–2024) | 1,482 |
| Matched | 1,478 |
| **Match rate (in-scope event rows)** | **99.73%** |
| Ambiguous | **0** |
| Unmatched: no_alias (name not in alias table) | **0** |
| Unmatched: no_match (teams resolved, no game ±1 day) | 4 |
| Unmatched: outside_seasons (all 2025) | 29 |
| nflverse games 2022–2024 | 854 (815 REG + 39 postseason) |
| nflverse REG games matched | 799 / 815 (98.0%) |

Match types: `alias_date` 1,162 · `alias_date_shift` 316 · `swapped` 0 · `swapped_date_shift` 0.
15 unit tests in `nfl-edge/tests/test_nfl_identity.py`, all green.

## Method (deterministic, no fuzzy)

1. Odds full team names resolve **only** via the explicit 32-entry alias table
   (`nfl-edge/ops/nfl_team_aliases.json`, Odds name → nflverse abbreviation).
   Absent names → `no_alias`, never guessed. Verified: every alias value
   appears in the bundled nflverse data; the Rams are `LA`, not `LAR`.
2. nflverse side compared verbatim (uppercase/strip).
3. Date: exact UTC commence date first, then deterministic −1/+1 day fallback
   (316 matches — Sunday/Monday-night games landing on the next UTC day),
   then swapped home/away as a separate labeled attempt.
4. >1 candidate → `ambiguous`, preserved, never auto-resolved.

## Multiplicity finding (schema consequence)

The Odds provider **re-issued event ids across historical snapshots**: one
nflverse game maps to a median of 2 (max 5) distinct Odds event ids —
1,011 unique `(nflverse_game_id, odds_event_id)` pairs from 1,478 matched
rows. The mapping is therefore **1:n**, and `migrations/017_nfl_game_identity.sql`
uses a composite primary key on `(nflverse_game_id, odds_event_id)`.
Downstream quote joins must map *any* observed `odds_event_id` to its
`nflverse_game_id`; treating the Odds event id as a stable game key is wrong.

## The 4 no_match events (all legitimate, none forced)

- `bfc194ec…` 2024-12-20 Bengals(home) vs Browns — real game 2024-12-22;
  provider `commence_time` 2 days off (data quirk).
- `2564203d…` 2024-12-22 Chargers(home) vs Broncos — real game 2024-12-19;
  provider `commence_time` 3 days off (data quirk).
- `7a1a8c9b…` 2023-12-19 Patriots(home) vs Chiefs — real game 2023-12-17;
  provider `commence_time` 2 days off (data quirk).
- `20118ff7…` 2023-01-03 Bengals(home) vs Bills — the suspended
  Damar Hamlin game; never completed, absent from nflverse schedules.

Widening the date window beyond ±1 day to capture the first three would be
data-fitting. The deterministic rule stands.

## Coverage gaps (not matching failures)

- **All 39 postseason games** (WC/DIV/CON/SB, 2022–2024) have no Odds events:
  the frozen dataset's historical snapshots cover regular-season weeks only.
- **All 16 unmatched REG games are 2024 Week 18** (gamedays 2025-01-04/05):
  absent from the frozen backfill.
- 29 events with 2025 commence dates excluded as `outside_seasons`.

## Shared-infrastructure statement

This join is the identity backbone for the new NFL research lane: **injuries,
rosters, officiating, play-by-play-derived features, and weather** all key off
`nflverse_game_id` ↔ `odds_event_id` via this layer. `017_nfl_game_identity.sql`
carries extensible hook columns (`injury_report_key`, `weather_station_key`,
`nflverse_gsis`). The bundled schedules (`nfl-edge/ops/nflverse_schedules_2022_2024.json`)
include per-game **stadium** names, which is the input for the deterministic
stadium → IEM MOS station mapping. **The pending trigger — "stand up the
forecast archive agent as soon as the identity/mapping deliverable validates" —
is now satisfied.**

## Files

- `nfl-edge/ops/nfl_team_aliases.json` — explicit 32-entry alias table
- `nfl-edge/ops/nflverse_schedules_2022_2024.json` — bundled nflverse schedules
  (854 games; `temp`/`wind` realized-weather columns deliberately excluded)
- `nfl-edge/ops/nfl_canonical_identity.py` — deterministic matcher (read-only on
  `public.nfl_edge_market_history`; frozen dataset/model/shadow untouched)
- `nfl-edge/ops/migrations/017_nfl_game_identity.sql` — extensible mapping table
- `nfl-edge/tests/test_nfl_identity.py` — 15 unit tests
- `.github/workflows/canonical-identity.yml` — `sport=nfl` mode
- CI artifact `canonical-identity-nfl` on run `35041420559` — full mapping JSON
  with matched / ambiguous / unmatched lists
