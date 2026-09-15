# Preregistration: NCAAF market-structure map F1

**Status: PREREGISTERED — no results viewed. Analysis code does not yet exist.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-15
- Phase: F (market-efficiency map), under the 2026-09-15 contract amendment
  (`nfl-edge/docs/ncaaf-plan.md` §7).
- Dataset (frozen): `docs/ncaaf-dataset-freeze.md`
- Dataset fingerprint: `684c58410968c440a6d7500582ac9ecf`
- Scope: seasons **2022–2024 only** (weeks 0–14), markets FULL_GAME_SPREAD and
  FULL_GAME_TOTAL, regions us,eu. **2025 is sealed and excluded.**
- Question: what does the market's own structure look like — where do books
  disagree, how do lines move across the weekly snapshot windows, and how does
  Pinnacle sit relative to consensus? This establishes the baseline every
  later challenger must beat. No modeling, no thresholds, no picks.

## Definitions (pre-declared)

- **Game identity:** `provider_event_id`. Validated: within each
  `provider_event_id`, `home_team`/`away_team`/`kickoff` must be constant;
  violations are counted and reported, and the event is dropped.
- **Decision windows:** the planned snapshot instants mapped from each quote's
  `observed_at` to the nearest planned `requested_at` within 1800s
  (same rule as the audit). Labels: `early` = Wed 12:00 UTC, `mid` = Fri 20:00
  UTC, `late` = Sat 15:00 UTC. Quotes not matching any planned instant are
  excluded and counted.
- **Season filter:** a quote belongs to 2022–24 iff its matched planned
  instant is in seasons 2022, 2023, or 2024. 2025 quotes are never read
  (the SQL filters on the matched season set).
- **Pair selection:** per (game, window, book, market): the book's two-sided
  pair at its modal line within the window; ties broken by closeness to the
  cross-book median line at that window; fully deterministic.
- **Reference selection:** spreads → home-team side; totals → Over.
- **Fair probabilities:** the stored `fair_probability` (multiplicative
  de-vig, tournament-selected, computed at backfill from the book's own pair).
- **Consensus line:** cross-book median of the selected pair lines at
  (game, window, market), all books.
- **Freeze verification before running:** distinct planned snapshots matched
  in 2022–24 == 135 (45 × 3 seasons), else the run aborts. Quote count is
  recorded, not gated.
- **Execution:** read-only SELECTs against `ncaaf_edge_historical_quotes`
  only. Zero Odds API credits; no provider HTTP calls.
- **Point-in-time safety:** a window's quotes are compared only against
  information at or before that window. Later windows are never inputs to
  earlier-window statistics (movement is ex-post description, not a signal).

## Planned outputs (descriptive; no hypothesis tests)

1. **Coverage:** events × windows with ≥1 / ≥5 / ≥10 books, by season and
   market; per-book window presence.
2. **Disagreement:** per (game, window, market) with ≥2 books: std of
   reference-side fair probability across books; range of lines. Report
   mean / p50 / p90 by market × window × season, and by spread-magnitude
   bucket (|median line| in [0,3), [3,7), [7,14), [14,∞)) and total-range
   bucket (median total in (−∞,45), [45,55), [55,65), [65,∞)).
3. **Line movement:** per (game, market, book) present in all three windows:
   |line_late − line_early|, |line_mid − line_early|, |line_late − line_mid|;
   same for the consensus line. Report mean / p50 / p90 by market.
4. **Pinnacle vs consensus:** per (game, window, market) with Pinnacle present:
   |pinnacle_line − consensus_line| vs the mean |book_line − consensus_line|
   over other books (paired within event-window-market). Report the paired
   mean difference and its distribution.
5. **Anomaly counts:** game-identity violations, unmatched quotes, windows
   with <2 books.

## Interpretation guardrails

- This is a map, not a test: no p-values, no winner selection, no thresholds.
- Any "interesting" pattern found here becomes a hypothesis for a *separately
  preregistered* challenger in Phase H — it is not evidence of an edge.
- Findings must be reported with the full distribution (not just the mean)
  and with the sample sizes behind each cell.

## Amendment A1 (2026-09-15, before any results viewed)

- Season scope is enforced by kickoff-derived season (Aug–Dec → that year,
  Jan–Jul bowls → prior year); rows outside 2022–24 are excluded before
  window matching. 2025 never enters the analysis.
- Game-identity validation is scoped per (provider_event_id, season): the
  provider reuses event ids across seasons, which is legitimate and must not
  count as a violation.
