# Preregistration — NCAAF market vs outcomes F(b)

- **Family:** market-outcomes-f2
- **Status:** preregistered 2026-09-15, before any outcomes viewed
- **Scope:** frozen dataset fp `684c58410968c440a6d7500582ac9ecf`,
  kickoff-seasons **2022, 2023, 2024 only**. 2025 is sealed: the scores fetch
  requests CFBD years 2022–2024 only; no 2025 game is ever downloaded.
- **2026 season:** untouched, out of scope.

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
