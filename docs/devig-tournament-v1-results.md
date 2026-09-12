# De-vig tournament v1 — results

**Status:** completed 2026-09-12 · **Role: SHADOW** · nothing promoted, nothing advanced.

## Provenance

- Preregistration: `docs/preregistrations/devig-tournament-v1.md` (commit `59ed4454329b7f14f86d6b7bc6bb0576d40d4efc`), written before any execution.
- Implementation: `nfl-edge/research/devig.py`, `nfl-edge/research/devig_tournament.py`, tests `nfl-edge/tests/test_devig.py` (15 tests), workflow `.github/workflows/devig-tournament.yml`.
- Code commits: `ecdfed0dbebd` (tournament), `576437e14c98` (test fix for `model/research` shadowing the top-level `research` package name in the CI suite). CI green on both (`34694861272` success).
- Tournament execution: workflow run `34694931824` (success), dispatched exactly once. Artifact: `devig-tournament-v1` (run artifact `10298414146`).
- Dataset: frozen 2022–2024 historical set. Runner verified 162/162 snapshots and 291,586/291,586 quotes against the freeze record. The stored 32-hex fingerprint (`43f853a44bf93937d85149ca5fd7241b`) is a different fingerprint construction than the runner's SHA-256 content hash (`28aa7517…33173d12`), so `fingerprint_match=false` is expected and not an integrity failure; counts match exactly.
- Zero Odds API credits spent (read-only Neon queries).

## Declared primary metric: UNTESTABLE this round (as preregistered)

Mean log-loss vs settled outcomes could not be computed: the historical tables (`nfl_edge_market_history`, `nfl_edge_historical_quotes`) have no outcome/score columns, and the backfill pipeline only ever called the historical *odds* endpoint — scores were never collected. Final scores exist only on `nfl_edge_picks` (migration `004_settlement.sql`), which covers forward shadow picks, not 2022–2024 history. This is untestable **by construction**, not just by schema inspection.

## Confirmatory round (preregistered): closing-convergence MSE, LOBO

- **Events:** 937 · **cells:** 108,761 per method (complete-case; every cell has all three methods)
  - *Audit correction (2026-09-12):* the 937 "events" were keyed on unstable provider ids; canonical game identity gives 825 games (816 regular-season + 9 playoff-weekend listings). Re-run with fixed keying: 812 games, 109,985 cells, decision unchanged. See "Event-identity audit" below.
- Consensus: method-specific, leave-one-book-out, ≥2 other books at the latest closing snapshot. Strict: target book never enters its own consensus (tested).
- Inference: paired event-level t-tests (n=937 events), 3 pairwise comparisons, Holm-Bonferroni family-wise α=0.05.

| method | mean SE | RMSE | n cells |
|---|---|---|---|
| multiplicative | 0.00014466 | 0.01202745 | 108,761 |
| additive | 0.00015935 | 0.01262330 | 108,761 |
| power | 0.00016778 | 0.01295290 | 108,761 |

Pairwise (mean diff of event-level MSE, a−b):

| comparison | mean diff | p (Holm) | significant |
|---|---|---|---|
| additive − multiplicative | +1.33e-05 | 4.9e-75 | yes |
| additive − power | −7.67e-06 | 1.7e-48 | yes |
| multiplicative − power | −2.09e-05 | 1.7e-63 | yes |

**Decision: `multiplicative`.** It significantly beats both alternatives after Holm correction, satisfying the preregistered winner rule.

### Honest effect-size read (exploratory)

The win is statistically decisive but practically small: multiplicative's RMSE is ~0.0006 probability points lower than additive's (≈9% lower MSE in relative terms, ≈0.06 probability points absolute). All three methods agree closely — expected, since two-outcome de-vig methods differ only in how they split a small vig. The ranking is consistent across markets (spread and total) and is not driven by any single book (per-book splits in artifact).

## What this decision does and does not mean

- **Does:** selects **multiplicative** as the de-vig formula the forward-shadow calibration pipeline will use when computing no-vig probabilities. A pipeline configuration choice, still SHADOW.
- **Does NOT:** constitute a betting edge, a model promotion, or evidence that any method beats the market. The confirmatory metric measures convergence to the *closing consensus*, not predictive skill vs outcomes. No method was tested against settled results, and none can be on this dataset.
- **Failed/null results preserved:** the primary metric is recorded as untestable; no method was crowned "best predictor" of anything.

## Caveats and follow-ups

1. **Event count (937) exceeds the expected ~816 regular-season games (2022–2024).** Likely cause: the same game keyed as two events across snapshots (team-name or home/away inconsistencies). The paired within-event design is unaffected in structure, but some games may be double-counted, mildly inflating precision. P-values are ~1e-48..1e-75, so dedup would not change the decision — but an event-identity audit is warranted before reusing this event keying downstream.
2. **Calibration tables** in the artifact are convergence-to-close diagnostics (mean de-vigged prob vs mean closing consensus per decile), not outcome calibration. ~40% of cells sit exactly at 0.50/0.50 (the −110/−110 mass).
3. **Shin equivalence:** Shin was excluded from the candidate set because it is algebraically identical to additive for two-outcome markets (numerical check: max difference ~2.8e-16). Not a missing comparison.
4. The `z` column in pairwise tests is the paired t-statistic (n=937, t≈z).

## Event-identity audit (2026-09-12) — root cause found and fixed

**Root cause:** The Odds API re-issues `provider_event_id` for the same real game between snapshots. The tournament keyed directly on that unstable id, so one game was split into multiple longitudinal series: 1,040 distinct provider ids, 375 of them with multiple kickoff timestamps (seconds-to-days of jitter, plus flex-schedule moves). Zero provider ids span two (home, away) pairs — the ids are team-consistent, just temporally unstable. Grouping by (home, away, kickoff day) shows 847 apparent games with 206 having two provider ids, and **no** game/snapshot ever contains two ids simultaneously: a clean temporal id handoff, not same-snapshot double ingestion. Example: Chiefs–Chargers 2022-09-16 appears under one id for snapshots 2022-09-07→11, then a replacement id on 2022-09-14.

**Fix (committed, tested):** canonical game identity = `(home_team, away_team)` + kickoff-proximity clustering (7-day gap starts a new game). The threshold is data-validated, not arbitrary: within-game kickoff gaps are all ≤48h (flex moves); between-game gaps are all ≥528h (22 days). `run_tournament` now groups by canonical game, keeps every `observed_at` as a distinct snapshot observation, and records `provider_event_ids` on each cell for provenance. The held-out book still never enters its own consensus (strict LOBO unchanged).

**Reconciliation (read-only Neon queries, frozen dataset):**

| unit | count |
|---|---|
| distinct provider ids | 1,040 |
| tournament "events" (old keying) | 937 |
| canonical games (new keying) | 825 |
| phantom splits from id churn (937 − 825) | 112 |
| of which: regular-season games | 816 = 272 × 3 seasons, exact |
| of which: distinct Wild Card-weekend listings (2025-01-12 kickoffs, present in late 2024 snapshots) | 9 |

No canonical game mixes two (home, away) pairs. The 9 extra games are genuinely distinct listings (6 actual + 3 speculative playoff matchups), not duplication.

**Re-run of the tournament core (local, read-only, zero credits) with the fixed keying:** 812 games with ≥1 complete-case cell, 109,985 cells (vs 108,761 before — merging ids unlocked old-id prediction snapshots against replacement-id closing snapshots). Decision **unchanged**: multiplicative still beats both alternatives after Holm correction (additive−multiplicative p≈2.7e-78; additive−power p≈1.4e-50; multiplicative−power p≈3.3e-66). RMSE: multiplicative 0.011995, additive 0.012589, power 0.012918 — same ranking, same small practical effect (~0.06 probability points).

**Regression tests:** 5 new tests in `nfl-edge/tests/test_devig.py` (`TestCanonicalGameIdentity`): id-churn merge to one game with both ids on cells, flex kickoff move stays one game, divisional rematch stays two games, key determinism, un-keyable quotes excluded. Full suite: 274 passed (2 pre-existing sklearn collection errors in unrelated files, unchanged).

## Ledger

- Canonical entry appended to `nfl-edge/model/research/results/experiments.jsonl`.
- Registry updated: `nfl-edge/research/experiments.json` (`devig-tournament-v1`, `completed_method_selected_shadow_only`).
- Full machine-readable results: workflow artifact `devig-tournament-v1` on run `34694931824`.
