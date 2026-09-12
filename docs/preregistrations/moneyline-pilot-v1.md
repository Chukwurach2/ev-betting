# Preregistration: moneyline-pilot-v1 (NEW DATA — paid API)

**Status: PREREGISTERED — no results viewed. No data pulled. No analysis code run.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Question: does the moneyline market (new ground — never analyzed) show a
  tradeable cross-book deviation, and can the paid historical endpoint supply
  moneyline quotes at usable cost?
- Credit math (from `ops/backfill_history.py`): historical calls cost
  10 x markets x regions. Moneyline (`h2h`) x `us,eu` = **20 credits/snapshot**.

## Phase 1 — feasibility (HARD CAP: 60 credits)

Sample calls (markets=h2h, regions=us,eu, oddsFormat=american) at known-good
snapshot instants from the frozen dataset: 2024-09-04T12:00:00Z,
2024-10-12T12:00:00Z, 2024-11-16T12:00:00Z. Per call, record: HTTP status,
provider returned timestamp vs requested (same 1h tolerance as the backfill),
credit cost from response headers, number of events with h2h bookmakers,
books present (is Pinnacle among them?).

**Feasibility gate:** ALL of: 200 on every sample call; h2h quotes present for
a majority of events; Pinnacle present in >= 1 sample; measured cost <= 25
credits/call. If any fails -> verdict `infeasible`, record, STOP. No pilot.

## Phase 2 — pilot pull (HARD CAP: 2,000 credits total, --max-credits enforced)

Only if Phase 1 passes. Season 2024 only, the same 54 snapshot instants
(Wed 12:00 / Sat 12:00 / Sun 15:30 UTC, weeks 1-18), markets=h2h, regions=us,eu.
Expected cost 54 x 20 = ~1,080 credits. `--min-remaining 1000` protects the
account. Stored via the existing backfill pipeline (raw envelopes +
normalized quotes, markets labeled FULL_GAME_MONEYLINE, line stored as 0 —
moneyline has no point) so the standing per-pull audit trail holds.

**Pilot quality gate (before analysis):** >= 50% of 2024 games have >= 3 books
with moneyline pairs in their closing snapshot; else verdict
`infeasible_for_analysis` (data too thin), record, STOP.

## Phase 3 — analysis (only if Phase 2 passes)

Moneyline execution test, same machinery as execution-alpha-v1 (preregistered
separately) adapted to two-outcome h2h: reference selection = home team;
multiplicative de-vig on the home/away pair; best available home-team fair
prob per (game, snapshot) vs closing LOBO consensus (evaluated book excluded);
noise-null comparison for the confirmatory test (the Amendment A1 lesson:
best-price selection is positively biased under the noise null, so the test
is observed-vs-simulated-null, NOT mean-vs-zero).

- Confirmatory: one-sided observed-vs-null test, p < 0.05.
- Practical bar: observed mean per-game CLV >= 0.01.
- Min 30 games with >= 1 cell, else `infeasible_for_analysis`.

**Verdicts:** `infeasible` | `pilot_failed` | `infeasible_for_analysis` |
`execution_edge` (p<0.05 AND mean>=0.01) | `no_edge`. A positive finding
selects the hypothesis for FORWARD-SHADOW validation only — the mandatory
promotion gate remains positive CLV against actual closing lines on forward
data. Historical results never promote.

**Point-in-time safety:** identical to the frozen-dataset work — a snapshot's
quotes are compared only against information at or before its observed_at;
closing snapshots are ex-post benchmarks only. Target book never in its own
consensus. Multiplicative de-vig throughout.
