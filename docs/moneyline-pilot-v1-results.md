# Moneyline-pilot v1 results — Phase 3 execution test

Role: **SHADOW research only.** Nothing here promotes anything.
Preregistration: `docs/preregistrations/moneyline-pilot-v1.md` (Phases 1–3
all pre-specified; no amendments). Read-only analysis; the paid pull is
complete and documented below.

## Phase 1 — feasibility: `feasible_proceed_to_pilot` (passed)

3/3 sample calls HTTP 200, h2h quotes on 100% of events (272/272, 26/26,
26/26), Pinnacle present (16/13/13 events), 20 credits/call ≤ 25 cap.

## Phase 2 — pilot pull: complete

- 2024 season, 54/54 snapshot instants, markets=h2h, regions=us,eu.
- 47,236 normalized FULL_GAME_MONEYLINE quotes stored via the backfill
  pipeline with full audit receipts.
- Credit spend: 60 (feasibility) + 1,060 (pilot) + 20 (repair) = **1,140**;
  12,158 remaining. Under the 2,000 cap.
- Two pipeline fixes shipped during the pilot (both committed, CI green):
  migration 010 (market CHECK widened), and the skip-incomplete check made
  market-aware after the frozen spread/total quotes at the same snapshot_at
  masked an incomplete h2h capture. One snapshot (2024-09-04) re-fetched.
- **Quality gate: PASSED.** 281 games with ≥1 cell (min 30); the closing
  snapshots carry deep multi-book moneyline coverage (22–25 books).

## Phase 3 — execution test: verdict `no_edge`

Moneyline execution test, same Amendment-A1 machinery as execution-alpha-v1:
best available home-team fair prob per (game, snapshot) vs closing exact-line
LOBO consensus (evaluated book excluded), observed-vs-simulated-noise-null.

- 281 games, 1,380 best-price cells.
- Observed mean per-game CLV: **+0.01172** (95% CI [0.0066, 0.0168]) —
  clears the 0.01 practical bar against zero.
- Simulated noise null: mean **0.01156**, 95th percentile 0.01190.
- Observed-vs-null p = **0.216**. Not significant.

## Bottom line

The moneyline market shows the same signature as spread/total: cross-book
price dispersion exists (+1.2pp shopping "edge") but is 100% explained by
the mechanical best-price selection bias under the noise null. There is no
harvestable execution edge in 2024 moneylines either. This is the third
independent market (spread, total, moneyline) where the A1 null comparison
kills an apparently-positive shopping edge. Hypothesis closed. A positive
finding would have selected it for forward-shadow validation only; nothing
advanced from SHADOW, no picks or recommendations generated.
