# Research Investment Card: Price-Pressure / Cross-Book Disagreement

**Family:** Price-pressure / observed asynchronous repricing
**Status:** Proposed 2026-09-15
**Prior plausibility:** LOW (default; no independent supporting evidence yet)

## Question

Does cross-book disagreement in paired de-vig fair probability at observation t
predict subsequent consensus repricing at t+1?

Two sub-questions:
1. Same-line convergence: does dispersion at exact (event, market, selection, line)
   predict price convergence by next window?
2. Market-level repricing: does directional outlier pressure predict the direction
   of subsequent consensus line movement?

## Current evidence

Exploratory only. No prior results.

## Statistical capital

- **Consumed:** Zero. Analysis runs on burned 2022–24 data only.
- **At risk:** None until a prospective claim is preregistered.

## Operational capital

- **API credits:** Zero (historical data already frozen).
- **Engineering:** Bounded descriptive analysis. No threshold optimization.

## Measurement prerequisites

1. Timestamp quality validated (or explicitly scoped to snapshot ordering).
2. Quote-state semantics defined for persistence claims.

## Primary descriptive outputs

- Dispersion → next-window movement magnitude (same-line).
- Dispersion/outlier direction → next-window movement direction (market-level).

## Decisions

- **Success:** Coherent, stable mechanism across 2022/23/24 → justifies
  prospective microstructure pilot with preregistered claim.
- **Failure:** Lowers prior on price-pressure branch. Only a small capped
  feasibility pilot permitted before retiring the broader microstructure direction.

## No-go constraints

- No ROI optimization.
- No threshold hunting (no "2-cent/5-cent/10-cent" selection).
- No selecting best book/window/season after seeing results.
- No alpha claim from this analysis under any outcome.
