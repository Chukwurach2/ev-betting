# NCAAF F3 — Market Baseline Completion (Preregistration)

**Date:** 2026-09-15. **Phase:** F (Market baseline).
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024 (2025 sealed).
**Sign oracle:** CFBD pre-game lines (Amendment A4 method). See
`docs/ncaaf-evidence-validity.md`.

## Motivation

F2 measured market-vs-outcomes by bucket/season/window/conference but did
not split spreads by favorite/underdog, did not deeply calibrate totals,
and did not characterize the baseline edge distribution. Every H challenger
must answer whether it adds information **beyond the market** — this
completes the market-only baseline they must beat.

## Analyses (all descriptive, no strategy selection)

### 1. Favorite/underdog spread covers
Using the CFBD sign oracle, split spread event-windows into:
- Home favorite (provider home laying points)
- Home underdog (provider home receiving points)

Report cover rate and n for each, by season. Question: does the market
price favorites and underdogs symmetrically? (Null: both ~50%.)

### 2. Totals calibration (fine bins)
Bin the de-vigged Over probability into [0.35,0.45), [0.45,0.50),
[0.50,0.55), [0.55,0.65). Report realized over-rate and n per bin, by
window (early/mid/late). Question: are totals probabilities calibrated?
(F2's coarse bins suggested yes; confirm with finer resolution.)

### 3. Baseline edge distribution
For each spread and total quote, the market's fair probability `p` implies
a "market edge" of `|p - 0.5|`. Characterize:
- Distribution of `|p - 0.5|`: what fraction of quotes imply >52.4%
  (breakeven at -110), >55%, >60%?
- By market (spread vs total) and window.

This sets the selectivity bar: a challenger claiming a 3pp edge must find
quotes where the market is wrong by 3pp, and the base rate of such quotes
tells us how selective it must be.

## What this is NOT
- Not a strategy. No thresholds, no picks, no ROI optimization.
- No 2025 data. No forward data.
- The FBSvFCS hypothesis (registered separately) is not tested here.

## Success criterion
Descriptive tables that let any H challenger preregistration state its
null precisely. PASS/FAIL does not apply; this is measurement.
