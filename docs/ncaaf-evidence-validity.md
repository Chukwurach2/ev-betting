# NCAAF Evidence Validity — Unsigned Spread Lines

**Date:** 2026-09-15.
**Frozen dataset fingerprint:** `684c58410968c440a6d7500582ac9ecf` (immutable).

## The representation issue

The frozen 2022–2025 NCAAF dataset stores spread lines as **unsigned
magnitudes** (`|spread|`). The backfill paired the two sides by `abs(line)`
and persisted the absolute value. The sign — which team was -7 vs +7 —
is not in the frozen tables.

This fingerprint is the immutable raw freeze. It is NOT repaired, patched,
or re-written. Any sign recovery lives in a separate derived layer with
explicit provenance (see below).

## What remains valid WITHOUT the sign

These analyses never needed the sign and are unaffected:

- **Totals (over/under).** No sign concept; selection name gives direction.
  All F2 totals results stand on the frozen data alone.
- **Moneyline.** No spread sign involved.
- **Market structure (F1).** Line-magnitude distributions, cross-book
  dispersion, range, movement in points — all use `|line|` or differences.
- **Spread line movement (magnitude).** "The line moved 1.5 points" needs no sign.
- **Per-selection existence/counts.** Which games/teams/books quoted, at what
  magnitude.

## What REQUIRES the sign (derived layer)

Any analysis that determines **which side covered** needs the sign:

- Spread cover rates (F2 `by_line_bucket`, `by_window`, `by_matchup`, etc.)
- Spread calibration (realized cover vs stated fair probability)
- Spread closing efficiency
- Any future spread residual modeling (H challengers)

**Derived layer (Amendment A4):** the sign is recovered from CFBD pre-game
`/lines`, a free point-in-time-safe source independent of the provider
dataset. Method: median CFBD spread per game (negative = CFBD home favored);
provider-home favored ⟺ (CFBD spread < 0) XOR (orientation swapped).
Only the SIGN comes from CFBD; magnitudes, probabilities, scores, and all
market data remain the frozen provider dataset. Games with no CFBD line
(67 / 1.1%) are excluded and counted, never guessed.

**F2 run 35004257658 used the A4 derived layer.** The ~50.5% home-cover
result reconstructs ATS orientation independently from the unsigned stored
line. It does NOT assume the unsigned line's sign. The result is valid
evidence.

## Provenance chain

1. Raw freeze: fingerprint `684c58410968c440a6d7500582ac9ecf` (unsigned spreads).
2. CFBD fixtures: `cfbd_lines_{2022,2023,2024}.json` (sha256 in prereg A4).
3. Analysis code: `market_outcomes.py` `load_lines()` + sign logic (commit 72c326d5).
4. Result: run 35004257658, artifact 10411365437.

## Forward fix (not a repair)

`backfill_history.py` now stores **signed** spread lines for future pulls
(commit 0ba8f600). The live collector already did. This does not alter the
frozen 2022–2025 dataset or its fingerprint.

## Rule for H and beyond

Any spread residual model must declare its sign source in the preregistration.
The unsigned frozen line alone is never sufficient for cover determination.
