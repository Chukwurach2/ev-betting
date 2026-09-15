# Investment Card: Weather → Totals (Candidate #3) — PARKED

**Family:** Fundamental / environmental
**Prior:** LOW (default for new families)
**Date:** 2026-09-15
**Status:** PARKED — awaiting data infrastructure

## Why This Family

Mechanism is genuinely orthogonal to market microstructure:
- Physical conditions (wind, temperature, precipitation) affect scoring
- Not a transformation of cross-book disagreement
- Not a book-specific pattern
- Tests whether the market fully incorporates observable pre-kickoff weather

## Hypothesis (draft, not yet preregistered)

Extreme weather conditions (high wind, low temperature, precipitation) suppress
scoring relative to the posted total, creating a directional bias the market
may not fully price.

## Why PARKED, Not Blocked/Failed

The hypothesis is sound. The data is not yet available:
1. **Outcome data:** Blocked on CFBD API key (needed for actual totals)
2. **Weather data:** Need timestamp-safe historical weather (known before kickoff, no leakage)
3. **Join key:** Canonical game identity required to link weather ↔ games ↔ quotes

## Data Infrastructure Required (in priority order)

1. **Resolve outcome-data access** (CFBD free API key from user)
   - Need: actual game totals for 2022-24
   - Without this, no fundamental hypothesis is testable

2. **Weather reconstruction feasibility**
   - Can we get hourly stadium-level weather for 2022-24 games?
   - Critical: must be knowable *before kickoff* (forecast, not realized)
   - No leakage: cannot use post-game weather observations

3. **Pace/tempo/play-by-play audit**
   - What's available? CFBD? Other sources?
   - Timestamp integrity: can we trust the "when"?

4. **Canonical game identity**
   - Required to join: quotes ↔ outcomes ↔ weather ↔ play-by-play
   - The H5 forensic taught us this is non-trivial
   - Shared infrastructure, not family-specific

## What "Unparking" Requires

- [ ] Outcome data flowing for 2022-24
- [ ] Weather source identified with pre-kickoff timestamps
- [ ] Join feasibility demonstrated on sample
- [ ] Then: write preregistered test specification
- [ ] Then: unpark and run

## What This Is Not

- Not a market-microstructure variant
- Not cross-book disagreement with a different label
- Not Pinnacle or any book-specific pattern

## Related Families (also parked)

- Pace/explosiveness → totals (needs play-by-play)
- Travel/rest → performance (needs outcomes)
- Injuries/QB → line movement (needs injury timestamps)

All share the same data infrastructure prerequisites.
