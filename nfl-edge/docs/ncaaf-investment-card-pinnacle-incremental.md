# Investment Card: Pinnacle Incremental Information (Candidate #2)

**Family:** Market microstructure / book-specific information
**Prior:** LOW
**Date:** 2026-09-15
**Status:** Exploratory (burned 2022-24 data)

## Hypothesis (reframed)

**NOT:** "Pinnacle moves first" (cannot be established — timestamps are provider metadata).

**TESTABLE:** Does a change in Pinnacle's totals market state from Wed→Fri contain
incremental information about consensus movement from Fri→Sat, beyond what is
already observable in the non-Pinnacle consensus at Fri?

## Why This Isn't Candidate #1 Again

Candidate #1: cross-book *disagreement* at t predicts *direction* of repricing at t+1.

Candidate #2: Pinnacle's *change* from t-1→t predicts *subsequent* consensus change
t→t+1, **controlling for** the contemporaneous non-Pinnacle consensus change and
the candidate #1 pressure signal.

If Pinnacle and consensus moved together Wed→Fri, a raw hit rate would just
rediscover market momentum. The incremental test is the entire point.

## Test Specification

For each event with full Wed/Fri/Sat totals coverage:

- ΔP = Pinnacle_consensus_total(Fri) − Pinnacle_consensus_total(Wed)
  - Pinnacle "consensus" = median Pinnacle line if multiple, else single
- ΔC = non-Pinnacle consensus total(Fri) − non-Pinnacle consensus total(Wed)
- pressure_Fri = candidate #1 pressure signal at Fri (for control)
- Target: ΔC2 = non-Pinnacle consensus total(Sat) − non-Pinnacle consensus total(Wed→Fri baseline)

Regression: ΔC2 ~ β0 + β1·ΔP + β2·ΔC + β3·pressure_Fri

**Advancement criterion:** β1 significant (p < 0.05) AND |β1| materially non-zero
after controlling for ΔC and pressure_Fri.

**If null:** Close. No Pinnacle variants (no spreads Pinnacle, no different windows,
no "Pinnacle disagreement" — that would be candidate #1 with a label).

## Statistical Capital

- Uses burned 2022-24 exploratory data (same pool as candidate #1)
- Zero API credits
- One test, preregistered here. No specification search.

## Operational Capital

- Engineering: one script + workflow (already have infrastructure)
- No ongoing collection cost (historical only)

## What Advancement Would Mean

Candidate #2 would earn the right to a *separate* prospective experiment,
with its own frozen rule and firewall. It would NOT modify candidate #1.

## What Null Means

Close the Pinnacle family. The Phase F "Pinnacle lead/lag" lead is exhausted.
Move to the next family (pace/explosiveness, weather, etc.).
