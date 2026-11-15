# Paired de-vig diagnostic for phone cards

The phone feed can now compare its proportional reference median with a
power de-vig median at the same offer. Each reference must have the exact
opposing side in the same game, market, operator feed and source timestamp.
Totals must share a line. Spreads require the opposing team and opposite
line. Both stored fair probabilities must agree with the reconstructed
proportional pair before the alternate estimate is calculated.

The diagnostic is null when any pair is unavailable or inconsistent. Only
the aggregate alternate probability and estimated EV are added to the feed.
The response adds no raw opposing quote fields. Baseline prices, ordering,
labels and publication rules continue to use the existing estimate. The
diagnostic makes no independent-source or profitability claim.

Fifteen synthetic phone-card checks passed locally. The new cases cover
both sports, totals, mirrored spreads, stale opposite timestamps, inconsistent
probabilities and preservation of the baseline when pairing fails. No odds
request, ledger write or deployment was performed.
