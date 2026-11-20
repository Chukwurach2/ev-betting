# Curated probability input review

The private-card validator accepted a numeric string or one-element array as
fair_probability because its range comparison coerced the value. An otherwise
fully checked payload could consequently display as observed without a numeric
scalar probability. The publisher now requires a finite JavaScript number
strictly between zero and one; malformed inputs are unavailable rather than
silently converted. Valid numeric estimates retain their existing behavior.

A new regression case failed against the prior exact PR56 head and passed after
the fix. It covers strings, arrays, coercible objects, missing/null/boolean values,
non-finite numbers and range boundaries, alongside valid numeric probabilities.
All 26 focused private-card tests pass. The four privately staged touchdown
payloads retain numeric probabilities and remain expired without modification.

This is input validation, not evidence that an estimate is accurate or independent.
No market prices, probability estimates, research cohort, paper returns, source
permissions or model status changed. It does not establish fresh NFL/NBA cards,
private owner-scoped phone delivery, a deployment or a validated strategy. No
provider odds request or statistical experiment was run.
