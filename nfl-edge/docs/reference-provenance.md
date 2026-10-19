# Phone lean source provenance

Each generated lean now retains the reference operator keys, source times,
offered odds and stored paired fair probabilities used in its median. The
reference probability min/max is a sensitivity range, not a confidence interval.
The offered operator remains excluded and aliases count as one operator.

expires_at is the oldest offered or supporting source timestamp plus15minutes.
The phone also checks this bound, so a fresh offer cannot extend the apparent
life of older reference evidence. This does not introduce any new polling or
relax freshness.

EVSharps Recommended remains an additional discovery/cross-check source:
retain its visible method and timestamps privately; label its FV provider-derived.
The same source books are not independent validation. No subscriber table or
EVSharps observations are included in this change. The signed-in Safari research
check on2026-10-19 was blocked by the locked Mac; no current candidates were
claimed from that unavailable session.
