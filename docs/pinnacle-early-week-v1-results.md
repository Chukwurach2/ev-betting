# Pinnacle-early-week v1 results — early Pinnacle vs the close

Role: **SHADOW research only.** Nothing here promotes anything.
Preregistration: `docs/preregistrations/pinnacle-early-week-v1.md` (no
amendments to the methodology). Dataset: frozen 2022–2024, fingerprint
`43f853a44bf93937d85149ca5fd7241b`, 162/162 snapshots, 291,586/291,586
quotes verified at run time. Zero API credits; read-only Neon.

**Implementation note (transparent):** the first run returned 0 units
because the Wednesday filter required `hour == 12` while the provider
stores snapshots at ~11:55 UTC. Fixed to match on Wednesday (weekday)
— the preregistered "Wednesday 12:00 UTC cadence snapshot" — plus a loud
guard that fails if no Wednesday snapshots exist at all. No methodology
changed; the re-run below is the valid one.

## Verdict: `no_signal`

- 325 units across 289 games (feasible). Diagnostics: 488 no-signal
  (|deviation| < 0.005), 529 game-markets with no valid early snapshot,
  308 with no close.
- **Direction:** agreement rate 0.375 (103/275), one-sided binomial
  p ≈ 1.0 vs 0.5 — Pinnacle's early lean does NOT predict the close.
- **Tradeability:** consensus-priced Wednesday Pinnacle-lean strategy CLV
  mean +0.0005 per $1, p = 0.24 — nothing.
- **Falsification (exploratory):** Pinnacle's own price converges toward
  the early cross-book consensus by the close 75% of the time (193
  observations). The information flow runs consensus → Pinnacle, not the
  reverse, at the weekly horizon.

## Exploratory observation (NOT preregistered — do not trade)

Agreement is significantly BELOW 0.5 (two-sided p ≈ 2e-5): early Pinnacle
deviations tend to REVERSE by the close. HOWEVER, this does not imply a
"fade Pinnacle" edge: the consensus-priced WITH-lean strategy's mean CLV is
+0.0005, so the mechanical fade's implied mean CLV is −0.0005 — also zero.
The pattern is "Pinnacle usually wrong-small, occasionally right-big"
(direction loses 62.5% of the time but magnitudes offset), which nets to
no tradeable edge. No follow-up preregistration is warranted on CLV
grounds; the directional curiosity alone is not a betting strategy.

## Bottom line

No early-week Pinnacle edge in 2022–2024 full-game spread/total. Combined
with market-alpha-v1 Track A (no snapshot-to-snapshot lead/lag), Pinnacle
shows no exploitable leadership in this dataset at any tested horizon.
Hypothesis closed. Nothing advanced from SHADOW; no picks or
recommendations generated.
