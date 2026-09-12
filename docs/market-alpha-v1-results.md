# Market-alpha v1 results — Pinnacle lead/lag + stale prices

Role: **SHADOW research only.** Nothing here promotes anything; "no edge found" is a recorded outcome.
Preregistration: `docs/preregistrations/market-alpha-v1.md`, Amendment A1 (pre-results):
`docs/preregistrations/market-alpha-v1-amendment-a1.md` — committed before any real-data execution.
Dataset: frozen 2022–2024, fingerprint `43f853a44bf93937d85149ca5fd7241b`, 162/162 snapshots,
291,586/291,586 quotes verified at run time. Canonical game identity (event-identity audit):
825 games. Zero API credits spent.

## Track A — Pinnacle lead/lag: verdict `mixed_inconclusive` (no edge)

798 Pinnacle-move events. Per-book follower agreement (Holm family of 5, α=0.05):

| book | n | agree rate | p (Holm) | significant |
|---|---|---|---|---|
| fanduel | 463 | 0.549 | 0.102 | no |
| betmgm | 193 | 0.554 | 0.300 | no |
| draftkings | 446 | 0.509 | 0.711 | no |
| betrivers | 616 | 0.511 | 0.711 | no |
| williamhill_us | 158 | 0.532 | 0.711 | no |

Pooled across books: agree rate 0.526, n=1876, p=0.0125 (significant, +2.6pp over chance).
Reverse falsification (do followers lead Pinnacle?): no book significant after Holm.

Honest read: the confirmatory per-book family shows nothing. The pooled +2.6pp is
statistically nonzero but far too small to be a tradeable edge after any execution
friction, and it was not the pre-declared decision criterion. No lead/lag edge.

## Track B — stale prices: verdict `no_stale_price_signal`

Directional lag test (Amendment A1, exact-line LOBO, evaluated book never in its own
consensus): 66 valid flagged units across 16 games. Lagging fraction 0.348
(one-sided binomial p=0.995 vs 0.5 — decisively NOT lagging). Mean lagging-cell edge
vs close 0.0197, but the mechanism test fails so the verdict is no signal.

Transparency note: the superseded naive t-test (kept for the record) would have
reported mean edge 0.0217 with p≈2.6e-176 — the exact false positive Amendment A1
was written to prevent. Selection bias confirmed on real data, not just simulation.

## Bottom line

Both market-alpha tracks: **no edge found.** Pinnacle does not lead followers by a
tradeable margin in this dataset; no stale-price mechanism detected. These
hypotheses are closed for the 2022–2024 full-game spread/total dataset. Any future
revisit (e.g. moneylines, in-play, different thresholds) requires a new preregistration.

Next candidates (separate preregistrations): market-only-v1 (predict closing line
from early prices), residual-v1 (team-strength vs market), alpha attribution.
