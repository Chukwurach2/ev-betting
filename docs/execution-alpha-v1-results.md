# Execution-alpha v1 results — pure line-shopping CLV

Role: **SHADOW research only.** Nothing here promotes anything.
Preregistration: `docs/preregistrations/execution-alpha-v1.md` + Amendment A1
(pre-results: unbiased c4(n) sigma in the null DGP — the preregistered
population-stdev version was caught miscalibrated by the A1-template
calibration test, 59% false rejections on pure noise, before any real data).
Dataset: frozen 2022–2024, fingerprint `43f853a44bf93937d85149ca5fd7241b`,
162/162 snapshots, 291,586/291,586 quotes verified at run time. Zero API
credits; read-only Neon.

## Verdict: `no_edge`

- 825 games, 5,258 best-price cells (spread + total).
- Observed mean game-level CLV of always taking the best available price:
  **+0.00647** per $1, 95% CI [0.0059, 0.0070] — positive and "significant"
  against zero, exactly as shopping-bias theory predicts.
- Simulated noise null (books = consensus + iid noise, no staleness, no
  signal): mean **0.00675**, 95th percentile 0.00692.
- Observed-vs-null p = 0.998. The observed "edge" sits BELOW the null mean:
  100% of the measured shopping CLV is explained by the mechanical
  best-price selection bias. There is no genuine cross-book dispersion
  beyond noise to harvest.

## Executability diagnostics (exploratory, pre-declared)

- Best-price cells potentially stale (|dev| >= 0.02): 4.3% — not the driver.
- Edge concentration: top 5% of games contribute 19% of total edge — diffuse.
- Best-price book is spread across books (FanDuel 466, betonlineag 460,
  BetMGM 369, DraftKings 361, onexbet 354, lowvig 345, ...) — no single
  sharp book to follow.

## Bottom line

Pure execution (line shopping, zero prediction) does not beat the close
beyond what noise alone produces. This is the second independent
confirmation (after market-alpha-v1's stale-price track) that cross-book
price discrepancies in this dataset are noise, not harvestable staleness.
Hypothesis closed for 2022–2024 full-game spread/total. A positive finding
would have selected it for forward-shadow validation only; nothing advanced
from SHADOW, no picks or recommendations generated.
