# Football Edge — Current State

One page for what is frozen, what is running, and what is gated next. Updated 2026-09-13.

## Product tracks

| | NFL Edge | NCAAF Edge |
|---|---|---|
| Role | Frozen forward-shadow engine | Separate research and prospective-shadow stack |
| Current phase | v1.3 prospective measurement | A–C complete; D live; 2022–2025 acquisition active |
| Training boundary | No forward observations used for tuning | 2022–2025 development only; all 2026 sealed prospective |
| Recommendation state | No validated champion; zero public predictions | No model or predictions before audit E |
| Tables | `nfl_edge_*` | `ncaaf_edge_*` |

NFL models never run on college games. NCAAF evidence never enters NFL promotion decisions.

## Frozen contracts

- **NFL historical dataset** — frozen 2026-09-12: 162/162 snapshots, 291,586 paired quotes, fingerprint `43f853a44bf93937d85149ca5fd7241b`.
- **NFL forward engine** — `v1.3-consensus-lobo-3pp-4pct-15m`; probability edge ≥3pp, EV ≥4%, odds ≥-150, source freshness ≤15 minutes. Its forward evidence window is isolated from prior engines.
- **NCAAF research contract** — [`ncaaf-plan.md`](ncaaf-plan.md), frozen 2026-09-13. FBS spreads and totals first; FBS-vs-FBS separated from FBS-vs-FCS; multiplicative de-vig from observable books; no Pinnacle requirement.
- **NCAAF phase-C probe** — [`ncaaf-probe-receipt.md`](ncaaf-probe-receipt.md), frozen 2026-09-13: about 120 credits, every hard infrastructure gate passed, and 3/3 reruns skipped idempotently.

Both collector time and provider observation time are preserved. Collector time determines decision-window membership; provider time determines quote freshness.

## Live operations

| Collector | Schedule | Quota rule |
|---|---|---|
| NFL (`collect-odds.yml`) | Off-minute five-minute schedule plus target-specific weekly guardrails | Priority; no NCAAF stand-down gate |
| NCAAF (`collect-odds-ncaaf.yml`) | Every 15 minutes at :07/:22/:37/:52 | Stands down below `NCAAF_EDGE_MIN_QUOTA_REMAINING=2000` |

Late captures retain their actual timestamps and may be stored for diagnostics. They receive no on-time health credit, cannot generate frozen-engine picks, and are promotion-ineligible. Missed checkpoints remain missed and are never reconstructed.

### Measured snapshot — 2026-09-13 23:07 UTC

- NFL: 86 checkpoints, 912 materialized quotes, 8 on-time captures, zero v1.3 picks, zero public predictions.
- The latest NFL T-90 capture was inside the strict window, but it came from a manual dispatch. The target-specific schedule remains operationally unverified; reliability is mitigated, not yet proven healthy.
- NCAAF: 57 future games, 8 checkpoints, 126 live quotes. Phase-D 2026 collection is active.
- NCAAF historical storage contains 21,883 quotes across 7 request snapshots, and the bulk 2022–2025 acquisition workflow is running.

These counts are a timestamped receipt, not a live dashboard.

## Next gates

1. Continue uncontaminated NFL v1.3 forward measurement and prove schedule-triggered decision-window capture.
2. Continue NCAAF phase D without consuming the NFL quota reserve.
3. Finish the 2022–2025 historical acquisition.
4. Run audit E: schedule and quote coverage, paired lines, timestamps, books, duplicates, settlement, credit reconciliation, and deterministic fingerprint.
5. Only after E passes, build baseline F.
6. Preregister and evaluate challenger G with spread and total families kept separate.

No modeling before audit E. No 2026 observations enter NCAAF development. No model promotion or scope expansion is implied by a profitable week.
