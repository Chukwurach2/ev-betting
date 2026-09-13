# Football Edge project map

This page separates durable policy, current state, evidence, and executable operations so each can change without obscuring the others.

## Read in this order

| Purpose | Source |
|---|---|
| Current operating snapshot | [`STATUS.md`](STATUS.md) |
| NFL research and promotion policy | [`../research/AGENT.md`](../research/AGENT.md) |
| Immutable experiment registry | [`../research/experiments.json`](../research/experiments.json) |
| NCAAF frozen research contract | [`ncaaf-plan.md`](ncaaf-plan.md) |
| NCAAF phase-C infrastructure receipt | [`ncaaf-probe-receipt.md`](ncaaf-probe-receipt.md) |
| Operational procedures | [`../OPS_README.md`](../OPS_README.md) |
| Model reports and retained failures | [`../model/reports/`](../model/reports/) |

`STATUS.md` may contain timestamped counts. The policy, registry, contracts, and receipts are the authoritative durable evidence.

## Shared platform

The platform shares:

- authoritative schedules and event identity;
- immutable odds checkpoints and quote materialization;
- same-book pricing and de-vig utilities;
- settlement, exact-line CLV, and health reporting;
- transactional, idempotent publication controls; and
- the application shell and operational monitoring.

Each sport owns separate tables, schedules, model versions, experiment records, evidence windows, and promotion decisions.

## NFL track

The current forward engine is `v1.3-consensus-lobo-3pp-4pct-15m`. It is frozen and shadow-only. Forward observations are measurement data, never training data. Earlier drive and direct-Q1 candidates did not validate and remain recorded failures.

Operational work may improve collection, settlement, evidence isolation, and application truthfulness without changing the frozen engine. Checkpoint reliability is mitigated, not yet proven healthy.

## NCAAF track

The NCAAF sequence is hard-gated:

- **A — Contract:** complete and frozen.
- **B — Sport parameterization:** complete; NFL remains the default behavior and sport tables are isolated.
- **C — Historical probe:** complete; receipt frozen after infrastructure gates and idempotency checks passed.
- **D — 2026 prospective collection:** live.
- **Historical acquisition:** 2022–2025 bulk collection in progress.
- **E — Coverage audit and freeze:** blocked until acquisition completes.
- **F — Market baseline:** blocked on E.
- **G — First challenger:** blocked on F and must be preregistered before evaluation.

The intended first challenger is deliberately narrow: opponent-adjusted team strength plus market consensus, modeling residual information versus the market, with 2022–2024 training and untouched 2025 testing. Spread and total families remain separate. This description is a preregistration skeleton, not authorization to start before gates E and F.

## Evidence rules

- Preserve collector and provider timestamps.
- Keep exact market, side, and line identity.
- Never backfill missed decision windows.
- Retain late captures as diagnostic-only when useful, but exclude them from evidence credit and picks.
- Compare calibration, Brier score, and log loss with market and baseline.
- Treat ROI as evidence only with executable historical odds, real vig and settlement rules, fixed-unit stakes, uncertainty, stability, and drawdown.
- Preserve every completed trial and failure.
- Never use sealed prospective observations to choose models, features, thresholds, or market families.

Repository cleanup should favor clearer entrypoints and separation without deleting historical provenance or moving active executable code during in-flight acquisition.
