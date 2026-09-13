# Football Edge project map

This directory is the human-readable map of the current NFL + NCAAF workstreams. The codebase remains shared; sport-specific evidence, research contracts, schedules, models, and settlement are kept isolated.

## Source of truth

| Area | Source | Purpose |
|---|---|---|
| NFL research policy | `../research/AGENT.md` | Governing rules for NFL research, forward evidence, promotion, and operations |
| NFL experiment registry | `../research/experiments.json` | Immutable record of registered/completed NFL experiments and failures |
| NCAAF research contract | `ncaaf-plan.md` | Frozen 2022–2025 development / 2026 prospective-only NCAAF contract |
| Operations | `../OPS_README.md` | Runtime, collection, deployment, and operational procedures |
| Release history | `../RELEASE.md` | Historical release notes; not the preferred current-state tracker |

## Current workstreams

### NFL — frozen forward-shadow track

- Production engine: v1.3 forward-shadow family; model remains frozen.
- Forward data is measurement-only and must never tune features, thresholds, market selection, or parameters.
- Primary objective: accumulate healthy, uncontaminated prospective evidence against actual closing lines.
- Operational state: checkpoint reliability is **mitigated, not yet proven healthy**. Late captures remain diagnostic-only.
- Promotion remains blocked until the preregistered statistical and operational gates are satisfied.
- No requirement exists to manufacture picks; PASS is the correct output when gates are not met.

### NCAAF — separate research/evidence track

- Contract: frozen in `ncaaf-plan.md`.
- Development seasons: 2022–2025 only.
- Entire 2026 season: sealed prospective shadow; never training/tuning data.
- Phase-1 markets: FBS spreads + totals only; FBS-vs-FBS and FBS-vs-FCS are distinguished.
- Shared infrastructure is parameterized by sport; NCAAF evidence/tables remain isolated from NFL evidence.
- Phase order:
  1. A — contract freeze: **done**
  2. B — historical sport parameterization: **done / validating in CI**
  3. C — cheapest historical production-key probe: **staged, hard gate before bulk pull**
  4. D — immutable 2026 live collection: **in flight in parallel**
  5. E — full 2022–2025 audit + deterministic freeze: **blocked on C**
  6. F — market baseline: **blocked on E**
  7. G — preregister/evaluate v0 residual-vs-market challenger: **blocked on F**

## Separation rules

- NFL and NCAAF models/evidence are never pooled.
- A sport-specific model may only operate on contract families validated for that sport.
- Shared code is preferred over duplicated sport-specific implementations where behavior is identical.
- Sport-specific tables, checkpoints, experiments, settlement, and model versions remain explicitly namespaced.
- Collector time controls checkpoint/window membership; provider observation time controls quote freshness.
- Late or missed checkpoints are never reconstructed from later quotes.
- Private bet/bankroll data is not part of public model status or research evidence.

## What to read first

For an NFL research/production task: read `../research/AGENT.md`, then `../research/experiments.json`, then recent run receipts/changes.

For an NCAAF task: read `ncaaf-plan.md` first, then inspect the current phase implementation and durable receipts. Do not begin modeling until the historical probe and dataset audit gates pass.

## Naming convention

Use **Football Edge** for the shared platform/project and **NFL Edge** / **NCAAF Edge** for sport-specific surfaces, engines, tables, collectors, and research artifacts. This keeps the current NFL application identity intact while making the multi-sport architecture explicit.
