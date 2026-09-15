# Football Edge — Current State

One page: where things stand, what's frozen, what's running, what comes next.
Updated 2026-09-13.

## The product

**Football Edge**: two independent betting-intelligence stacks sharing one
infrastructure (ingestion, checkpoints, de-vig, CLV, settlement, health):

| | NFL Edge | NCAAF Edge |
|---|---|---|
| Role | Champion stack, shadow-only | Experimental stack, infrastructure phase |
| State | **FROZEN historical dataset** (2022–24, fingerprint `43f853a4`); v1.2/v1.3 shadow accumulating forward evidence | Phase C done, phase D starting |
| Picks | None qualified; PASS is valid | No modeling until phase-E audit passes |
| Tables | `nfl_edge_*` | `ncaaf_edge_*` (fully isolated) |

NFL models never run on college games. NCAAF work never diverts NFL work.

## Frozen contracts

- **NFL historical dataset** — frozen 2026-09-12. 162/162 snapshots, 291,586
  paired quotes, fingerprint `43f853a44bf93937d85149ca5fd7241b`.
- **NCAAF research contract** — `nfl-edge/docs/ncaaf-plan.md`, frozen 2026-09-13.
  Dev data: seasons 2022–2025 only. Everything after 2026-08-01 is sealed
  prospective evidence, never training data. Both timestamps preserved
  (collector capture time = window membership; provider observed time =
  freshness). FBS spreads/totals first; FBS-vs-FBS tagged separately from
  FBS-vs-FCS. No player props. Pinnacle not required.
- **NCAAF probe receipt** — `nfl-edge/docs/ncaaf-probe-receipt.md`, frozen
  2026-09-13. 120 credits, all gates pass, 3/3 idempotent skips on rerun.

## Live operations

| Collector | Schedule | Quota rule |
|---|---|---|
| NFL (`collect-odds.yml`) | every 15 min (:00/:15/:30/:45) | **Priority** — no stand-down gate |
| NCAAF (`collect-odds-ncaaf.yml`) | every 15 min (:07/:22/:37/:52) | Stands down if remaining < 2000 (`NCAAF_EDGE_MIN_QUOTA_REMAINING`) |

A college-slate surge can never starve an NFL T-90/close checkpoint.
Late captures are diagnostic-only (status `missed`, zero quotes invented).
Provider timestamps are the freshness authority everywhere.

## What comes next (gated, in order)

1. ~~Gate 1: collector review~~ — done 2026-09-13
2. ~~Gate 2: probe C infrastructure validation~~ — done 2026-09-13
3. **Gate 3: D live** — 2026 prospective accumulation running now
4. ~~Bulk 2022–2025 acquisition → **audit E** → fingerprint/freeze~~ — done 2026-09-15
   (180/180 snapshots, 686,578 quotes, fingerprint `684c58410968c440a6d7500582ac9ecf`;
   receipt: `docs/ncaaf-dataset-freeze.md`)
5. **Baseline F** → preregister/evaluate **G**

No modeling before the phase-E audit passes. No scope expansion.
