# Football Edge

Football Edge is a shared evidence platform with two isolated sport stacks:

- **NFL Edge** — frozen v1.3 shadow engine accumulating prospective evidence.
- **NCAAF Edge** — 2022–2025 development track plus a sealed 2026 prospective track.

Start with [`docs/STATUS.md`](docs/STATUS.md) for the current operating state and [`docs/README.md`](docs/README.md) for the durable project map. The governing contracts are [`research/AGENT.md`](research/AGENT.md), [`research/experiments.json`](research/experiments.json), and [`docs/ncaaf-plan.md`](docs/ncaaf-plan.md).

## Current posture

There is no validated champion and no claim of profitability.

NFL v1.3 is frozen. Forward observations are measurement-only: they may evaluate checkpoint health, calibration, exact-line CLV, settlement, and fixed-unit performance, but they cannot tune the model, thresholds, features, or market selection. Checkpoint reliability is mitigated, not yet proven healthy.

NCAAF phases A–C are complete. Phase D live 2026 collection is active, and 2022–2025 historical acquisition is in progress. No NCAAF modeling may begin until the phase-E coverage audit passes and the development dataset is fingerprinted and frozen. The entire 2026 season remains sealed prospective evidence.

The two sports share ingestion, checkpoint, pricing, settlement, CLV, and health infrastructure, but never share model evidence or promotion decisions.

## Recommendation contract

A recommendation must have:

- executable odds of at least -150;
- probability edge of at least 3 percentage points;
- expected value of at least 4%;
- correctly paired and de-vigged comparable prices;
- quote and prediction ages no greater than 15 minutes;
- a future authoritative kickoff;
- verified New York eligibility; and
- complete inputs for the exact market and line.

Most evaluated markets should return **PASS**. No thresholds are lowered to create picks.

## Local checks

```sh
node --test tests/*.test.mjs
python -m unittest discover -s tests -p 'test_*.py' -v
node ops/build.mjs
```

Python dependencies are pinned in `model/requirements.txt`. The build publishes only the application bundle; source, tests, credentials, and research artifacts are not exposed as static files.

## Operations

GitHub Actions runs regression checks and the quota-aware checkpoint collectors. NFL retains quota priority. NCAAF stands down below its declared reserve floor. Actual provider and collector timestamps are preserved; late observations may remain available for diagnostics but cannot count as on-time evidence or generate frozen-engine picks.

The Vercel project is `nfl-edge-v2-1` with repository root `nfl-edge`. `THE_ODDS_API_KEY` remains server-only. Durable operational state and immutable run receipts live in the isolated Neon schema.

Historical failures and superseded experiments are retained as provenance. See `model/reports/`, `research/experiments.json`, and the receipts linked from the project map.
