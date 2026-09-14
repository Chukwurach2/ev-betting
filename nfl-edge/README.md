# Football Edge / NFL Edge V2.5

Football Edge is the shared platform for rigorous NFL and NCAAF betting research, live market collection, forward-shadow evaluation, settlement, CLV measurement, and the user-facing decision app. NFL and NCAAF share infrastructure where appropriate but keep models, evidence, experiments, schedules, settlement, and promotion decisions isolated.

The deployed application remains **NFL Edge V2.5**. No automated strategy is considered production-validated merely because it has a positive backtest or a winning week.

## Current project map

Start with [`docs/README.md`](docs/README.md). It identifies the current source of truth and separates the active workstreams.

### NFL

- Current posture: frozen forward-shadow engine; forward data is measurement-only.
- Primary milestone: accumulate healthy, uncontaminated prospective evidence against actual closing lines.
- Checkpoint reliability is **mitigated, not yet proven healthy**; late captures remain diagnostic-only.
- Model/market changes require a new preregistered research cycle.
- Research policy: [`research/AGENT.md`](research/AGENT.md).
- Experiment registry: [`research/experiments.json`](research/experiments.json).

### NCAAF

- Separate research/evidence track inside the same platform.
- Frozen contract: [`docs/ncaaf-plan.md`](docs/ncaaf-plan.md).
- Development: 2022–2025 only.
- Entire 2026 season: sealed prospective shadow, never training/tuning data.
- Initial scope: FBS spreads + totals, with line-aware identity and exact-line CLV.
- Current progression: contract frozen; historical sport parameterization implemented and under CI validation; a small historical production-key probe is staged; 2026 live-collector parameterization is being developed in parallel.
- No NCAAF modeling begins until the historical probe and full dataset audit/freeze gates pass.

## Core rules

- Do not fabricate probabilities, prices, closes, settlements, or model validation.
- Do not tune models on prospective shadow evidence.
- Preserve provider observation time separately from collector time: collector time controls checkpoint membership; provider time controls recommendation freshness.
- Never reconstruct a missed T-24/T-3/T-90/close checkpoint from later information.
- Only exact comparable contracts may be paired/de-vigged or used for CLV.
- A PASS is preferable to an unsupported recommendation.
- Private bet/bankroll data is separate from public research/model status.

## Local checks

```bash
node --test tests/*.test.mjs
python -m unittest discover -s tests -p 'test_*.py' -v
node ops/build.mjs
```

Python dependencies are pinned in `model/requirements.txt`. The build copies only the frontend and saved schedule into `dist/`. Serverless functions are in `api/`; source, tests, and research artifacts are not served as static files.

## Deployment

Vercel project: `nfl-edge-v2-1`, team `nfl6`.

Repository: `Chukwurach2/ev-betting`, production branch `main`, Root Directory `nfl-edge`, framework Other. Build command `npm run build`; output directory `dist`.

`THE_ODDS_API_KEY` is server-only. `/api/provider-status` is the authentication diagnostic. `/odds-check.json` is only an aging build-time coverage sample and is never treated as a live actionable feed.

## Research evidence

There is currently no validated champion that justifies a profitability claim.

The recovered NFL drive model failed to improve overall loss versus its baseline. The direct Q1 experiment evaluated 816 held-out games with log loss 0.615154 versus 0.615635 for the baseline, and the 95% interval for the difference crossed zero. It therefore was not promoted.

Historical and forward research must follow the policies in `research/AGENT.md`; completed and failed trials remain preserved in `research/experiments.json`.

## Operations and release history

- Runtime and operating procedures: [`OPS_README.md`](OPS_README.md)
- Historical release notes: [`RELEASE.md`](RELEASE.md)
- Current cross-sport project/status map: [`docs/README.md`](docs/README.md)

Release notes are historical context, not the preferred current-state tracker. The docs map and governing research contracts should be consulted first for new work.
