# NFL Edge V2.5

NFL schedule and private bet journal with bankroll, results and raw price CLV. The public UI uses the existing Neon Auth/Data API, with account ownership enforced in PostgreSQL. Server credentials are never embedded in the browser.

## Verified scope

The application provides a schedule, device journal and account-based sync implementation. The production database had 288 schedule rows and zero model versions, predictions or odds snapshots when inspected on September 9, 2026. Live authenticated cross-device acceptance remains required.

**Automatic betting recommendations are not production-ready.** Training success alone must never change that label. This release fixes future-data leakage, removes unverified game-time/closing-line features, removes class balancing that distorts probability calibration, adds real temporal-invariance checks, compares validation against a historical-frequency baseline and fingerprints data/model artifacts. A drive model remains a shadow candidate until market-level calibration and a live publisher are validated.

## Local checks

```
node --test tests/*.test.mjs
python -m unittest discover -s tests -p 'test_*.py' -v
node ops/build.mjs
```

Python dependencies are pinned in `model/requirements.txt`. The build copies only the frontend and saved schedule into `dist/`. Serverless functions are in `api/`; source, tests and artifacts are not served as static files.

## GitHub training

The repository workflow `.github/workflows/nfl-edge.yml` runs regression checks and downloads official nflverse PBP for 2016–2025. It fits on 2016–2022 and validates on 2023–2025. Artifacts and the validation report are uploaded to the Actions run with a 14-day retention window. Jobs have bounded timeouts and no write permissions, and never deploy a model or place bets.

## Vercel project

Existing project: `nfl-edge-v2-1`, team `nfl6`.

Connect Git repository `Chukwurach2/ev-betting`, production branch `main`, **Root Directory `nfl-edge`**, framework Other. Build command `npm run build`; output directory `dist`. The root directory matters because the repository root is an existing Streamlit application.

Keep existing project environment values. `/api/health` exposes only configuration presence and honest readiness status, never secrets. `THE_ODDS_API_KEY` must be server-only. The adapter currently accepts DraftKings/FanDuel Q1 half-point totals; first-drive, team-total, spread and moneyline pricing are not supported by this model.

## Remaining production acceptance

- Complete the historical-data job and inspect measured results and integrity checks.
- Validate probabilities for the actual offered contracts, including quarter boundaries, pushes, defensive scores and special teams. The recovered simulation is not sufficient evidence.
- Implement and activate a protected, quota-aware odds/inference publisher with durable checkpoint deduplication and frozen snapshots in Neon. Required checkpoints are T-24, T-3, T-90 and a last pregame observation; late jobs must record missed checkpoints, never backdate quotes.
- Verify the actual provider key, mapped live markets, same-book paired de-vigging, quote freshness and NY book eligibility.
- Verify sign-in, create/update/reload across two sessions, and cross-account isolation. The user's own bets must not be used as test data.
- Monitor failures and missed checkpoints; evaluate all shadow/recommendation observations separately from selected personal bets.

No claim of profitability, historical ROI or production validation is made by this release.
