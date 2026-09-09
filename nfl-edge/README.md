# NFL Edge V2.5

NFL schedule and private bet journal with bankroll, results and raw price CLV. The public UI uses the existing Neon Auth/Data API, with account ownership enforced in PostgreSQL. Server credentials are never embedded in the browser.

## Verified scope

The application provides a schedule, device journal and account-based sync implementation. On September 9, 2026, the deployed V2.5 UI loaded all 16 Week 1 games, the registered shadow model's validation metrics and the sportsbook coverage diagnostic. No model was promoted and no automated recommendations were published. Live authenticated cross-device acceptance remains required.

**Automatic betting recommendations are not production-ready.** Training success alone must never change that label. This release fixes future-data leakage, removes unverified game-time/closing-line features, removes class balancing that distorts probability calibration, adds real temporal-invariance checks, compares validation against a historical-frequency baseline and fingerprints data/model artifacts. A drive model remains a shadow candidate until market-level calibration and a live publisher are validated.

## Local checks

```
node --test tests/*.test.mjs
python -m unittest discover -s tests -p 'test_*.py' -v
node ops/build.mjs
```

Python dependencies are pinned in `model/requirements.txt`. The build copies only the frontend and saved schedule into `dist/`. Serverless functions are in `api/`; source, tests and artifacts are not served as static files.

## GitHub training

The repository workflow `.github/workflows/nfl-edge.yml` runs regression checks on source pushes. A manual workflow run, or a commit message containing `[train]`, also downloads official nflverse PBP for 2016–2025. It fits on 2016–2022 and validates on 2023–2025. Artifacts and the validation report are uploaded to the Actions run with a 14-day retention window. Jobs have bounded timeouts and no write permissions, and never deploy a model or place bets.

## Vercel project

Existing project: `nfl-edge-v2-1`, team `nfl6`.

Connect Git repository `Chukwurach2/ev-betting`, production branch `main`, **Root Directory `nfl-edge`**, framework Other. Build command `npm run build`; output directory `dist`. The root directory matters because the repository root is an existing Streamlit application.

Keep existing project environment values. `/api/health` exposes only configuration presence and honest readiness status, never secrets. `THE_ODDS_API_KEY` must be server-only. The adapter currently accepts DraftKings/FanDuel Q1 half-point totals; first-drive, team-total, spread and moneyline pricing are not supported by this model.

## Remaining production acceptance

- Historical job completed successfully: 9,721 training drives and 4,143 validation drives. Report: `model/reports/2026-09-09-validation.json`. Overall log loss did not improve over the baseline; no model was promoted.
- Validate probabilities for the actual offered contracts, including quarter boundaries, pushes, defensive scores and special teams. The recovered simulation is not sufficient evidence.
- Implement and activate a protected, quota-aware odds/inference publisher with durable checkpoint deduplication and frozen snapshots in Neon. Required checkpoints are T-24, T-3, T-90 and a last pregame observation; late jobs must record missed checkpoints, never backdate quotes.
- Verify the actual provider key, mapped live markets, same-book paired de-vigging, quote freshness and NY book eligibility.
- Verify sign-in, create/update/reload across two sessions, and cross-account isolation. The user's own bets must not be used as test data.
- Monitor failures and missed checkpoints; evaluate all shadow/recommendation observations separately from selected personal bets.

No claim of profitability, historical ROI or production validation is made by this release.

Latest live checks: The Odds API key authenticated via the free events endpoint. The build-time check on September 9 at 22:45 UTC verified paired DraftKings and FanDuel Q1 totals for New England at Seattle, using one API credit. Each build requests at most one Q1-total market response and writes `odds-check.json`; this is diagnostic coverage, not an actionable pick. Its paired prices retain their provider timestamps. Both HTTP and transport failures in the Python adapter use secret-safe errors.

Source is pushed to GitHub and the production site is deployed. Automatic Git-to-Vercel deployment still requires connecting the repository in the dashboard with the root-directory setting above; the deployment connector does not expose that setting. The last verified source build passed at `https://github.com/Chukwurach2/ev-betting/actions/runs/34413796122`.
