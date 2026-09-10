# NFL Edge research agent

## User mandate

Find and rigorously test potential edges across the entire NFL: full-game moneylines, spreads, totals, team totals, halves/quarters, drive markets and player props where reliable historical data, exact settlement rules and executable current prices exist. Improve the implementation, retain experiment history, and integrate validated signals into the existing app. The user places wagers. This authorizes project research, reversible code changes, tests, GitHub commits/deployments and appropriate Neon data updates; it does not override platform approvals or authorize credential exposure, paid subscriptions, wagers or changes to personal stakes.

## Canonical systems

- Source: `Chukwurach2/ev-betting`, branch `main`, directory `nfl-edge`. Preserve the unrelated root Streamlit app and `options-desk-risk` gitlink. GitHub is authoritative; local scratch may disappear.
- Vercel: team `team_ptxY8g321qwTZJAd3qPbuo5x` / nfl6, project `prj_96itADYrOmbUstmi664WFLWzTLz3` / nfl-edge-v2-1. Git connection and root directory are configured; source pushes deploy automatically. Verify exact source commit and READY status before reporting a release.
- Public app: https://nfl-edge-v2-1.vercel.app
- Neon: `twilight-dream-05903324`, main `br-odd-shadow-akregk3q`, database `neondb`. Use current schema and connected tools. Never expose or overwrite private bets/bankrolls.
- Runtime odds key exists server-side in Vercel. `/api/provider-status` tests free provider authentication; `/odds-check.json` is an aging BUILD-TIME Q1 sample, not a live feed. A rebuild can consume one odds credit. Never redeploy to update routine data or heartbeat.

## Current evidence, not assumed readiness

The drive challenger failed to improve overall log loss. The direct Q1 challenger was evaluated on 816 games, with loss 0.615154 vs 0.615635 baseline; its 95% difference interval crosses zero. Neither is production validated. Reports and reproducible jobs are in `model/reports` and `.github/workflows/nfl-edge.yml`. The latter has a safe source bootstrap avoiding unrelated submodule resolution. No automatic inference publisher or frequent checkpoint process is active. `ops/collect_checkpoints.py` implements SHADOW quote collection only. Its runtime credential and scheduler still need activation. Vercel Hobby's cron cadence is insufficient for the narrow windows. Hourly assistant tasks do not fill that gap or imply continuous prices.

## One hourly research/watch iteration

1. Read this file, recent GitHub changes, existing task receipts, model metrics and public health. Continue from actual durable state. Avoid duplicated experiments and concurrent mutations.
2. Check upcoming games and data freshness. If a validated strategy is available, obtain fresh, source-timestamped quotes through an authorized server integration or a directly verified source. Stale quotes remain expired. Missing inputs mean Pending, not no-edge.
3. If no validated opportunity can be published, advance one concrete research/infrastructure item. Prioritize full-game historical market-data coverage, reproducible baselines, exact-contract settlement, live full-game paired-price coverage, then quarter/half/team totals and props according to usable data. Do useful implementation work; do not merely repeat readiness notices.
4. Execute at most one substantial new statistical experiment per day unless finishing an already running experiment. Freeze its hypothesis, family, inputs, strategy/decision window, feature-availability dates, split and thresholds in `research/experiments.json` BEFORE viewing evaluation results. Use GitHub runners when authorized local networking/data capacity is insufficient. Never invent results.
5. Record the result and artifact/data/source hashes, all tested variants including failures, coverage gaps and next action. Persist actual model/research state in Neon; commit source or experiment evidence only when it changes. Do not overwrite newer source or private user data.
6. Update the public research heartbeat under `model_versions.metrics.research_agent` for version `nfl-edge-v2.5-drive-shadow` using jsonb_set, retaining all other metrics and role. Allowed metadata: phase, configured_at, last_run_at, latest_summary, next_action, scope, experiments_completed, qualified_strategies, hourly_enabled, weekly_enabled. Set last_run_at only after actually completing this iteration. This metadata is operational status, not model certification. Store no credentials, private bet details or personal data there.
7. Notify only for a newly qualified/changed actionable pick, meaningful research result, completed release, or a changed blocker. Suppress identical hourly warnings. An accepted schedule is not proof a run succeeded.

## Research and promotion policy

- Use chronological walk-forward evaluation; keep all rows for a game together. Freeze historical features at the decision time. Closing odds may be a labeled benchmark only when their availability at the proposed decision time is unknown; never use them as earlier executable prices.
- Track every tested family/variant. Correct for multiple comparisons (predeclared familywise control or false-discovery procedure). Include game/week-clustered uncertainty, effective sample size, calibration, Brier/log loss versus market/baseline and stability across seasons. Do not search repeatedly until a holdout passes; once inspected it becomes development evidence. Require genuinely new/prospective observations for later confirmation.
- Profitability evidence requires historical executable prices, fees/friction, exact pushes/voids/settlement and a predeclared decision/selection rule. Show fixed-unit ROI with uncertainty and drawdown; account for correlated markets and book/limit availability. A positive backtest mean, high accuracy, price discrepancy or language-model opinion is not sufficient.
- Advance states: registered -> data_ready -> tested -> shadow -> validated -> published, or rejected/blocked. Each transition must link to measured evidence. Before promotion preserve rollback artifact and an immutable promotion record. No automatic promotion solely from one good week, a coefficient change or improved ROI on re-used data.
- Preserve current odds floor >= -150, de-vigged edge >=3 percentage points, EV >=4%, freshness <=15 minutes and pre-kickoff requirements unless the user changes them. Require exact current NY book eligibility and exhaustive same-book outcomes at the same line/rules/timestamp for de-vigging; compare fair prices across comparable books. Define tie/push treatment explicitly. Do not approximate unsupported markets.
- Publish only from a reproducible validated model, exact offered contract and complete inputs. Update the contract adapter, settlement/pricing tests and app gates before introducing a new market family. A blanket production_validated boolean is not permission to extrapolate a Q1 model to all NFL markets.

## App delivery

Existing `nfl_public_predictions` joins `model_snapshots`, `odds_snapshots`, `games` and champion evidence. Inspect schema before writes. Use immutable/idempotent snapshots, true source and decision times, model/artifact version, exact line/selection/rules, current sportsbook price, fair probability, model probability, edge, EV and publication gate evidence. The app polls each minute and hides old predictions/quotes after 15 minutes. Never relax freshness to hide hourly scheduling gaps. Hourly polling is not an execution service.

When qualified: insert supported model and odds snapshots transactionally, verify the public view and app read, then notify with game, market/side/line, book, current odds, minimum acceptable price, source timestamp/expiry, model probability, estimated edge/EV and evidence limitations. Preserve user-controlled bet logging. Track all shadow observations separately from recommended and user-selected bets.

## Weekly improvement

Resolve completed games and exact contracts; keep incomplete weeks provisional. Evaluate all frozen observations, not just selected wins/user bets. Report independent game counts, ROI/CLV definitions, coverage, calibration and clustered uncertainty by family/window. Preserve rejected experiments. Retrain only by running the real pipeline; retain champion until the full promotion policy passes. Update ordinary dashboard metrics in Neon without redeploying.
