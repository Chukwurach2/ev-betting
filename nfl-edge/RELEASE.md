# September 9 update — current production state

Deployment dpl_47nncdaeiAhpekvUJAvSHbpz8xZa is READY on https://nfl-edge-v2-1.vercel.app/. This update supersedes the September 8 public-feed blocker below. The deployed files are index.html, app.js, core.mjs and style.css.

Public reads now use client.from(...), with allowAnonymous:true and explicit Neon Auth/Data API URLs. The prior client.auth.getJWTToken() call was erroneous; query-client-managed token acquisition works. No PostgreSQL password is needed in Vercel. The server-side credential transfer remained blocked even after explicit user approval, so that design was abandoned. Do not retry that transfer or deploy the obsolete public-feed-release candidate. Neon Functions is unavailable in this project's us-west-2 region.

Browser verified: signed-out live Neon feed loads 16 Week 1 games; logging a local QA-only bet at +150 with $1 stake works; settling win displays $1.50 profit; closing +130 displays 3.48 pp raw CLV. The entry persisted through reload. Cleanup click hit a browser timeout; a QA-only entry may remain in the isolated test browser, never in the user's cloud journal. Six Node regression tests pass. Authenticated cross-device sync has not been tested end-to-end.

Both NFL automations are enabled and updated to use the direct public feed and to avoid obsolete credential prompts/deployments. Database audit still finds zero model versions, forecasts and quotes. Saved refresh_week.py only emits empty picks and checks artifact/key presence. No validated trained V2 model or usable odds feed is established. Historical PBP download attempt was blocked by runtime network approval cancellation. No predictions or trained status were fabricated. Production pick generation remains incomplete; schedule/UI availability is not model readiness.

Required completion: access historical training inputs, run reproducible chronological validation with feature-availability checks, execute real pregame inference with a licensed supported-market odds feed, persist complete frozen snapshots, and verify account sync plus a live dry run. Monitoring alone cannot build missing artifacts or obtain credentials.

---

# NFL Edge V2.3 release — September 8, 2026

Production: https://nfl-edge-v2-1.vercel.app
Deployment: dpl_8QtQcM7UPewuiCuUiPi6sU1fZd3C, READY.
Project: prj_96itADYrOmbUstmi664WFLWzTLz3, team nfl6.

## Deployed build
Deploy only index.html, app.js, core.mjs, style.css and ops/schedule-2026.json renamed to schedule-2026.json at the web root. These are the currently deployed files. The root package/API files support the pending upgrade, and are not part of this static deployment.

Schedules for all 272 regular-season games are included, sourced from nflverse/nfldata games.csv on September 8, 2026. They are labeled as a saved schedule for signed-out visitors. Signed-in users query Neon. Database has 288 rows because 16 earlier Week 1 IDs coexist with nflverse IDs; the app normalizes team aliases and deduplicates matchups without deleting references. Future ingestion must reuse existing IDs, including any game referenced by a bet.

Private journals are scoped by account, with persistent edit queues, UUID create deduplication, revision-checked updates, export/reload conflict recovery, and explicit legacy/device import. Bankroll import fills only an empty account bankroll. The original legacy journal remains preserved. Raw implied-probability CLV is labeled separately from de-vigged CLV.

The applied migration adds client_id and revision fields, a revision trigger, and the public prediction view. Original bet/bankroll ownership RLS remains in place. Migration was tested on QA branch br-floral-glitter-akuo4jrn before production. Revision update/stale-write rejection and public/private read privileges were checked. Six Node regression tests pass: freshness/kickoff/validation gates, pricing thresholds, latest decision selection, payout/CLV and input handling. Browser verified production V2.3, 16 Week 1 games, and Week 2 navigation. Authenticated account round-trip has NOT been browser-verified. SDK session initialization is not proof of successful cross-device sync.

## Prepared public-feed upgrade — NOT DEPLOYED
public-feed-release contains the complete candidate deployment, including api/feed.js and the pinned serverless Postgres dependency. It uses a fixed read-only SQL transaction and validates season/week. No arbitrary SQL or table names are accepted. It never queries user bets or bankroll. It requires server-only environment variable NFL_PUBLIC_DATABASE_URL. No secret is included in this archive.

Automatic approval review rejected sending this credential to Vercel without explicit destination approval. Before retrying, obtain that approval. Then get the existing nfl_feed_reader connection through the authorized Neon connection tool (project twilight-dream-05903324, main br-odd-shadow-akregk3q, neondb), deploy the candidate with the server-only environment variable, confirm READY, and verify /api/feed?season=2026&week=1 plus the browser. If the role password is unavailable, reset only this unused role via the authorized Neon tool. Never use the database owner credential.

Verified nfl_feed_reader: no superuser, bypass-RLS, create-role, create-db or admin membership; no bet or bankroll SELECT; no games INSERT; default_transaction_read_only=on. Grants are public schema USAGE and SELECT on games, nfl_public_predictions, model_versions. The temporary broader role created by the console tool was deleted. The read-only role remains ready for activation.

Neon Data API anonymous requests require a JWT; anonymous-token acquisition returned HTTP 404 in browser, so the deployed build does not depend on it. Auth uses explicit provisioned endpoint URLs. Public-feed upgrade avoids anonymous-token dependency. Both app aliases are trusted by Neon Auth; temporary preview origin is also trusted. The nfl6 alias requested Vercel authentication in this browser; the shorter public alias was verified. Do not change protection settings or bypass protected-preview authentication without appropriate authorization.

## Prediction readiness and weekly process
No validated trained V2 artifact or usable live odds feed was located; model_versions is empty. Earlier archives contain scaffolding, not evidence of a production engine. Do not fabricate a model version, probabilities or picks. Complete real inference/quote/input validation before marking a champion production_validated. Forecasts require complete contract/source metadata, NY licensing evidence, paired same-book de-vigging, observed quotes AND prediction timestamps within 15 minutes, and pre-kickoff status. Existing -150-or-longer / 3pp edge / 4% EV rules apply.

Weekly review automation 6a9f418b395c81918b6d87bfd1ba633d remains enabled with improved coverage, cohort deduplication, exact settlement, CLV definitions, uncertainty, leakage prevention and promotion controls. Day-Before Picks automation 6a9f364939b881918ef5d3c5c4373333 was already paused and remains paused. Both prompts now reference V2.3 and these blockers. Hourly monitoring cannot continuously support a 15-minute executable-quote freshness rule. No pick start date is established.

V2.4-free-odds update: added The Odds API as primary production odds source with quota-aware event-level discovery. SportsGameOdds is no longer required for production readiness.
