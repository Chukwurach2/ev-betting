# Football Edge app — product requirements audit (2026-09-16)

Audited surface: `nfl-edge/index.html` + `app.js` + `api/*.js` (the deployed Football Edge web app; the Streamlit `app.py` at repo root is the legacy manual EV dashboard and is out of scope).

Status key: MET / PARTIAL / MISSING. Evidence is one line each.

## Boards, games, status

- Separate NFL/NCAAF boards and status: **MET** — Home cards + NFL/NCAAF tabs with subtabs (Picks/Board/Games; Board/Status), per-sport status pills.
- Upcoming games, kickoff times, current markets: **MET** — NFL games list, Sunday Board (lines across books, movement vs opener, Pinnacle vs consensus), NCAAF Saturday board.
- Odds freshness: **PARTIAL** — quote timestamps shown (`Quote {time}`), but no computed quote age and no staleness warning. → fixed 2026-09-16 (quote age + STALE badge on shadow picks; board freshness line).
- Stale/failure warnings: **PARTIAL** — pipeline pill (LIVE/DEGRADED/DOWN via /api/health), explicit "unavailable" states; no age-based staleness threshold. → partially fixed 2026-09-16.
- Production vs Forward Shadow vs Retired vs No Qualifying Bets: **MET** — everything is labeled SHADOW/research; "no validated production model" pending states; PASS copy ("reports an empty set rather than forcing picks"); retired families are not surfaced as live.

## Recommendations and execution

- Qualifying recommendations only after earned promotion: **MET** — nothing is promoted; publish rule is shown but gated on a champion that does not exist.
- Exact book/selection/line/price/timestamp/quote age: **PARTIAL** — book/selection/line/price/timestamp shown on shadow picks; quote age was missing → fixed 2026-09-16.
- Model/edge/EV and maximum acceptable price only when defensible: **MET** — shown on shadow picks as research fields; no max-acceptable-price is published (correct: none is defensible).
- Displayed vs actionable vs recommended vs executed: **PARTIAL** — shadow picks are display-only (no log button; correct for research), My Bets is a separate manual journal; the separation was implicit → made explicit in copy 2026-09-16.
- Bet / Log Bet workflow (actual book, line, odds, stake, timestamp): **MET** — bet form captures all five plus optional model prob/grade; `placed_at` set at submit; snapshot linkage via `model_snapshot_id`.
- Never infer a displayed recommendation was taken: **MET** — logging requires explicit form submit; shadow picks have no one-tap log path.
- Bet states open/won/lost/pushed/voided/skipped: **PARTIAL** — journal supports open/win/loss/push/void; "skipped" is a recommendation-lifecycle state and is N/A until production recommendations exist → deferred, gated on promotion.
- Recommendation history distinct from executed-bet history: **PARTIAL** — shadow history existed only as aggregates; per-pick history view was missing → fixed 2026-09-16 (Shadow history table in Model view, sourced from /api/performance raw rows).

## Evidence and performance

- Automated closing-price capture, CLV, settlement: **MET** for shadow engine (settlement job + `clv_prob_points` vs closing consensus); **PARTIAL** for personal bets (closing odds entered manually; auto-capture deferred — needs a scheduled bet→closing-line matcher).
- Bankroll/performance, ROI, calibration/Brier/log loss: **MET** — shadow track record (units P/L, ROI, avg CLV, drawdown, calibration buckets, by-market); personal journal (bankroll, realized P/L, ROI, raw CLV, by-market diagnostics).
- Breakdowns by sport/market/model/version/edge bucket/season/evidence regime/period: **PARTIAL** — by-market exists for both; edge-bucket was missing → fixed 2026-09-16 (`by_edge_bucket` in summarize.mjs + UI table + test). By week/season/version/evidence-regime deferred (sample too small to be meaningful yet).
- Selective notifications: **MISSING** — no push/web-notification infrastructure exists. Deferred: needs service-worker + subscription backend; revisit only when production recommendations exist to notify about.

## Trust, mobile, persistence

- Mobile-first: **PARTIAL** — viewport meta, bottom tab nav, card layout; no device testing performed in this audit.
- Secure persistence, auth, integrity, audit trails: **MET** — Supabase auth, per-user `placed_bets` with revision-based conflict handling, localStorage queue with cloud sync, export/import.

## Implemented 2026-09-16 (this audit)

1. Quote age + STALE badge (>30 min) on shadow pick cards.
2. Shadow pick history table (per-pick: date, matchup, market/selection/line, book/odds, edge, result, CLV) in the Model view — recommendation history distinct from My Bets.
3. Explicit display-only copy on shadow picks; recommendation-vs-execution separation documented.
4. Edge-bucket breakdown (3–5% / 5–8% / 8%+) in `lib/summarize.mjs`, UI table, and `tests/performance.test.mjs` coverage.
5. Sunday board freshness line ("generated Xm ago" + STALE warning >60 min).

## Deliberately deferred

- Push notifications (no infra; only valuable with production recommendations).
- Automated closing-price capture for personal bets (manual entry works; auto-matcher is a scheduled-job build).
- "Skipped" recommendation state (gated on promotion — no production recommendations exist).
- Week/season/version/evidence-regime breakdowns (sample too small; revisit when settled N grows).
- Production log/skip workflow for recommendations (building it against shadow picks would blur the research/production line; gated on promotion).
