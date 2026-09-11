# NFL Edge operation status

GitHub `Chukwurach2/ev-betting`, branch `main`, subdirectory `nfl-edge` is connected to Vercel `nfl-edge-v2-1`. Git-source deployment was verified READY at commit `0bebe30`. The existing app remains on Neon Auth/Data API with account-owned journal records.

## Measured model status

The direct Q1-total experiment completed on 1,775 training games (2016–2022 after cold-start exclusions) and 816 validation games (2023–2025). Log loss was 0.615154 versus 0.615635 baseline. The week-block 95% interval for the difference was [-0.003335, 0.002463], which crosses zero. The accuracy gate did not pass. The report is `model/reports/2026-09-10-quarter-validation.json`; the original drive experiment is also retained. Neither model is production validated. No executable historical odds, ROI or CLV validation has been completed.

## Checkpoint worker

Implemented in `ops/checkpoints.py`, `ops/postgres_checkpoints.py` and `ops/collect_checkpoints.py`:

- T-24, T-3, T-90 and a last pregame observation targeted at T-5 minutes.
- Fifteen-minute collection windows, capped at kickoff. Observations retain original provider timestamps. Late responses are marked missed rather than backdated.
- Atomic unique claims committed before any paid request. Existing or abandoned claims are never charged again automatically. Connection failure after a request can leave an abandoned claim; reconciliation reports it without a paid retry.
- A direct Postgres session advisory lock prevents overlapping worker runs. Pooled connection URLs are rejected because session locks need a stable connection.
- Free event discovery, exact home/away/kickoff mapping, DraftKings/FanDuel Q1 totals only, same-book paired prices.
- At most 16 event requests per run, allowing a simultaneous NFL slate, while preserving a ten-credit quota reserve. Unknown quota stops paid requests. A single one-market response is requested per event; no automatic request retry.
- Shadow quote receipts only. This worker never writes `model_snapshots`, promotes a champion, qualifies a wager or places a bet.

The isolated `nfl_edge_checkpoints` table is applied to Neon with RLS enabled and no anonymous/authenticated client access. Existing bets and bankroll tables are unchanged.

Preview a schedule without credentials or network requests:

```
python ops/checkpoint_plan.py --schedule ops/schedule-2026.json
```

To run the worker in an authorized server environment, install `ops/requirements.txt` and provide `NFL_EDGE_DATABASE_URL` (direct TLS connection, permissions limited to reading games and claiming/updating checkpoint rows) plus `THE_ODDS_API_KEY`. Then run:

```
python ops/collect_checkpoints.py
```

**Activation (2026-09-12):** the worker is scheduled via `.github/workflows/collect-odds.yml`
(GitHub Actions cron every 15 minutes plus manual `workflow_dispatch`). Each run applies
additive migrations (`ops/migrate.py`), syncs `ops/schedule-2026.json` into `public.games`
(`ops/sync_schedule.py`, best-effort), then runs `ops/collect_checkpoints.py`. Runs with no
due checkpoints spend zero provider credits. `CRON_SECRET` is configured as a Vercel
production env var and a GitHub Actions secret, unblocking the protected `/api/odds` route.
A Postgres trigger (`ops/migrations/002_quote_history.sql`) fans every captured checkpoint
into the normalized, idempotent `nfl_edge_odds_quotes` history table for line-movement
reconstruction and later CLV calculation.

Two repository secrets are still required under GitHub Settings > Secrets and variables >
Actions before scheduled runs can collect: `NFL_EDGE_DATABASE_URL` (a **direct** Neon
connection string, not the pooler — the worker uses a session advisory lock) and
`THE_ODDS_API_KEY`. Until both are present, runs fail fast with a clear message and spend
nothing. Do not represent a passing workflow run as model validation: collection output
remains shadow-only. Live authenticated cross-device journal acceptance and
market-validated inference/publication also remain outstanding.

## Shadow picks engine (v1-consensus)

`ops/picks.py` runs after collection on every scheduled tick. It reads the
latest fresh quotes (<=30 min old, game not started) from
`nfl_edge_odds_quotes`, takes the median no-vig fair probability across books
as consensus, and emits a **shadow** pick for any book whose offered odds beat
consensus by >= 2% edge. Staking is quarter-Kelly capped at 1 unit of a
100-unit shadow bankroll. An empty pick set is normal: the engine never forces
picks.

- Ledger: `nfl_edge_picks` (migration `003_picks.sql`), deterministic pick IDs,
  `ON CONFLICT DO NOTHING` for idempotent re-runs.
- `mode` is hard-wired to `'shadow'`. `assert_shadow()` raises on any other
  mode. Promotion requires the validation evidence in
  `model/production_gate.py`; the Q1 drive model failed its accuracy gate and
  is not used.
- Not a predictive model: v1 detects cross-book line edges only. A trained
  challenger model can be plugged in later behind the same gate.
