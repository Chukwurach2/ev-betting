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

**Activation remains incomplete:** no runtime database credential or repeating worker schedule is configured. Vercel's current Hobby plan cannot provide the required frequent cron cadence. A scheduler must invoke the worker frequently enough for these windows, monitor nonzero exits and report abandoned/missed records. Do not represent a manual run or passing unit tests as an active scheduler. Live authenticated cross-device journal acceptance and market-validated inference/publication also remain outstanding.
