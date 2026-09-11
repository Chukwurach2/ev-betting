# Market Opportunity Matrix

Which markets get recommendations is decided by evidence, not by ambition.
A row is updated every time the replay lab (`model/research/replay.py`)
runs. Nothing reaches `Candidate` without beating baselines out-of-sample;
nothing reaches `Production` without the full promotion bar below.

**Status vocabulary:** `Research` (exploratory) → `Candidate` (beats
baselines, needs more evidence) → `Challenger` (meets promotion bar, still
shadow) → `Production` (none yet) → `Rejected` (evidence against).

## Current matrix

| Market | Model | Hist sample | Brier vs base | Sim ROI @-110 | Avg edge pp | Calibration | Status |
|---|---|---|---|---|---|---|---|
| Spread | elo-v1 | 3,970 games (2010–2024) | 0.2668, no-beat | −4.1% (3,250 bets) | +10.1 perceived | −0.01 | **Rejected** |
| Total | elo-v1 | 4,033 games (2010–2024) | 0.2690, no-beat | −5.8% (3,373 bets) | +10.2 perceived | −0.05 | **Rejected** |
| Win prob | elo-v1 | 4,066 games (2010–2024) | 0.2247, BEATS | n/a (no ML odds) | n/a | 0.75 | Research |

*Run: `replay-elo-v1-2010-2024.json` (+ `evaluation_v1` standardized
artifact, seed=7), edge_min=0.03, rolling-origin walk-forward, no lookahead.
Bootstrap (B=1000): spread ROI 95% CI [−0.074, −0.009], P(ROI>0)=0.01;
total ROI 95% CI [−0.092, −0.026], P(ROI>0)=0.00. The CIs sit entirely below
zero — a statistically confident rejection. Automated governance
(`model/governance.py`) verdict: **REJECT** on both markets (only the
sample-size check passed). Ledger: `model/research/results/experiments.jsonl`.*

### Reading the elo-v1 rows

The Elo prices its taken spread/total sides at ~+10pp over breakeven on
average — it *believes* it has large edge — yet realizes −4% to −6% ROI with
calibration slopes near zero. That is textbook overconfidence: the closing
line already contains everything this Elo knows, and more. The model's
information set (past scores) is a strict subset of the market's. This is
why the promotion gate requires beating the closing line, not a naive
baseline — and why elo-v1 will never graduate on spreads/totals.

Win probability beats the home-win baseline but with slope 0.75
(under-confident at the extremes). No moneyline prices exist in the
historical data, so betting cannot be simulated; forward moneyline captures
will decide this row.

## Promotion bar (all required)

1. Positive simulated CLV-proxy (avg edge pp > 0) **and** positive
   simulated ROI at realistic vig, out-of-sample.
2. Beats Brier **and** log-loss baselines on the same window.
3. Calibration slope within [0.8, 1.2].
4. ≥ 500 independent bets (not games — bets the strategy actually takes).
5. Profitable in a majority of individual seasons (robustness, not one
   lucky year).
6. Profitable after realistic vig (already in the sim at −110).
7. No dependence on a single team, book, or season (leave-one-out check).
8. **Minimum effect size**: realized edge ≥ 1.0pp. A statistically positive
   but negligible edge does not promote (multiple-testing protection).
9. **Untouched holdout**: ≥ 200 bets with positive ROI on seasons never
   used during feature/model development. The holdout seasons are declared
   in the experiment ledger (`holdout_seasons`) before the final read and
   recorded immutably; the evaluation artifact carries matching
   `holdout` economics. Absence fails the check — no exceptions for
   "promising" families.

## Alpha attribution (kept separate forever)

Every opportunity is attributed across three independent sources, which
must sum to the quoted edge:

- **football alpha**: the model knows something about the game the market
  hasn't priced (residual-v1 vs the closing line).
- **market alpha**: a temporarily stale/off-market quote (LOBO signal —
  tagged `alpha_source: "market"` at the source).
- **execution alpha**: multiple books agree on fair value but one book
  offers a materially better line/price (best-price selection).

`select_opportunities` validates the attribution sums to `edge_pp` and
raises on mismatch. Selected outputs always carry the attribution
(zeros when unknown). When performance deteriorates, this tells us
which edge disappeared.

## Data honesty

- `spread_line`/`total_line` in nflverse are **consensus** lines, not
  book-specific closes. The true closing-line test runs on forward
  captures (opener/T-24/T-3/T-90/Close), which record both book and line.
- No historical intraday movement exists in this data: the lab cannot
  evaluate opener-vs-close timing. That analysis begins once forward
  captures accumulate.
- Simulated ROI assumes −110 both sides, flat 1u, no limits or slippage.
  It is a screen, not a P&L promise.

## Next families to enter the tournament

- `market-only-v1`: line-movement / book-disagreement features, no team
  strength model. Tests the "price discovery > prediction" hypothesis.
- `logistic-v1`: logistic regression on rest days, home/away, recent form.
- `epa-v1`: play-by-play EPA features (needs nflverse pbp ingest).
- `ensemble-v1`: only after two families independently reach Candidate.

## Historical backfill (paid tier, 2026-09-12)

The free tier blocks historical endpoints (403
HISTORICAL_UNAVAILABLE_ON_FREE_USAGE_PLAN), so market-only research had
no training data for line movement. The 20K/month paid tier unlocks
5-minute-granularity historical snapshots since Sep 2022.

`ops/backfill_history.py` (manual workflow `.github/workflows/backfill-history.yml`):

- Per week: Wednesday 12:00 UTC (early), Saturday 12:00 UTC (late),
  Sunday 15:30 UTC (~90 min before the 1pm ET slate; close proxy).
- One bulk call per snapshot: `10 x markets x regions` credits.
  Default (spreads,totals x us,eu) = 40/snapshot; 3 x 18 weeks x 3 seasons
  (2022-2024) ~= 6,500 credits total.
- Raw envelopes -> `nfl_edge_market_history`; normalized no-vig quotes ->
  `nfl_edge_historical_quotes` (same grain as `nfl_edge_odds_quotes`,
  source='historical_backfill'). Idempotent via ON CONFLICT DO NOTHING.
- Fails fast on free-tier 403; 429 is a hard stop, never retried in a loop.

Pinnacle (eu region) is the sharp anchor for market-only-v1. The live
collector takes `NFL_EDGE_REGIONS` (default "us") and `NFL_EDGE_BOOKMAKERS`;
after the tier upgrade, set `NFL_EDGE_REGIONS="us,eu"` and extend the book
list with `pinnacle` (signal-only, ny_licensed=false), `williamhill_us`
(Caesars) and `fanatics`. Live cost becomes 4 credits/request (2 markets x
2 regions); widening the book list stays quota-neutral.
