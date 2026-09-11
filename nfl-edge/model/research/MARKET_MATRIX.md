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

*Run: `replay-elo-v1-2010-2024.json`, edge_min=0.03, rolling-origin
walk-forward, no lookahead. Per-season: 13/15 seasons negative ROI on both
spread and total — a robust rejection, not noise.*

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
