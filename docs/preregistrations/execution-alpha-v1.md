# Preregistration: execution-alpha-v1 (line-shopping CLV, NO prediction)

**Status: PREREGISTERED — no results viewed. Analysis code does not yet exist.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Dataset (frozen): fingerprint `43f853a44bf93937d85149ca5fd7241b`,
  162/162 snapshots, 291,586 quotes. Freeze verification before running:
  quote count = 291,586 and distinct snapshots = 162, else the run stops.
- Scope: FULL_GAME_SPREAD and FULL_GAME_TOTAL, 2022-2024.
- Question: does PURE EXECUTION — always taking the best available price
  across books at each snapshot, with zero prediction — beat the closing
  consensus? This is distinct from the market-alpha-v1 stale-price mechanism
  test (which found no lagging mechanism): here the shopping bias itself IS
  the strategy under test.

## Definitions (pre-declared)

- **Game identity:** canonical keys from the event-identity audit
  (`(home,away)` + kickoff-proximity clustering), reusing
  `canonical_game_keys` in `research/devig_tournament.py`.
- **Closing snapshot** per game: the last snapshot with
  `observed_at < kickoff`.
- **Cells:** per (canonical game, market, snapshot s strictly before the
  closing snapshot, exact line L): books with a valid two-sided pair at L;
  require >= 2 books. Exact-line comparability (Amendment A1 rule): books at
  different lines are never mixed.
- **Pair selection:** per (game, market, snapshot, book): the book's pair at
  its modal line within the snapshot; ties broken by closeness to the
  cross-book median line; deterministic (de-vig tournament rule).
- **Reference selection:** spreads -> home team; totals -> Over.
- **Fair probabilities:** multiplicative de-vig (tournament winner),
  recomputed from raw `american_odds` on the book's own pair.
- **Best book** b\* per cell = argmax fair prob of the reference selection.
- **Edge** per cell = f_b\*(s,L) - C_close(L), where C_close(L) = median fair
  prob over books != b\* offering EXACTLY line L at the closing snapshot
  (strict LOBO: the evaluated book never enters its own benchmark); require
  >= 2 such books (complete-case, else cell dropped).
- **Game-level edge:** mean of cell edges within the game. **Primary
  metric:** mean of game-level edges across games with >= 1 cell, with 95%
  CI. Minimum 30 games, else `infeasible`.

## The Amendment A1 lesson, applied (pre-declared)

Under the noise null, best-price selection has POSITIVE expected edge
(mechanical shopping bias). The confirmatory test is therefore NOT
"mean > 0". It is **observed vs a simulated noise null**:

- **Null DGP:** for each observed cell, let C = observed same-snapshot
  cross-book median fair prob at line L, sigma = observed cross-book std
  at that cell. Null: every book's fair prob = C + iid N(0, sigma);
  the closing benchmark = median of books' null draws at the close.
  (True price constant within cell; books synchronous — no staleness, no
  signal. This null GENERATES the mechanical shopping bias, so beating it
  means beating mere noise-shopping.)
- Simulate 500 replicates of the full procedure on the observed cell
  structure -> null distribution of the mean game-level edge.
- **Confirmatory p** = (1 + #{null replicates >= observed}) / 501,
  one-sided, alpha = 0.05. This is the ONE confirmatory test (no
  multiplicity issue: single hypothesis).

**Calibration pre-check (A1 template, before real-data execution):**
generate data directly FROM the null DGP and run the whole procedure;
require the rejection rate ~= 5% at alpha=0.05 (else the test is
miscalibrated and must be amended pre-results).

## Decision rule (pre-declared)

- `execution_edge`: null-comparison p < 0.05 AND observed mean game-level
  edge >= 0.01 (practical bar: one probability point per $1).
- `no_edge`: otherwise.
- `infeasible`: < 30 games with >= 1 cell.

## Executability analysis (exploratory, pre-declared — the key limitation)

The primary metric ASSUMES the best quote was obtainable at observed_at.
Stale quotes may not be executable. Report, without changing the verdict:
(a) distribution of which books supply b\* (concentration risk);
(b) fraction of b\* cells where |f_b\* - same-snapshot consensus| >= 0.02
(potentially stale);
(c) edge concentration: share of total edge from the top 5% of games.
If the edge lives entirely in potentially-stale cells, the verdict stands
but the results doc flags `edge_but_likely_unexecutable`.

## Transparency

Point-in-time safe throughout (cells use snapshot s only; the close is an
ex-post benchmark). Zero API credits; read-only Neon. A positive finding
selects the hypothesis for FORWARD-SHADOW validation only — the mandatory
promotion gate remains positive CLV against actual closing lines on forward
data. Historical results never promote.
