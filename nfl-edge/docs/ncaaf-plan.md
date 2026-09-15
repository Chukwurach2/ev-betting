# NCAAF Research Contract (Football Edge)

Status: FROZEN 2026-09-13 — governing NCAAF research contract (user-approved with two corrections).

Second sport stack inside Football Edge: separate evidence and models, shared platform.
NFL v1.3 remains frozen and completely isolated; NCAAF uses idle research capacity.

## 1. Frozen terms

### 1.1 Data windows
- **Development:** 2022–2025 seasons. Only snapshots timestamped before `2026-08-01T00:00:00Z`.
- **Prospective:** every 2026-season observation — live captures and any historical
  pull timestamped after the cutoff. Prospective data is never training data.
  No 2026 result feeds back into any model, threshold, feature, or market selection.

### 1.2 Scope
- Phase-1 markets: **FBS spreads and totals only.**
- Every game tagged FBS-vs-FBS or FBS-vs-FCS (or other). Analyze separately;
  never pool casually.
- Later phases (moneylines/team totals, then halves/quarters, then props) only
  after phase-1 promotion or an explicit re-plan. No player props in this contract.

### 1.3 Event and line identity
- Canonical event: `(home_team, away_team)` + kickoff-proximity clustering
  (same rule as the NFL post-freeze identity fix).
- Line-aware identity for all quotes, picks, and CLV:
  `(sport, event, market, selection, line)`.
- CLV is exact-line only: closing consensus fair probability at the same line
  minus taken-price de-vigged fair probability. Positive = beat the close.

### 1.4 Timing windows (point-in-time)
- Windows: Opener (first-seen, games 24h–6d out), T-24, T-3, T-90min, Close.
- Preserve **both** timestamps on every observation. Collector capture time
  determines window membership (whether the checkpoint landed in
  T-24/T-3/T-90/Close); provider observation/update time determines quote
  freshness (whether a quote is fresh enough to be recommendation-eligible).
  Never conflate the two — this is the distinction learned from the NFL
  forward-shadow audit.
- A checkpoint belongs to a window only if captured inside that window's declared
  interval. Late captures are diagnostic-only and never count toward promotion evidence.
- Never retroactively construct a T-24/T-3/T-90 observation from a later quote.

### 1.5 Price and book rules
- Same-book fair probabilities via multiplicative de-vig (NFL tournament winner).
- Cross-book consensus built from books actually observable at each timestamp.
  Pinnacle is absent from the us-region NCAAF feed: **not a blocker**; record as
  a limitation and construct a robust consensus from observable books.
- Any "executable price" claim must reference a NY-licensed book
  (draftkings, fanduel, betmgm, betrivers observed in the 2026-09-13 probe).

### 1.6 Settlement
- Final scores including overtime.
- Pushes (integer spread/total landing exactly): void — excluded from win/loss,
  recorded separately.
- Settlement mapping verified in the dataset audit before any modeling.

### 1.7 Promotion criteria (NFL-identical statistics and health; sample gates locked later)
- Positive exact-line CLV against actual closes, statistical significance
  with multiple-testing protection, calibration requirements, and
  operational-health gates (capture ≥90%, no anomalies, ≤10% of settled
  picks lacking exact-line closes) — all identical in kind to the NFL gate.
- Sample-size and duration gates are **not** hard-coded yet. Observe NCAAF's
  natural prospective qualifying frequency first, then lock them
  prospectively — an NFL-derived ≥200 picks / ≥8 weeks rule could be
  arbitrary against the much larger college slate.
- No forward-data threshold tuning. Only the locked gate plus the user's
  decision ends SHADOW.

## 2. Architecture

shared platform → sport=nfl|ncaaf → sport-specific schedules/models/settlement
→ common odds/checkpoint/CLV/evidence layer → common app.

Parameterize. Never fork into two divergent codebases.

## 3. Phase plan

- **(A) Review/finalize this contract.** ← we are here
- **(B) Implement sport parameterization** in the historical pull path
  (collector, settlement, and app follow as their phases arrive).
- **(C) Cheapest production-key probe:** one week × a few decision timestamps.
  Verify historical availability, timestamps, book identities, line
  representation, quota cost, settlement mapping. Gate: probe passes before any
  bulk pull is authorized.
- **(D) Start immutable 2026 collection** at the declared windows, independent
  of research progress. Every week of 2026 banked now is prospective evidence
  that can never be reconstructed later.
- **(E) Audit + freeze the 2022–2025 dataset.** Hard gate before any research:
  expected vs captured games; quote coverage by season/week/book/window;
  duplicate IDs; timestamp integrity; signed spreads; paired outcomes;
  push-capable integer lines; settlement coverage; provider-credit
  reconciliation. Produce a deterministic dataset fingerprint. No modeling
  until it passes.
- **(F) Market baseline.** Measure how well de-vigged consensus predicts
  ATS/total outcomes by season, conference, spread magnitude,
  favorite/underdog, total range, and decision window. The question every
  challenger must answer is whether it adds information **beyond the available
  betting market** — not whether a football model predicts games.
- **(G) Preregister v0, then evaluate.**

## 4. v0 challenger (preregistration skeleton)

- Deliberately boring: opponent-adjusted team strength + market consensus.
- Train chronologically 2022→2024; test on untouched 2025.
- Candidate inputs: EPA/play, success rate, explosiveness, pace,
  offensive/defensive strength, home field, opponent adjustment.
- Dependent variable: the model's **residual versus the market**, not a
  from-scratch manufactured spread.
- Spread and total families modeled separately.
- Richer information families — talent/recruiting, returning
  production/transfers, QB continuity, coaching changes, injuries, travel/rest,
  weather — are separate preregistered challengers **after** v0 is evaluated.
  This buys attribution: we learn whether an information family improves
  calibration instead of merely increasing model complexity.

## 5. Shadow promotion

- A NCAAF model enters shadow only after passing on 2025. Then frozen.
- From that point, 2026 evaluates it exactly like NFL v1.3: qualifying signals,
  exact entry prices, subsequent exact-line closes, CLV, calibration,
  fixed-unit ROI, pipeline health.

## 6. Open questions

- Pinnacle NCAAF coverage via alternate regions.
- Exact historical cost per NCAAF snapshot (phase C probe).
- Challenger feature data source (collegefootballdata free tier is the likely candidate).
- Live 2026 capture cadence vs quota (phase D design).
- Whether 57 games × books × snapshots changes the promotion sample math
  (larger prospective laboratory than NFL — revisit after phase F).

## 7. Contract amendment (2026-09-15, user-directed)

Sections 1–6 above remain the frozen original. This amendment revises the
research path; where it conflicts with the original, the amendment governs.

### Revised phase path

- **E ✓ Frozen dataset** — done 2026-09-15 (fingerprint
  `684c58410968c440a6d7500582ac9ecf`; receipt `docs/ncaaf-dataset-freeze.md`).
- **F Market-efficiency map / baselines** — comprehensive alpha map of the
  frozen **2022–24 development sample only; 2025 stays sealed**. Per market
  (spread, total) × decision window: de-vigged consensus accuracy vs outcomes,
  book disagreement, line movement toward close, market-implied calibration.
  Establishes the baseline every challenger must beat.
- **G Preregistered v0 + 2022–24 walk-forward** — simple market+team-strength
  benchmark, nested chronological walk-forward inside 2022–24. 2025 untouched.
- **H Preregistered challenger tournament on 2022–24** — genuinely
  differentiated hypotheses, each separately preregistered: opponent-adjusted
  efficiency, pace/explosiveness, matchup interactions, injuries/QB
  information, weather, travel/rest, market disagreement, price movement,
  nonlinear models. v0 is never repeatedly modified; challengers are separate.
- **Freeze the complete strategy** — market(s), model, selection thresholds,
  execution rules — then and only then open 2025.
- **I One-shot untouched 2025 evaluation** — the sealed holdout is spent once,
  on the frozen strategy. No re-tuning on 2025, ever.
- **J Freeze production candidate.**
- **K Prospective 2026 evidence from the freeze timestamp onward** — a
  prospective eligibility timestamp is recorded when the final model/strategy
  hash freezes. 2026 data collected before that timestamp is archived, not
  prospective confirmation. Promotion only if executable CLV / calibration /
  ROI evidence survives on post-freeze 2026 data.

### Research rules

1. **2025 is genuinely untouched** until the complete strategy is frozen.
   All research, feature/model selection, and thresholds use 2022–24 with
   nested chronological walk-forward.
2. **No "one experiment per day" constraint.** Protection comes from the
   experiment registry, immutable results, hypothesis-family accounting,
   multiple-testing correction, and untouched confirmation — not calendar days.
   Multiple independent preregistered experiments may run efficiently.
3. **Beat the market, not football.** Every candidate must demonstrate
   incremental information relative to the contemporaneous betting market.
   Raw prediction accuracy is not the target.
4. **Execution is part of the research.** Backtests use only information
   available at the decision window, actual lines/odds available then,
   same-book de-vigging, realistic selection rules, pushes/voids, fixed-unit
   returns. Closing lines are an evaluation benchmark, never model input.
5. **Winner-selection metric (in order):** out-of-sample incremental
   calibration / log loss vs market → residual predictive power → CLV-like
   price movement where measurable → stability across seasons/books/line
   buckets → executable ROI with uncertainty. Historical ROI alone never
   selects the winner.
6. **Market/decision-window discovery is legitimate alpha research** on
   2022–24 (e.g. totals at T-24 carry signal while spreads at T-90 do not).
   What is forbidden: discovering anything on 2025/2026 and retroactively
   calling it preregistered.
7. **PASS is valid; picks are the objective.** Thresholds are never lowered to
   manufacture bets, but the program actively searches across legitimate
   hypotheses, markets, and windows. If repeated clean research finds no
   exploitable edge in NCAAF spreads/totals, we move to another information
   set or contract instead of endlessly optimizing the same problem.
