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
