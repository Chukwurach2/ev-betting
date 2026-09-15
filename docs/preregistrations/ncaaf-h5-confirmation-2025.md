# H5: 2025 One-Shot Confirmation (FROZEN)

**Family:** H5. **Date frozen:** 2026-09-15.
**Status:** FROZEN. No changes after this commit.
**2025 data:** NOT YET RETRIEVED. This document is hashed before any query.

## Hypothesis (from discovery)
FBS home teams cover the spread against FCS opponents at a rate exceeding
the breakeven threshold, reflecting talent-gap mispricing.

**Discovery:** 2022–24 home cover 55.6% (n=942). Those observations are
burned. This is the single confirmatory test.

## Frozen rule

**Selection:**
- Games where home team classification = "fbs" AND away team
  classification = "fcs" (per CFBD).
- Season = 2025. Regular season + conference championships + bowls/playoffs.
- Market: FULL_GAME_SPREAD.

**Side:** Home team (the FBS side).

**Price source — "early consensus":**
- Window: Wednesday 12:00 UTC of game week (same "early" definition as F1/H1).
- Consensus: median across all books quoting that game/window.
- Minimum 3 books required; games with <3 books excluded and counted.

**Sign/orientation:**
- CFBD pre-game spread as sign oracle (A4 method). Negative = home favored.
- Home cover determined from signed spread and actual score margin.

**Push handling:**
- Pushes (margin + spread == 0) are EXCLUDED from the binomial denominator.
- Number of pushes reported separately. Push rate >10% triggers a data
  quality review (not a re-test).

**Missing/unmappable games:**
- Games without CFBD classification, without 3+ book early consensus, or
  without a CFBD sign oracle are EXCLUDED and counted by reason.
- No imputation. No substitution.

**Pricing assumption:**
- Breakeven = 52.38% (from -110 pricing: 1.10/2.10).
- Rounded to 52.4% for the threshold.

## Primary estimand
True home cover probability, p, for the frozen H5 rule on 2025 games.

## Statistical method (frozen)
**Wilson 95% confidence interval** for a binomial proportion.
- Not Wald. Not Clopper-Pearson. Wilson, preregistered.
- Computed on (covers, non-push decisive games).

## Three-way decision rule (frozen)

Let [L, U] be the Wilson 95% CI for p. Let n = decisive games.

- **CONFIRMATORY:** n ≥ 100 AND L > 52.4%.
  → H5 advances to execution/economic validation (Stage 2).
  → NOT automatically a production betting edge.

- **REFUTED:** n ≥ 100 AND U < 52.4%.
  → H5 is dead. No re-testing on any data.

- **INCONCLUSIVE:** Everything else, including n < 100.
  → H5 remains an unresolved candidate.
  → Requires genuinely new prospective observations.
  → NOT permission to interrogate 2025 further.

This asks the exact economic question: does the clean holdout establish
that the true cover probability is above or below breakeven?

## 2025 firewall (claim-scoped)

**Permitted:**
- Retrieve 2025 FBS-vs-FCS games (CFBD: scores, classifications).
- Retrieve early-window spread consensus for THOSE GAMES ONLY.
- Retrieve CFBD 2025 pre-game lines (sign oracle) for THOSE GAMES ONLY.
- Run the single frozen binomial test. Record result. Stop.

**Forbidden:**
- Inspecting 2025 small spreads, totals, Elo residuals, or movement.
- Testing H4-small or any other hypothesis on 2025.
- Alternative H5 definitions, windows, or thresholds.
- Post-result threshold changes.
- "Exploring why" if the result is null — record and move on.

The 2025 firewall is claim-scoped. One test has permission; the season
does not become a sandbox.

## What happens after

- **Confirmatory:** H5 enters Stage 2 (executable economics: CLV, slippage,
  position sizing under uncertainty). Still not production until Stage 2
  passes.
- **Inconclusive:** H5 waits for prospective 2026 shadow data. 2025 is
  not re-examined.
- **Refuted:** Phase H produced zero historically confirmed production
  candidates. Accepted. Research moves outward (richer information, other
  markets, H2 granular data, prospective hypotheses).

## Artifact hash
SHA256 (document above this section): `cfb5fa96e71de0ddf80c6db9f9cf5de236762522f7da2f439dd8bee58854bc3e`
Computed 2026-09-15 before any 2025 data retrieval.

---
*Frozen 2026-09-15. One shot. No second looks.*
