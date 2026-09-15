# H4: Underdog Mechanism (Preregistration)

**Family:** H4. **Date:** 2026-09-15 (revised). **Stage:** 1 (explain pattern).
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**Status:** DOWNGRADED to discovered hypothesis. The *split* was preregistered
(F3); the *direction* was not. 2022–24 provides observation/mechanism-search
material, not confirmation. Any mechanism found here requires fresh
confirmation data before it can advance.

## Hypothesis (discovered, not confirmatory)
The F3 finding (home fav 48.8% / home dog 52.1%) reflects an identifiable
mechanism, not uniform noise. We seek the mechanism. **This is mechanism
analysis on the discovery sample** — explanatory only. It can design the
next hypothesis but cannot claim confirmatory evidence from 2022–24.

## Why not "bet underdogs"
52.1% is below the 52.4% breakeven at -110. "Bet all underdogs" has negative
EV. H4 does not test a betting strategy; it tests **explanations**.

## Candidate mechanisms (preregistered, tested in order)
1. **M1 (spread magnitude):** The lean is stronger in large spreads
   (|line| ≥ 14) where public favorite bias is strongest. Test: cover rate
   by |line| bucket × fav/dog.
2. **M2 (home/away):** The lean is home-specific (home dogs cover more than
   away dogs). Test: home-dog vs away-dog cover rates.
3. **M3 (H3 residual):** The H3 Elo residual explains the lean (i.e., the
   market systematically overprices favorites relative to Elo). Test:
   correlation between Elo-implied edge and fav/dog cover rates.

## Statistical tests
- M1: Interaction test (fav/dog × |line| bucket) in logistic regression.
- M2: Two-proportion z-test (home-dog vs away-dog).
- M3: If H3 finds a significant Elo coefficient, check if it accounts for
  the fav/dog gap (mediation).

**Primary: M1 interaction.** (M2, M3 secondary.)

## What H4 produces
- If a mechanism is found: it becomes a **feature** for the H3 residual
  model or a standalone H-finalist (with its own 2025 confirmation).
- If no mechanism: the 52.1% is unexplained noise. H4 is abandoned. We do
  not bet underdogs.

## Failure threshold
- M1 interaction p (Holm-adjusted) > 0.05 AND M2 p > 0.05, OR
- No mechanism explains >50% of the 3.3pp gap.

---
*Stage 1 only. Explains the pattern; does not bet it.*
