# NCAAF Phase H — Challenger Tournament (Master Preregistration)

**Date:** 2026-09-15. **Phase:** H (Challenger tournament).
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`.
**Tournament universe:** 2022–2024 ONLY. 2025 is sealed.
**Sign oracle:** CFBD pre-game lines (A4 method) where needed.

## Governance (user-directed 2026-09-15)

### 2025 is not a tournament sandbox
2025 is our cleanest historical confirmation set. The tournament runs on
2022–2024. Only **preregistered finalists** get a **one-shot 2025
evaluation**. No casual 2025 use, no 2025 tuning, no second shots.

### Five hypotheses, not parameter grids
The tournament tests five genuinely different ideas (H1–H5 below).
Multiple parameterizations of the same idea belong to **one hypothesis
family** and share multiplicity control. We do not test dozens of
variations.

### Predict first, bet later (contract change)
Challengers are NOT required to produce bets. Stage 1 asks: does this
predict something **economically relevant**?
- Future market movement (direction/magnitude), or
- Residual outcome (component unexplained by market), or
- Closing-line improvement (CLV).

Only mechanisms that survive Stage 1 become betting strategies (Stage 2).
This dramatically reduces ROI-noise mining.

### The funnel
1. **2022–24 discovery / rolling week-level OOS** — preregistered hypotheses,
   chronological origins, no future info in features, clustered uncertainty.
2. **Multiplicity-adjusted selection** — Holm-Bonferroni across active
   families (family-wise α=0.05). A family advances only if its primary
   Stage-1 test survives.
3. **Freeze finalist(s)** — hypothesis, features, model, selection rule,
   thresholds all frozen.
4. **One-shot 2025 confirmation** — the frozen finalist is evaluated once
   on sealed 2025. No tuning. Pass/fail against preregistered thresholds.
5. **Freeze production candidate** — only on 2025 confirmation.
6. **Prospective 2026 shadow** — from the model-freeze timestamp forward.

**2025 remains unopened** through all 2022–24 work. Finalist selection and
complete freeze precede the single 2025 question.

### The null is a first-class result
Phase F established a formidable baseline: ~50.5% home cover, |p−0.5|
≈ 0.1pp, calibrated probabilities. H is not trying to predict football
better in isolation; it seeks **information that improves upon the market**
after vig, timing, and execution. If H finds nothing, we broaden the
information set (injuries/QB, weather, roster continuity, travel/rest,
play-by-play, more markets) — we do not weaken gates.

### Failure thresholds (per challenger)
Each H prereg defines, BEFORE seeing results:
- Primary statistical test and its null
- Economic relevance bar (Stage 1)
- Betting viability bar (Stage 2, if reached)
- Conditions under which the hypothesis is **abandoned**

## The five hypotheses

### H1: Inter-window market-movement prediction (highest priority)
**Q:** Using information at window t, can we predict direction/magnitude of
consensus movement at t+1?
**Stage 1:** Predict `next_window_line − current_window_line` from
current-window features. Pinnacle-relative features allowed as predictors;
H1 makes NO causal/leadership claim about Pinnacle.
**Target hierarchy (preregistered):** Primary = next-window consensus movement.
Secondary = movement-to-close. One hypothesis family.
**Validation:** Rolling week-level origins (train through week k, predict
subsequent weeks). Game/week-clustered SEs. Report 2022/2023/2024 stability
separately. More folds ≠ more independent seasons.
**Stage-1 gate (revised):** Statistically credible, directionally stable across
folds, positive OOS improvement over null, economically signed correctly.
Report magnitude with uncertainty; no arbitrary knockout threshold.
**Why first:** F1 showed real Wed→Sat movement (spreads 0.67 / totals 1.03
mean |Δ|). If predictable, it's directly executable (beat the move).
**Prereg:** `ncaaf-h1-movement.md`.

### H2: Pinnacle lead/lag dynamics
**Q:** Does a **change** at Pinnacle predict subsequent **consensus**
movement?
**Stage 1:** Timestamped Pinnacle line changes → consensus movement in the
next window. Null: Pinnacle changes have no predictive power for consensus.
**Distinct from F:** F showed Pinnacle's *static level* ≈ consensus. This
tests *changes*, not levels.
**Prereg:** `ncaaf-h2-pinnacle-lead.md`.

### H3: Football-information residual model
**Q:** Does opponent-adjusted team strength predict the **residual** —
the component of outcomes the market doesn't explain?
**Stage 1:** Model P(cover) or expected margin from (team strength +
market line); target is residual vs market-only baseline. Separate spread
and total models. Null: team strength adds nothing beyond the market line.
**Prereg:** `ncaaf-h3-residual-model.md`.

### H4: Underdog mechanism — DOWNGRADED to discovered
**Q:** What explains the 48.8% / 52.1% favorite/underdog split (F3)?
**Status:** The *split* was preregistered (F3); the *direction* was not.
Therefore 2022–24 establishes observation/mechanism-search material, not
confirmation. H4 is a discovered hypothesis.
**Stage 1:** Test whether identifiable subsets (spread magnitude, home/away,
conference) or H3 residuals explain the pattern. Null: the lean is uniform
noise across subsets.
**Not a strategy:** 52.1% is below 52.4% breakeven at -110. "Bet underdogs"
is not a challenger. We seek the **mechanism**, if any.
**Prereg:** `ncaaf-h4-underdog-mechanism.md`.

### H5: FBS-vs-FCS (segregated discovered hypothesis)
**Q:** Does the FBS-vs-FCS lean replicate on uninspected evidence?
**Status:** Discovered on 2022–24 (55.6% home cover, n=942). Those observations
are **burned** for confirmation.
**First:** Sign-independent identity check (FBS-home vs FCS-home vs favorite
relationships) — data-contract verification, not hypothesis testing.
**Then:** Define the economically meaningful exposure (likely FBS side vs FCS,
not blindly "home") BEFORE preregistration. Do not choose the definition with
the best historical cover rate.
**Confirmation:** Reserved untouched evidence (2025 one-shot, as finalist).
The 2022–24 result is the hypothesis generator, never the test statistic.
**Prereg:** `ncaaf-h5-fbsfcs.md` (+ `ncaaf-hypothesis-fbsfcs.md` discovery record).

## Multiplicity
Four active families (H1, H3, H4, H5). H2 dormant. Primary Stage-1 test per
family. Holm-Bonferroni, FWER 0.05. Secondary/exploratory analyses within a
family are labeled as such and do not advance the family.

## What advances
A family advances to finalist IFF:
1. Primary Stage-1 test rejects the null (multiplicity-adjusted), AND
2. Effect size is economically meaningful (preregistered bar), AND
3. Effect is stable across 2022/2023/2024 (no single-season driver).

Then: freeze → one-shot 2025 → (if confirms) production candidate.

---
*Supersedes the "build v0 → immediately spend 2025" sequence. 2025 is
spent once, on finalists, or not at all.*
