# H4 Findings: Underdog Mechanism Analysis

**Run:** 35007069363. **Artifact:** 10412505274.
**Prereg:** `docs/preregistrations/ncaaf-h4-underdog-mechanism.md`.
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**2025:** sealed, not touched. **API credits:** zero.
**Status:** DISCOVERED hypothesis (downgraded). Explanatory only.

## Result: Hypothesized mechanisms NOT supported. Descriptive pattern found.

The 48.3% / 52.3% favorite/underdog gap (4.0pp, n=1,903) is **not** explained
by the preregistered mechanisms. However, the lean has a clear structure:
it is concentrated in small spreads and vanishes in large spreads — the
**opposite** of the public-bias hypothesis.

## M1: Line magnitude (PRIMARY) — hypothesis refuted

| Bucket | Fav cover (n) | Dog cover (n) | Dog − Fav |
|---|---|---|---|
| [0,3) | 42.4% (165) | 50.4% (125) | **+8.0pp** |
| [3,7) | 49.0% (286) | 55.4% (184) | **+6.5pp** |
| [7,14) | 48.9% (313) | 53.1% (245) | +4.2pp |
| [14,∞) | 49.7% (443) | 48.6% (142) | −1.1pp |

**M1 predicted** the lean would be strongest in large spreads (public
favorite bias). **Observed:** the lean is strongest in [0,3) and
disappears entirely at [14,∞). The hypothesized mechanism is refuted.

**Descriptive pattern:** The market appears to overvalue home-field (or
undervalue road teams) specifically in close games. When the spread is
1–3 points, home favorites cover only 42.4% — the market's HFA estimate
exceeds reality. In blowouts, the talent gap dominates and the lean
vanishes.

This pattern is **explanatory only** on burned 2022–24 data. It can inform
a future hypothesis (e.g., "HFA is overpriced in pick'em games") but cannot
serve as confirmatory evidence.

## M2: Home/away (SECONDARY) — not supported

| | Cover | n |
|---|---|---|
| Home dog | 52.3% | 696 |
| Away dog | 51.7% | 1,207 |
| Difference | +0.6pp | p = 0.80 |

No significant difference. The lean is **not** home-specific. Both home
dogs and away dogs cover at ~52%.

## M3: H3 mediation — MOOT

H3 found no Elo signal (coef 0.0086, p=0.87). There is nothing to mediate.

## What this means

1. **The underdog lean is real but small** (4.0pp) and **below breakeven**
   (52.3% < 52.4%). It was never a betting strategy.
2. **It's not public favorite bias.** That story predicts the opposite
   pattern (stronger in large spreads).
3. **It's concentrated in close games**, suggesting a specific HFA
   mispricing rather than a general favorite/dog bias.
4. **It's not home-specific** — away dogs benefit equally.

## Governance consequences

- H4's preregistered mechanisms (M1-public-bias, M2-home-specific, M3-Elo)
  are **not supported**. H4 does not produce a confirmatory finding.
- The descriptive small-spread pattern is **hypothesis-generating only**.
  A future "HFA-overpricing in close games" hypothesis would be a **new
  family** requiring fresh confirmation data (not 2022–24).
- H4 does not advance. We do not bet underdogs.

## Data quality notes

- n = 1,903 (FBSvFBS, late-window consensus, pushes excluded).
- n_no_sign = 0, n_not_fbs = 505 excluded.

---
*H4's mechanisms refuted. The small-spread pattern is a lead for future
research, not a finding. Next: H5 awaits 2025; Phase H 2022–24 work complete.*
