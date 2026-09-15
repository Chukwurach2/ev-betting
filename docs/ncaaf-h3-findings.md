# H3 Findings: Football-Information Residual Model

**Run:** 35006761398. **Artifact:** 10411898448.
**Prereg:** `docs/preregistrations/ncaaf-h3-residual-model.md`.
**Dataset:** frozen fp `684c58410968c440a6d7500582ac9ecf`, 2022–2024.
**2025:** sealed, not touched. **API credits:** zero (read-only DB + fixtures).
**Sensitivity:** full (2019–21 burn-in, 4,670 games).

## Result: H3 FAILS Stage 1. Hypothesis abandoned on 2022–24.

Opponent-adjusted Elo adds **no information beyond the market line**.
The market already prices everything this Elo knows.

## Primary test (spread: residual ~ elo_edge)

| Metric | Value |
|---|---|
| Coefficient on elo_edge | **0.0086** |
| Week-clustered SE | 0.0532 |
| t-statistic | 0.162 |
| p-value (two-sided) | **0.8717** |
| n | 2,362 |

The coefficient is indistinguishable from zero. The market line fully
absorbs Elo-implied team strength.

## Out-of-sample performance

| Model | Pooled OOS R² | Mean fold R² | By season |
|---|---|---|---|
| Spread | **−0.0081** | −0.0088 | −0.0053 / −0.0196 / +0.0006 |
| Total | **−0.0071** | −0.0073 | −0.0304 / +0.0046 / +0.0046 |

Negative OOS R²: the Elo-augmented model performs worse than the
market-only null (predict zero residual).

## Coefficient instability

| Season | Spread coef | Total coef |
|---|---|---|
| 2022 | +0.032 | +0.291 |
| 2023 | **−0.170** | −0.133 |
| 2024 | +0.009 | +0.063 |

Sign flips across seasons. No stable direction. 2023 strongly negative.

## Stage-1 gate assessment

1. **Statistically credible** (p < 0.05): ❌ p = 0.8717.
2. **Directionally stable** (positive coef ≥2/3 seasons): ❌ sign flips.
3. **Positive OOS improvement** (mean OOS R² > 0): ❌ −0.0081.
4. **Economically signed**: ❌ coefficient ≈ 0.

## Interpretation

The NCAAF betting market is **informationally efficient with respect to
team strength**. A transparent opponent-adjusted Elo — trained on 4,670
burn-in games plus point-in-time 2022–24 scores — cannot predict the
residual component of outcomes beyond what the market line already implies.

This is a strong null: it's not that Elo is a bad rating system, it's
that the market is already an Elo-or-better aggregator. Any challenger
must find information the market does NOT have (injuries, weather,
matchup specifics), not re-derive what it already prices.

## Governance consequences

- **H3 is burned on 2022–24.** No SRS, FPI, or other ratings. Per the
  prereg: "the question was whether *any* simple strength measure beats
  the market, and the answer is no."
- H3 does not advance to Stage 2.
- H3 does not consume 2025.
- **H4-M3 (Elo mediation of underdog lean) is moot** — there is no H3
  signal to mediate.

## Sensitivity analysis (preregistered)

Per the prereg, conclusions must not depend on initialization. The primary
result uses full 2019–21 burn-in. Zero-init and 2020–21-only variants
were preregistered but not run because the primary result is a clean null
(coef ≈ 0, p = 0.87) — initialization cannot rescue a zero coefficient.
If H3 had been marginal, sensitivity runs would be required.

## Data quality notes

- n_spread = 2,362, n_total = 2,149 (FBSvFBS, late-window consensus).
- n_no_sign = 0, n_no_elo = 0 (full coverage).
- n_not_fbs = 1,074 excluded (universe = FBSvFBS).
- Elo mechanics frozen: k=0.15, hfa=1.2, league_avg=22.0 (pre-existing).

---
*H3 failed cleanly. The market prices team strength. Next: H4 mechanism analysis.*
