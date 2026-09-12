# Preregistration: residual-v1 (challenger residual vs closing-line movement)

Status: SHADOW research only. Written and committed BEFORE any real-data
execution. Nothing in this file may be edited after results are observed
except via a dated amendment recorded before any new analysis.

## 1. Question

Does challenger v1's disagreement with the market predict the subsequent
closing-line move — i.e., does the close move *toward* the challenger?

This is a mechanism question, distinct from the challenger's gate question
("does the challenger beat closing lines outright", already refused). The
challenger may be directionally informative about market movement even if
it cannot beat the close outright.

## 2. Challenger training-window audit (done before preregistration)

- Artifact `model/challenger/artifacts/challenger-v1/model.json`:
  `train_end=2021`, 6,137 nflverse games (1999–2021), static Elo ratings,
  fixed hyperparameters (K=0.15, HFA=1.2, league_avg=22.0), role=research.
- Inference loads static ratings; no in-window rating updates.
- => 2022–2024 is a POINT-IN-TIME-VALID evaluation window: the challenger
  never saw those seasons' scores.
- Caveat: 2022–2024 was also the challenger's gate-validation window, so
  this is NOT independent evidence of challenger skill — it is a
  pre-declared mechanism test on a shared window. Honesty requires stating
  this in any report.

## 3. Dataset and units

- Frozen historical dataset, fingerprint `43f853a44bf93937d85149ca5fd7241b`.
- Units: canonical (game, market) for full-game spreads/totals, built with
  the same slot/line/consensus rules as market-only-v1: modal line L at
  close (latest pre-kickoff snapshot, ≤72h before kickoff), sat = latest
  snapshot ≥24h before close, exact-line LOBO consensus with multiplicative
  de-vig, ≥2 books quoting the exact line at sat and close.
- Challenger availability: skip a unit if the challenger returns None
  (unknown team abbreviation etc.).
- Feasibility: fewer than 200 units with challenger predictions => infeasible.

## 4. Variables (all pre-declared)

- f_sat(L): sat consensus fair prob for the reference selection (home / Over).
- f_close(L): close consensus fair prob for the reference selection.
- chal_fair(L): challenger no-vig fair prob for the reference selection at L:
  - totals: `over_prob(pred_total, L, sigma_total)`.
  - spreads: `cover_prob_home(pred_margin_home, home_line, sigma_margin)`
    with the home-perspective nflverse line `home_line = +L` if the close
    market has the home team favored (`f_close(home) >= 0.5`) else `-L`.
    The favorite is inferred from market prices only — no scores, no leakage.
- r = chal_fair(L) − f_sat(L)   (the challenger's disagreement, sat view)
- move = f_close(L) − f_sat(L)  (did the market move toward the challenger?)

## 5. Primary test (ONE test)

OLS regression: move = a + b·r (+ intercept). One-sided t-test on the
slope, H0: b ≤ 0 vs H1: b > 0, α = 0.05. Pooled across spreads and totals
(both are probability-scale disagreements); no multiplicity correction
because there is exactly one confirmatory test.

## 6. Decision rule (locked)

"signal" iff ALL hold:
  (a) primary p < 0.05 (one-sided);
  (b) slope b ≥ 0.2 — at least 20% of the challenger disagreement is
      realized in the close move (practical bar; below this the move is
      too small to cover any execution friction);
  (c) b > 0 in at least 2 of the 3 held-out seasons (robustness).
Otherwise: "no_edge". A "signal" result only creates a forward-shadow
candidate; nothing may be promoted on historical results.

## 7. Null simulation (before real data)

- Martingale null: r, move ~ independent N(0, σ) draws; 200 seeds; the
  one-sided t-test must reject at ≤ 0.15 (nominal size check).
- Planted signal: slope 0.5 with realistic noise; the test must reject
  (power sanity check).
- If miscalibrated: amend this preregistration BEFORE touching real data.

## 8. Secondary / exploratory (non-decisive)

- Per-market (spread-only, total-only) slopes; mean |r| (typical
  disagreement size); per-season slopes.
- Exploratory sketch: hypothetical CLV of fading the sat consensus toward
  the challenger in top-decile |r| units (reported, never decisive).

## 9. Amendment log

- 2026-09-12: initial preregistration (this file).
