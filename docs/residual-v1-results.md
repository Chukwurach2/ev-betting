# Residual-v1 results

Role: **SHADOW research only.** Preregistered in
`docs/preregistrations/residual-v1.md` (committed before execution, with the
challenger training-window audit). Null simulation verified the primary
test calibrated (rejection rate 0.050 at α=0.05 over 200 seeds; planted
slope-0.5 power check passed in unit tests). No amendment was needed.
Dataset: frozen 2022–2024, fingerprint verified in the workflow
(291,586 quotes, 162 snapshots). Zero API credits. Workflow run
34698859132, success.

## Design (recap)

r = chal_fair(L) − f_sat(L) (challenger disagreement at the Sat snapshot);
move = f_close(L) − f_sat(L). One confirmatory test: OLS move ~ r,
one-sided t-test slope > 0, α=0.05, pooled over spreads/totals.

## Results

704 units with challenger predictions (feasibility floor: 200).

| scope | n | slope | p (one-sided) |
|---|---|---|---|
| pooled | 704 | +0.0010 | 0.1218 |
| 2022 | 223 | +0.0011 | 0.1737 |
| 2023 | 212 | +0.0007 | 0.3376 |
| 2024 | 269 | +0.0012 | 0.2116 |

Mean |r| = 0.36 — the challenger (static 2021 ratings) disagrees with the
market wildly, as expected for 3-year-stale ratings; the market does not
move toward it.

## Verdict: `no_edge`

Per the locked decision rule, "signal" required (a) p < 0.05, (b) slope ≥
0.2, (c) slope > 0 in ≥2/3 seasons. The primary test is not significant
(p=0.12) and the estimated slope (+0.001) is three orders of magnitude
below the practical bar. The challenger's residual carries no information
about subsequent closing-line movement on this window.

## Bottom line

Consistent with the challenger's own gate result (spread/total did not
beat closing lines): a stale-rating Elo has no exploitable residual on
2022–2024 closes. Residual-v1 is closed as an alpha candidate. Nothing
advanced from SHADOW; no picks or recommendations were generated.
