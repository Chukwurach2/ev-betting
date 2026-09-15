# Totals Price-Pressure Candidate v1.0 — FROZEN

**Frozen:** 2026-09-15
**Status:** SHADOW/FEASIBILITY through 2026-10-31. Prospective confirmation after firewall.
**Historical basis:** Run 35015417472 (descriptive), run 35015825735 (clustered).
**Historical result:** 67.1% directional accuracy, 95% CI [64.4%, 69.8%], n=1,190 pairs, 1,038 clusters.

## Rule Specification (immutable)

**Input:** Cross-book totals Over de-vig fair probabilities at snapshot t.

**Eligibility:**
- Market = FULL_GAME_TOTAL
- Exact consensus line L_t = median(line) across books at snapshot t
- ≥3 books with Over selection at line within 0.01 of L_t
- All have non-null fair_probability

**Signal:**
- pressure_t = mean(Over fair_probability at eligible books) − 0.5
- STRONG iff |pressure_t| ≥ 0.00126
- Direction = sign(pressure_t) (+1 = Over pressure, −1 = Under pressure)

**Prediction:** Consensus total at snapshot t+1 will move in direction of pressure.
- If pressure_t > 0: predict L_{t+1} > L_t
- If pressure_t < 0: predict L_{t+1} < L_t

**Transitions:** (early→mid) and (mid→late) as defined in market_structure.py.
- early = Wednesday 12:00 ET snapshot
- mid = Friday 20:00 ET snapshot
- late = Saturday 15:00 ET snapshot

**Primary endpoint:** Directional accuracy = P(sign(L_{t+1} − L_t) = sign(pressure_t) | STRONG, L_{t+1} ≠ L_t)

**Secondary endpoints:**
- Magnitude of subsequent move |L_{t+1} − L_t|
- Time until repricing (for live pilot)
- Quote persistence / opportunity lifetime
- CLV-like: eventual closing consensus vs signal-time consensus

**Execution claim:** NONE. Displayed-market prediction only until taken-price evidence exists.

## Change Log

- v1.0 (2026-09-15): Initial freeze. Threshold 0.00126 from burned 2022-24 median.
  No modifications permitted before prospective confirmation.

## Prohibited

- Do not alter threshold, pressure construction, eligibility, consensus calc,
  transition definition, or primary endpoint based on observations before confirmation.
- Do not optimize on 2022-24 (burned).
- Do not tune on pre-firewall 2026 shadow results (right/wrong).
- The 72% Fri→Sat subgroup is noted, not a separate candidate.
