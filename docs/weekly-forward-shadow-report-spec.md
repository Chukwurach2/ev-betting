# Weekly forward-shadow report — specification

Locked 2026-09-12. The cron `nfl-edge-weekly-forward-shadow-report` (Mondays
09:00 America/Los_Angeles) must follow this spec. **The checklist below cannot
be silently dropped**: any edit that removes or weakens a metric or the HEALTH
verdict rule must amend this document with a dated entry explaining why.

## Purpose

Pipeline health must appear **alongside** performance in every report, so a
"great" CLV week cannot hide a partial-data week. The report leads with the
HEALTH verdict; performance numbers are always read in its light.

## Data source

`GET /api/report-health` (read-only, 7-day window ending at report time).
Implementation: `nfl-edge/api/report-health.js`; verdict logic in
`nfl-edge/lib/report-health.mjs` (pure, unit-tested). All metrics are computed
from existing tables — checkpoints, `nfl_edge_odds_quotes`, `nfl_edge_picks` —
never fabricated. Pick-level performance and health metrics are restricted to
current engine `${VERSION}`; rows from other engine versions are never pooled.

## Required metrics (every report, every week)

1. **Checkpoint capture rate** — planned vs captured checkpoints with
   `target_at` in the last 7 days, by decision window
   (`Opener`/`T-24`/`T-3`/`T-90`/`Close`) plus the overall rate.
   `planned` = every checkpoint row whose target fell in the window (it was
   scheduled); `captured` = `status='captured'`. Missed/unavailable/failed are
   shown as separate counts, never folded into "captured".
2. **Materialization lag** — per captured checkpoint, `min(quotes.collected_at)
   − checkpoint.ended_at` (seconds); report median and p95. Measures how long
   after collection quotes became visible to the picks engine.
3. **Picks with valid exact-line closes** — of settled shadow picks
   (`settled_at` in the last 7 days), the percentage whose CLV was computed
   against a closing consensus at the identical line with ≥2 books
   (`clv_prob_points IS NOT NULL`).
4. **Quote age at pick time** — per pick generated in the last 7 days,
   `created_at − observed_at` (seconds); report median and p95.
5. **Missed closes** — count of settled picks with no valid closing price
   (`clv_prob_points IS NULL`).
6. **Pick counts by market / window / book** — generated in the last 7 days.
   Window comes from `picks.checkpoint_key → checkpoints.decision_window`
   (migration 012); a NULL window is shown as `unknown` and investigated.
7. **Duplicate/idempotency checks** — counts, must be zero, flagged loudly
   otherwise:
   - duplicate logical quote groups (same checkpoint/book/market/selection/
     line/observed_at appearing twice),
   - duplicate logical pick groups (same event/market/selection/line/book/
     observed_at with two pick IDs),
   - settlement anomalies (result set but `settled_at` NULL; `settled_at`
     before `created_at`).

## HEALTH verdict

One line per report: **HEALTHY** or **DEGRADED**, computed by
`evaluateVerdict` with these locked thresholds:

- `CAPTURE_RATE_MIN = 0.90` — DEGRADED if checkpoint capture rate < 90%
  (only when at least one checkpoint was planned; an empty week is not
  vacuously degraded).
- `CLOSE_VALID_MIN = 0.90` — DEGRADED if more than 10% of settled picks lack
  a valid exact-line close (only when at least one pick was settled).
- Any nonzero duplicate group or settlement anomaly → DEGRADED.

All violated conditions are listed as `verdict_reasons`. A DEGRADED report
must name the failing metric and the suspected cause (or state that the cause
is under investigation) — never bury it under performance numbers.

A DEGRADED verdict mechanically sets `promotion_eligible=false` and
`performance_status=diagnostic_only`. ROI, CLV, calibration, and gate-progress
figures may still be reported for debugging, but they cannot count toward
promotion evidence. A HEALTHY verdict sets
`performance_status=eligible_for_gate_evaluation` and leaves
`promotion_eligible=null`: data quality is necessary but never sufficient
for promotion.

## Report order

1. Week range + HEALTH verdict line (with reasons if DEGRADED).
2. Pipeline-health metrics 1–7 (compact).
3. Performance (picks generated/settled, CLV mean + CI, calibration,
   gate progress) — read in light of the verdict above.
4. One honest sentence on what the sample size does or does not support.

## Amendments

- 2026-09-12: initial spec (user-directed: health alongside performance).
- 2026-09-12: DEGRADED performance is mechanically diagnostic-only and not
  promotion-eligible.
- 2026-09-12: engine-scoped reporting added; threshold changes start a new
  forward window and prior engine versions cannot be aggregated.
