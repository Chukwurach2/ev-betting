// Pure, testable pipeline-health verdict for the weekly forward-shadow report.
// No I/O, no secrets. The rule is deliberately strict: a "great" CLV week
// must not be able to hide a partial-data week.
//
// DEGRADED when ANY of:
//   - duplicate quote groups > 0 or duplicate pick groups > 0 or
//     settlement anomalies > 0 (idempotency must be perfect)
//   - checkpoint capture rate < 90% (planned checkpoints with target_at in
//     the window; only when at least one checkpoint was planned)
//   - more than 10% of settled picks lack a valid exact-line close
//     (only when at least one pick was settled)
// Otherwise HEALTHY.

// Locked thresholds (mirrored in docs/weekly-forward-shadow-report-spec.md).
export const CAPTURE_RATE_MIN = 0.9;
export const CLOSE_VALID_MIN = 0.9; // i.e. at most 10% missing closes

// Nearest-rank percentile over a numeric array; null when empty.
export function percentile(values, p) {
  const xs = values
    .filter((v) => v !== null && v !== undefined && v !== '')
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (xs.length === 0) return null;
  const rank = Math.min(xs.length - 1, Math.floor((p / 100) * xs.length));
  return xs[rank];
}

// metrics: {
//   planned_checkpoints, captured_checkpoints,
//   duplicate_quote_groups, duplicate_pick_groups, settlement_anomalies,
//   settled_picks, picks_with_valid_close,
// }
// Returns {verdict: 'HEALTHY'|'DEGRADED', reasons: string[]}.
export function evaluateVerdict(m) {
  const reasons = [];
  const num = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);
  const dupQ = num(m.duplicate_quote_groups);
  const dupP = num(m.duplicate_pick_groups);
  const anom = num(m.settlement_anomalies);
  if (dupQ > 0) reasons.push(`${dupQ} duplicate quote group(s): idempotency violated`);
  if (dupP > 0) reasons.push(`${dupP} duplicate pick group(s): idempotency violated`);
  if (anom > 0) reasons.push(`${anom} settlement anomalie(s): picks settled without settled_at or settled before creation`);
  const planned = num(m.planned_checkpoints);
  if (planned > 0) {
    const rate = num(m.captured_checkpoints) / planned;
    if (rate < CAPTURE_RATE_MIN) {
      reasons.push(`checkpoint capture rate ${(rate * 100).toFixed(1)}% below ${(CAPTURE_RATE_MIN * 100).toFixed(0)}% floor`);
    }
  }
  const settled = num(m.settled_picks);
  if (settled > 0) {
    const validRate = num(m.picks_with_valid_close) / settled;
    if (validRate < CLOSE_VALID_MIN) {
      reasons.push(`${((1 - validRate) * 100).toFixed(1)}% of settled picks missing exact-line closes (>${((1 - CLOSE_VALID_MIN) * 100).toFixed(0)}% allowed)`);
    }
  }
  const verdict = reasons.length === 0 ? 'HEALTHY' : 'DEGRADED';
  return {
    verdict,
    reasons,
    // Health is necessary but never sufficient for promotion. A clean week
    // may be evaluated by the separately locked statistical gate; it does
    // not pass that gate merely by being complete.
    promotion_eligible: verdict === 'DEGRADED' ? false : null,
    performance_status: verdict === 'DEGRADED'
      ? 'diagnostic_only'
      : 'eligible_for_gate_evaluation',
  };
}
