// Pure merge for featured checkpoint coverage: row-based checkpoint
// aggregates + never-attempted rows -> rows carrying both the legacy
// row-based capture_rate and the honest plan-based capture_rate_planned.
// Dependency-free so CI tests can import it without node_modules (the
// validation workflow runs `npm test` on the bare nfl-edge/ subtree).
// The locked HEALTHY/DEGRADED verdict (lib/report-health.mjs) keeps
// consuming the legacy row-based planned/captured; this only ADDS the
// honest fields.
export function mergePlanCoverage(checkpointRows, neverAttemptedRows) {
  const neverMap = new Map(
    (neverAttemptedRows || []).map((r) => [
      r.decision_window,
      Number(r.never_attempted) || 0,
    ]),
  );
  const byWindow = (checkpointRows || []).map((r) => {
    const neverAttempted = neverMap.get(r.decision_window) || 0;
    const plannedTrue = Number(r.planned) + neverAttempted;
    return {
      ...r,
      never_attempted: neverAttempted,
      planned_true: plannedTrue,
      capture_rate_planned: plannedTrue > 0 ? Number(r.captured) / plannedTrue : null,
      capture_rate: Number(r.planned) > 0 ? Number(r.captured) / Number(r.planned) : null,
    };
  });
  const totals = byWindow.reduce(
    (a, r) => ({
      planned: a.planned + Number(r.planned),
      captured: a.captured + Number(r.captured),
      neverAttempted: a.neverAttempted + r.never_attempted,
      plannedTrue: a.plannedTrue + r.planned_true,
    }),
    {planned: 0, captured: 0, neverAttempted: 0, plannedTrue: 0},
  );
  return {byWindow, totals};
}
