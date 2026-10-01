// Pure merge for prop checkpoint completeness: attempt-based completeness
// rows + never-attempted rows -> rows carrying both the legacy attempt-based
// capture_rate and the honest plan-based capture_rate_planned.
// Dependency-free so CI tests can import it without node_modules (the
// validation workflow runs `npm test` on the bare nfl-edge/ subtree).
export function mergeCompleteness(completenessRows, neverAttemptedRows) {
  const neverMap = new Map(
    (neverAttemptedRows || []).map((r) => [
      r.checkpoint_name,
      Number(r.never_attempted) || 0,
    ]),
  );
  const byCheckpoint = (completenessRows || []).map((r) => {
    const neverAttempted = neverMap.get(r.checkpoint_name) || 0;
    const planned = Number(r.due) + neverAttempted;
    return {
      ...r,
      never_attempted: neverAttempted,
      planned,
      capture_rate_planned: planned > 0 ? Number(r.captured) / planned : null,
      capture_rate: Number(r.due) > 0 ? Number(r.captured) / Number(r.due) : null,
    };
  });
  const totals = byCheckpoint.reduce(
    (a, r) => ({
      due: a.due + Number(r.due),
      captured: a.captured + Number(r.captured),
      planned: a.planned + r.planned,
      neverAttempted: a.neverAttempted + r.never_attempted,
    }),
    {due: 0, captured: 0, planned: 0, neverAttempted: 0},
  );
  return {byCheckpoint, totals};
}
