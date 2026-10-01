import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mergeCompleteness} from '../api/prop-collection-report.js';

// Fixture mirrors the real 2026-09-15..2026-09-22 weekly payload: every
// attempted checkpoint captured, but NY Giants @ LA Rams (MNF) T-12/T-6/T-3/
// T-90m/Close were never attempted (dispatches skipped under the Actions
// spending block), so they must appear as never_attempted, not vanish.
const COMPLETENESS = [
  {checkpoint_name: 'Close', due: 15, captured: 15, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
  {checkpoint_name: 'T-12h', due: 15, captured: 15, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
  {checkpoint_name: 'T-24h', due: 16, captured: 16, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
  {checkpoint_name: 'T-3h', due: 15, captured: 15, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
  {checkpoint_name: 'T-6h', due: 15, captured: 15, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
  {checkpoint_name: 'T-90m', due: 15, captured: 15, empty: 0, no_event_id: 0, ambiguous_match: 0, missed: 0},
];
const NEVER_ATTEMPTED = [
  {checkpoint_name: 'Close', never_attempted: 1},
  {checkpoint_name: 'T-12h', never_attempted: 1},
  {checkpoint_name: 'T-3h', never_attempted: 1},
  {checkpoint_name: 'T-6h', never_attempted: 1},
  {checkpoint_name: 'T-90m', never_attempted: 1},
];

test('never-attempted checkpoints merge into planned totals', () => {
  const {byCheckpoint, totals} = mergeCompleteness(COMPLETENESS, NEVER_ATTEMPTED);
  const close = byCheckpoint.find((r) => r.checkpoint_name === 'Close');
  assert.equal(close.never_attempted, 1);
  assert.equal(close.planned, 16);
  assert.equal(close.capture_rate_planned, 15 / 16);
  // legacy attempt-based field is preserved
  assert.equal(close.capture_rate, 1);
  const t24 = byCheckpoint.find((r) => r.checkpoint_name === 'T-24h');
  assert.equal(t24.never_attempted, 0);
  assert.equal(t24.planned, 16);
  assert.equal(t24.capture_rate_planned, 1);
});

test('overall totals carry both rates', () => {
  const {totals} = mergeCompleteness(COMPLETENESS, NEVER_ATTEMPTED);
  assert.equal(totals.due, 91);
  assert.equal(totals.captured, 91);
  assert.equal(totals.neverAttempted, 5);
  assert.equal(totals.planned, 96);
  assert.equal(totals.captured / totals.planned, 91 / 96);
});

test('empty never-attempted list keeps legacy behavior', () => {
  const {byCheckpoint, totals} = mergeCompleteness(COMPLETENESS, []);
  assert.ok(byCheckpoint.every((r) => r.never_attempted === 0));
  assert.equal(totals.planned, totals.due);
  assert.equal(totals.planned, 91);
});

test('null inputs do not throw', () => {
  const {byCheckpoint, totals} = mergeCompleteness(null, null);
  assert.deepEqual(byCheckpoint, []);
  assert.equal(totals.planned, 0);
  assert.equal(totals.due, 0);
});
