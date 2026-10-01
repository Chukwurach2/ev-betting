import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';
import {percentile, evaluateVerdict, CAPTURE_RATE_MIN, CLOSE_VALID_MIN} from '../lib/report-health.mjs';

const CLEAN = {
  planned_checkpoints: 100,
  captured_checkpoints: 95,
  duplicate_quote_groups: 0,
  duplicate_pick_groups: 0,
  settlement_anomalies: 0,
  settled_picks: 20,
  picks_with_valid_close: 19,
};

test('thresholds are locked at 90%', () => {
  assert.equal(CAPTURE_RATE_MIN, 0.9);
  assert.equal(CLOSE_VALID_MIN, 0.9);
});

test('clean week is HEALTHY', () => {
  const {verdict, reasons} = evaluateVerdict(CLEAN);
  assert.equal(verdict, 'HEALTHY');
  assert.deepEqual(reasons, []);
  const result = evaluateVerdict(CLEAN);
  assert.equal(result.promotion_eligible, null);
  assert.equal(result.performance_status, 'eligible_for_gate_evaluation');
});

test('any duplicate quote group degrades', () => {
  const {verdict, reasons} = evaluateVerdict({...CLEAN, duplicate_quote_groups: 1});
  assert.equal(verdict, 'DEGRADED');
  assert.ok(reasons.some((r) => r.includes('duplicate quote')));
  const result = evaluateVerdict({...CLEAN, duplicate_quote_groups: 1});
  assert.equal(result.promotion_eligible, false);
  assert.equal(result.performance_status, 'diagnostic_only');
});

test('any duplicate pick group degrades', () => {
  const {verdict} = evaluateVerdict({...CLEAN, duplicate_pick_groups: 2});
  assert.equal(verdict, 'DEGRADED');
});

test('any settlement anomaly degrades', () => {
  const {verdict} = evaluateVerdict({...CLEAN, settlement_anomalies: 1});
  assert.equal(verdict, 'DEGRADED');
});

test('capture rate below 90% degrades', () => {
  const {verdict, reasons} = evaluateVerdict({...CLEAN, captured_checkpoints: 89});
  assert.equal(verdict, 'DEGRADED');
  assert.ok(reasons.some((r) => r.includes('capture rate')));
});

test('capture rate at exactly 90% is healthy', () => {
  const {verdict} = evaluateVerdict({...CLEAN, captured_checkpoints: 90});
  assert.equal(verdict, 'HEALTHY');
});

test('more than 10% missing closes degrades', () => {
  // 17/20 valid = 15% missing -> DEGRADED
  const {verdict} = evaluateVerdict({...CLEAN, picks_with_valid_close: 17});
  assert.equal(verdict, 'DEGRADED');
});

test('exactly 10% missing closes is healthy', () => {
  const {verdict} = evaluateVerdict({...CLEAN, picks_with_valid_close: 18});
  assert.equal(verdict, 'HEALTHY');
});

test('empty week (nothing planned, nothing settled) is healthy, not vacuously degraded', () => {
  const {verdict} = evaluateVerdict({
    planned_checkpoints: 0, captured_checkpoints: 0,
    duplicate_quote_groups: 0, duplicate_pick_groups: 0, settlement_anomalies: 0,
    settled_picks: 0, picks_with_valid_close: 0,
  });
  assert.equal(verdict, 'HEALTHY');
});

test('multiple violations all reported', () => {
  const {verdict, reasons} = evaluateVerdict({
    ...CLEAN, duplicate_quote_groups: 1, captured_checkpoints: 50,
  });
  assert.equal(verdict, 'DEGRADED');
  assert.equal(reasons.length, 2);
});

test('percentile: median/p95 and empty', () => {
  assert.equal(percentile([3, 1, 2], 50), 2);
  assert.equal(percentile([1, 2, 3, 4], 95), 4);
  assert.equal(percentile([], 50), null);
  assert.equal(percentile(['x', null], 50), null);
});

test('all shadow reporting and display feeds are pinned to the repaired engine', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  for (const rel of ['../api/picks.js', '../api/performance.js', '../api/report-health.js']) {
    const source = readFileSync(join(here, rel), 'utf8');
    assert.match(source, /v1\.3-consensus-lobo-3pp-4pct-15m/);
    assert.match(source, /engine_version = \$1/);
    assert.match(source, /\[ENGINE_VERSION\]/);
  }
});

test('public feeds exclude model sizing and performance is fixed-unit', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const picksSource = readFileSync(join(here, '../api/picks.js'), 'utf8');
  assert.doesNotMatch(picksSource, /kelly_fraction/);
  assert.doesNotMatch(picksSource, /\bstake_units\b/);
  assert.match(picksSource, /AS probability_edge/);
  assert.match(picksSource, /AS expected_value/);

  const performanceSource = readFileSync(join(here, '../api/performance.js'), 'utf8');
  assert.doesNotMatch(performanceSource, /kelly_fraction/);
  assert.match(performanceSource, /1\.0::numeric AS stake_units/);
  assert.match(performanceSource, /AS probability_edge/);
  assert.match(performanceSource, /AS expected_value/);
});

test('Sunday Board exposes and requires both locked gates', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const source = readFileSync(join(here, '../api/sunday-board.js'), 'utf8');
  assert.match(source, /MIN_PROBABILITY_EDGE = 0\.03/);
  assert.match(source, /MIN_EXPECTED_VALUE = 0\.04/);
  assert.match(source, /clearsProbability && clearsEv/);
});

test('late checkpoint captures are diagnostic-only in weekly health', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const source = readFileSync(join(here, '../api/report-health.js'), 'utf8');
  assert.match(source, /AS late_captured/);
  assert.ok(source.includes("target_at + interval '15 minutes'"));
  assert.match(source, /later captures are diagnostic only/);
});

test('never-attempted featured checkpoints merge into plan-true coverage', async () => {
  const {mergePlanCoverage} = await import('../api/report-health.js');
  // Fixture mirrors the 2026-09-21..23 GitHub Actions spending-block era:
  // rows exist for windows the collector touched, but elapsed block-era
  // targets (no run fired at all) never materialized rows and must appear
  // as never_attempted instead of vanishing from "planned".
  const rows = [
    {decision_window: 'Close', planned: 15, captured: 0, late_captured: 0, missed: 1, unavailable: 0, failed: 14, running: 0},
    {decision_window: 'T-24', planned: 16, captured: 1, late_captured: 12, missed: 3, unavailable: 0, failed: 0, running: 0},
  ];
  const never = [
    {decision_window: 'Close', never_attempted: 4},
    {decision_window: 'T-90', never_attempted: 2},
  ];
  const {byWindow, totals} = mergePlanCoverage(rows, never);
  const close = byWindow.find((r) => r.decision_window === 'Close');
  assert.equal(close.never_attempted, 4);
  assert.equal(close.planned_true, 19);
  assert.equal(close.capture_rate_planned, 0 / 19);
  assert.equal(close.capture_rate, 0 / 15); // legacy row-based rate unchanged
  const t24 = byWindow.find((r) => r.decision_window === 'T-24');
  assert.equal(t24.never_attempted, 0);
  assert.equal(t24.planned_true, 16);
  assert.equal(totals.neverAttempted, 4); // T-90 rows absent -> not counted; only merges with existing rows
  assert.equal(totals.plannedTrue, 16 + 19);
});

test('mergePlanCoverage keeps verdict inputs row-based', async () => {
  const {mergePlanCoverage} = await import('../api/report-health.js');
  const rows = [{decision_window: 'T-3', planned: 10, captured: 5}];
  const {byWindow, totals} = mergePlanCoverage(rows, [{decision_window: 'T-3', never_attempted: 10}]);
  // The locked HEALTHY/DEGRADED verdict must keep seeing the legacy
  // row-based planned/captured, never the plan-true total.
  assert.equal(totals.planned, 10);
  assert.equal(totals.captured, 5);
  assert.equal(byWindow[0].capture_rate_planned, 5 / 20);
});

test('mergePlanCoverage tolerates null inputs', async () => {
  const {mergePlanCoverage} = await import('../api/report-health.js');
  const {byWindow, totals} = mergePlanCoverage(null, null);
  assert.deepEqual(byWindow, []);
  assert.deepEqual(totals, {planned: 0, captured: 0, neverAttempted: 0, plannedTrue: 0});
});

test('never-attempted query mirrors the frozen featured checkpoint windows', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const source = readFileSync(join(here, '../api/report-health.js'), 'utf8');
  assert.match(source, /Q_NEVER_ATTEMPTED/);
  assert.match(source, /\('T-24', 1440\), \('T-3', 180\), \('T-90', 90\), \('Close', 5\)/);
  assert.match(source, /FROM public\.games/);
  assert.match(source, /nfl_edge_checkpoints/);
  // Verdict inputs stay row-based (locked spec); only honest fields are added.
  assert.match(source, /planned_checkpoints: planned/);
  assert.match(source, /capture_rate_planned/);
});
