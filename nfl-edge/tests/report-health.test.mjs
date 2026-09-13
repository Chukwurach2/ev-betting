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
  assert.match(source, /target_at \\+ interval '15 minutes'/);
  assert.match(source, /later captures are diagnostic only/);
});
