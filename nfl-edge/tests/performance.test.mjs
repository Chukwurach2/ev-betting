import {test} from 'node:test';
import assert from 'node:assert/strict';
import {profitOf, summarize} from '../lib/summarize.mjs';

const W = (over = {}) => ({result: 'win', stake_units: 1, decimal_odds: 2.0,
  consensus_fair_prob: 0.55, edge: 0.1, clv_prob_points: 0.02,
  settled_at: '2026-09-14T00:00:00Z', market: 'FULL_GAME_SPREAD', ...over});
const L = (over = {}) => W({result: 'loss', ...over});
const P = (over = {}) => W({result: 'push', ...over});

test('profit: win/loss/push', () => {
  assert.equal(profitOf(W()), 1.0);
  assert.equal(profitOf(L()), -1.0);
  assert.equal(profitOf(P()), 0);
  assert.ok(Math.abs(profitOf(W({stake_units: 0.5, decimal_odds: 1.91})) - 0.455) < 1e-9);
});

test('empty history summarizes to zeros and nulls', () => {
  const s = summarize([]);
  assert.equal(s.settled, 0);
  assert.equal(s.units_pl, 0);
  assert.equal(s.roi, null);
  assert.equal(s.avg_clv_pp, null);
  assert.deepEqual(s.calibration.map((c) => c.n), [0, 0, 0, 0]);
});

test('record, roi, clv and drawdown', () => {
  const s = summarize([W(), W(), L(), P({clv_prob_points: -0.01})]);
  assert.equal(s.settled, 4);
  assert.equal(s.wins, 2);
  assert.equal(s.pushes, 1);
  assert.equal(s.win_rate, 2 / 3);
  assert.equal(s.units_staked, 4);
  assert.equal(s.units_pl, 1); // +1+1-1+0
  assert.equal(s.roi, 0.25);
  assert.ok(Math.abs(s.avg_clv_pp - 0.0125) < 1e-9);
  assert.equal(s.positive_clv_rate, 0.75);
  assert.equal(s.max_drawdown_units, -1); // peaked at +2, fell to +1
});

test('calibration buckets exclude pushes', () => {
  const s = summarize([
    W({consensus_fair_prob: 0.55}),
    L({consensus_fair_prob: 0.58}),
    W({consensus_fair_prob: 0.72}),
    P({consensus_fair_prob: 0.52}),
  ]);
  const b5060 = s.calibration.find((c) => c.bucket === '50–60%');
  assert.equal(b5060.n, 2);
  assert.equal(b5060.actual, 0.5);
  assert.ok(Math.abs(b5060.predicted - 0.565) < 1e-9);
  const b70 = s.calibration.find((c) => c.bucket === '70%+');
  assert.equal(b70.n, 1);
  assert.equal(b70.actual, 1);
});

test('per-market breakdown', () => {
  const s = summarize([W({market: 'FULL_GAME_SPREAD'}), L({market: 'FULL_GAME_TOTAL'})]);
  const by = Object.fromEntries(s.by_market.map((m) => [m.market, m]));
  assert.equal(by.FULL_GAME_SPREAD.units_pl, 1);
  assert.equal(by.FULL_GAME_TOTAL.units_pl, -1);
  assert.equal(by.FULL_GAME_TOTAL.roi, -1);
});

test('per-edge-bucket breakdown', () => {
  const s = summarize([
    W({edge: 0.04}), // 3-5%: +1
    L({edge: 0.045}), // 3-5%: -1
    W({edge: 0.06}), // 5-8%: +1
    W({edge: 0.10}), // 8%+: +1
    W({edge: 0.01}), // below bar: excluded
  ]);
  const by = Object.fromEntries(s.by_edge_bucket.map((b) => [b.bucket, b]));
  assert.equal(by['3–5%'].n, 2);
  assert.equal(by['3–5%'].units_pl, 0);
  assert.equal(by['5–8%'].n, 1);
  assert.equal(by['5–8%'].units_pl, 1);
  assert.equal(by['8%+'].n, 1);
  assert.equal(by['8%+'].units_pl, 1);
  assert.equal(s.by_edge_bucket.reduce((a, b) => a + b.n, 0), 4);
});
