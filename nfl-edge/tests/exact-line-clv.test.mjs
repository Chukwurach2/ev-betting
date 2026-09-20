import {test} from 'node:test';
import assert from 'node:assert/strict';
import {americanToImplied, verifyExactLineClose} from '../lib/exact-line-clv.mjs';

const taken = (over = {}) => ({
  market: 'FULL_GAME_TOTAL', selection: 'Over', line: 45.5,
  taken_fair_probability: 0.50, ...over,
});
const q = (over = {}) => ({
  market: 'FULL_GAME_TOTAL', selection: 'Over', line: 45.5,
  american_odds: -120, book_key: 'draftkings',
  observed_at: '2026-09-20T00:00:00Z', ...over,
});

test('normalizes American prices to implied probability', () => {
  assert.equal(americanToImplied(100), 0.5);
  assert.equal(americanToImplied(-100), 0.5);
  assert.ok(Math.abs(americanToImplied(-120) - 120 / 220) < 1e-12);
  assert.equal(americanToImplied(-99), null);
  assert.equal(americanToImplied(0), null);
});

test('positive CLV is close fair probability minus taken fair probability', () => {
  const result = verifyExactLineClose(taken(), [
    q(), q({selection: 'Under', american_odds: 100}),
  ]);
  assert.equal(result.status, 'ok');
  assert.ok(result.clv_prob_points > 0);
  assert.ok(Math.abs(result.closing_fair_probability - (120 / 220) / ((120 / 220) + 0.5)) < 1e-12);
});

test('totals require the exact same line on both sides', () => {
  const result = verifyExactLineClose(taken(), [
    q(), q({selection: 'Under', line: 46.0, american_odds: 100}),
  ]);
  assert.equal(result.status, 'missing_exact_line_close');
});

test('spreads require opposite signed lines', () => {
  const result = verifyExactLineClose(taken({
    market: 'FULL_GAME_SPREAD', selection: 'BUF', line: -3.5,
  }), [
    q({market: 'FULL_GAME_SPREAD', selection: 'BUF', line: -3.5}),
    q({market: 'FULL_GAME_SPREAD', selection: 'MIA', line: 3.5, american_odds: 100}),
  ]);
  assert.equal(result.status, 'ok');
});

test('spread direction mismatch fails closed', () => {
  const result = verifyExactLineClose(taken({
    market: 'FULL_GAME_SPREAD', selection: 'BUF', line: -3.5,
  }), [
    q({market: 'FULL_GAME_SPREAD', selection: 'BUF', line: -3.5}),
    q({market: 'FULL_GAME_SPREAD', selection: 'MIA', line: 2.5, american_odds: 100}),
  ]);
  assert.equal(result.status, 'missing_exact_line_close');
});

test('same book and timestamp are mandatory for de-vig pairing', () => {
  const result = verifyExactLineClose(taken(), [
    q(), q({selection: 'Under', american_odds: 100, book_key: 'fanduel'}),
  ]);
  assert.equal(result.status, 'missing_exact_line_close');
});

test('missing closes and invalid taken probabilities remain explicit', () => {
  assert.equal(verifyExactLineClose(taken(), []).status, 'missing_exact_line_close');
  assert.equal(verifyExactLineClose(taken({taken_fair_probability: null}), []).status,
    'invalid_taken_probability');
});

test('multiple complete books are ambiguous unless a book is selected', () => {
  const quotes = [
    q(), q({selection: 'Under', american_odds: 100}),
    q({book_key: 'fanduel'}),
    q({book_key: 'fanduel', selection: 'Under', american_odds: 100}),
  ];
  assert.equal(verifyExactLineClose(taken(), quotes).status, 'ambiguous_exact_line_close');
  assert.equal(verifyExactLineClose(taken(), quotes, {bookKey: 'fanduel'}).status, 'ok');
});

test('push settlement does not alter price-based CLV', () => {
  const base = taken();
  const quotes = [q(), q({selection: 'Under', american_odds: 100})];
  const a = verifyExactLineClose(base, quotes);
  const b = verifyExactLineClose({...base, result: 'push'}, quotes);
  assert.equal(a.clv_prob_points, b.clv_prob_points);
});
