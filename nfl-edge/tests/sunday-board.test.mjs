import test from 'node:test';
import assert from 'node:assert/strict';

import {buildEngineVerdict} from '../lib/sunday-board-verdict.js';

test('missing quotes are Pending, never no-edge', () => {
  const verdict = buildEngineVerdict({enginePicks: 0, gamesWithQuotes: 0, slateGames: 12});
  assert.match(verdict, /^Pending/);
  assert.doesNotMatch(verdict, /No qualifying edges|no bet/i);
});

test('partial quote coverage remains Pending', () => {
  const verdict = buildEngineVerdict({enginePicks: 0, gamesWithQuotes: 2, slateGames: 12});
  assert.match(verdict, /^Pending/);
  assert.match(verdict, /2\/12/);
});

test('complete coverage may report zero recorded qualifying picks', () => {
  const verdict = buildEngineVerdict({enginePicks: 0, gamesWithQuotes: 12, slateGames: 12});
  assert.equal(verdict, 'No qualifying shadow picks are recorded under the current gates');
});

test('actual engine picks take precedence', () => {
  const verdict = buildEngineVerdict({enginePicks: 2, gamesWithQuotes: 0, slateGames: 12});
  assert.equal(verdict, '2 qualifying shadow edge(s) under review');
});
