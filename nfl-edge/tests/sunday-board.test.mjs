import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';

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

test('board includes the complete US Sunday slate after UTC midnight', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const source = readFileSync(join(here, '../api/sunday-board.js'), 'utf8');
  assert.match(source, /const DAY_START = '2026-09-13T00:00:00Z'/);
  assert.match(source, /^const DAY_END = '2026-09-14T12:00:00Z'/m);
  assert.doesNotMatch(source, /\\n/);
  assert.doesNotMatch(source, /const DAY_END = '2026-09-14T00:00:00Z'/);
});
