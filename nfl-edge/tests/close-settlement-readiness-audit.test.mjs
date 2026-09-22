import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const sql = readFileSync(
  new URL('../ops/audit_close_settlement_readiness.sql', import.meta.url),
  'utf8',
);
const executable = sql.replace(/--.*$/gm, '');

test('close/settlement readiness audit is one read-only aggregate query', () => {
  assert.match(executable.trim(), /^WITH\s/i);
  assert.equal((executable.match(/;/g) || []).length, 1);
  assert.doesNotMatch(
    executable,
    /\b(INSERT|UPDATE|DELETE|MERGE|ALTER|CREATE|DROP|TRUNCATE|GRANT|REVOKE|CALL|COPY)\b/i,
  );
  assert.match(executable, /count\(\*\) AS positions/i);
});

test('audit keys close coverage by canonical game receipt, not provider event id', () => {
  assert.match(executable, /cc\.game_id = p\.game_id/i);
  assert.match(executable, /q\.checkpoint_key = cc\.checkpoint_key/i);
  assert.doesNotMatch(executable, /provider_event_id/i);
});

test('exact-line candidate and governance blockers remain explicit', () => {
  assert.match(executable, /q\.line = p\.line/i);
  assert.match(executable, /count\(DISTINCT q\.book_key\)/i);
  assert.match(executable, /candidate_only_not_designated/i);
  assert.match(
    executable,
    /blocked_independent_frozen_export_and_owner_close_mapping/i,
  );
});

test('aggregate output does not expose outcomes, scores, prices, or identifiers', () => {
  const finalSelect = executable.slice(executable.lastIndexOf('SELECT now()'));
  for (const field of [
    'pick_id', 'selection', 'result', 'final_home_score', 'final_away_score',
    'american_odds', 'fair_probability', 'clv_prob_points',
  ]) assert.doesNotMatch(finalSelect, new RegExp('\\b' + field + '\\b', 'i'));
});
