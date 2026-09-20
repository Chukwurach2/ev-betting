import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { measureFrozenSettledClv } from '../lib/frozen-settled-clv.mjs';

function fixture() {
  const row = { positionId:'p1', contractId:'c1', canonicalGameId:'game1', rulesRef:'rules1',
    settlementSourceRef:'final1', market:'FULL_GAME_TOTAL', selection:'Over', line:40.5,
    homeName:'Home', awayName:'Away', finalHomeScore:24, finalAwayScore:20,
    gameStatus:'final', result:'win', decisionAt:'2026-01-01T12:00:00Z',
    kickoffAt:'2026-01-01T18:00:00Z', settledAt:'2026-01-01T22:00:00Z' };
  const pair = (snapshotRef, observedAt, odds) => ['Over', 'Under'].map((selection, i) => ({
    canonicalGameId:'game1', market:row.market, rulesRef:'rules1', bookKey:'book1',
    snapshotRef, observedAt, selection, line:40.5, americanOdds:odds[i] }));
  row.entryPair = pair('entry','2026-01-01T11:59:00Z',[-110,-110]);
  row.closePair = pair('close','2026-01-01T17:59:00Z',[-150,130]);
  row.closeEvidence = {kind:'frozen_designated_close',snapshotRef:'close',sourceRef:'close-receipt'};
  return row;
}
function run(rows, change = {}) {
  const raw = JSON.stringify(rows);
  return measureFrozenSettledClv(raw, {schema:'frozen-settled-clv-v1',sourceRef:'synthetic',
    frozenAt:'2026-01-02T00:00:00Z',rowCount:rows.length,
    sha256:createHash('sha256').update(raw).digest('hex'), ...change});
}
test('synthetic exact-line movement has explicit fraction and percentage-point units', () => {
  const row = fixture(), before = JSON.stringify(row), r = run([row]);
  assert.equal(r.measured,1); assert.equal(r.productionEligible,false); assert.equal(r.mode,'SHADOW');
  assert.ok(Math.abs(r.details[0].clvProbabilityDelta - (0.6/(0.6+100/230)-0.5)) < 1e-12);
  assert.equal(r.details[0].clvPercentagePoints,100*r.details[0].clvProbabilityDelta);
  assert.equal(JSON.stringify(row),before);
});
for (const [name, mutate, reason] of [
  ['game mismatch', r => r.closePair[1].canonicalGameId='other', 'missing_or_invalid_exact_close_pair'],
  ['line mismatch', r => r.closePair[1].line=41.5, 'missing_or_invalid_exact_close_pair'],
  ['same side', r => r.closePair[1].selection='Over', 'missing_or_invalid_exact_close_pair'],
  ['book mismatch', r => r.closePair[1].bookKey='other', 'missing_or_invalid_exact_close_pair'],
  ['rules mismatch', r => r.closePair[1].rulesRef='other', 'missing_or_invalid_exact_close_pair'],
  ['snapshot mismatch', r => r.closePair[1].snapshotRef='other', 'missing_or_invalid_exact_close_pair'],
  ['time mismatch', r => r.closePair[1].observedAt='2026-01-01T17:58:00Z', 'missing_or_invalid_exact_close_pair'],
  ['duplicate quotes', r => r.closePair.push({...r.closePair[0]}), 'missing_or_invalid_exact_close_pair'],
  ['missing close', r => r.closePair=[], 'missing_or_invalid_exact_close_pair'],
  ['invalid odds', r => r.closePair[0].americanOdds='-150', 'missing_or_invalid_exact_close_pair'],
  ['unverified close', r => delete r.closeEvidence, 'missing_close_designation'],
  ['postkickoff', r => r.closePair.forEach(q=>q.observedAt=r.kickoffAt), 'invalid_chronology'],
  ['entry after decision', r => r.entryPair.forEach(q=>q.observedAt='2026-01-01T13:00:00Z'), 'invalid_chronology'],
  ['unsettled', r => r.gameStatus='live', 'settlement_game_not_final'],
  ['wrong result', r => r.result='loss', 'settlement_result_mismatch'],
  ['integer line', r => r.line=40, 'push_mass_not_supported'],
  ['push', r => {r.line=44;r.result='push';}, 'push'],
  ['void', r => {r.result='void';r.voidEvidence={decision:'void',contractId:'c1',sourceRef:'void1',ruleRef:'rules1'};}, 'void'],
  ['late settlement', r => r.settledAt='2026-01-03T00:00:00Z', 'settled_after_freeze'],
]) test(name + ' stays explicit and excluded', () => {
  const row=fixture(); mutate(row); const r=run([row]);
  assert.equal(r.measured,0); assert.equal(r.excluded,1); assert.equal(r.details[0].reason,reason);
});
test('duplicates exclude every occurrence; nulls and empty input stay explicit', () => {
  assert.deepEqual(run([fixture(),fixture()]).details.map(r=>r.reason),['duplicate_position_id','duplicate_position_id']);
  assert.equal(run([null]).excluded,1); assert.equal(run([]).total,0);
});
test('fingerprint, schema, count and freeze are required', () => {
  for (const change of [{sha256:'0'.repeat(64)},{schema:'other'},{rowCount:2},{frozenAt:'yesterday'}]) {
    assert.throws(()=>run([fixture()],change));
  }
});
test('spread requires opposite team and negated line', () => {
  const r=fixture(); Object.assign(r,{market:'FULL_GAME_SPREAD',selection:'Home',line:-3.5});
  for (const pair of [r.entryPair,r.closePair]) pair.forEach((q,i)=>Object.assign(q,{
    market:r.market,selection:i?'Away':'Home',line:i?3.5:-3.5}));
  assert.equal(run([r]).measured,1);
  r.closePair[1].line=-3.5; assert.equal(run([r]).excluded,1);
});
test('Under and negative price movement preserve sign', () => {
  const r=fixture(); r.selection='Under'; r.result='loss';
  r.entryPair.reverse(); r.closePair.reverse();
  assert.ok(run([r]).details[0].clvProbabilityDelta < 0);
});
test('zero movement is measured, not treated as missing', () => {
  const r=fixture(); r.closePair.forEach(q=>q.americanOdds=-110);
  assert.equal(run([r]).details[0].clvProbabilityDelta,0);
});
