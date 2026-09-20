import test from "node:test";
import assert from "node:assert/strict";

import {
  expectedSettlement,
  verifySettlementRecord,
  verifySettlementRecords,
} from "../lib/settlement-verifier.mjs";

const base = {
  gameStatus: "final",
  tieRule: "push",
  homeName: "Kansas City Chiefs",
  awayName: "Buffalo Bills",
  finalHomeScore: 24,
  finalAwayScore: 20,
};

test("verifies a home favorite cover", () => {
  const out = verifySettlementRecord({
    ...base, market: "FULL_GAME_SPREAD", selection: base.homeName,
    line: -3.5, result: "win",
  });
  assert.equal(out.status, "verified");
});

test("verifies an away underdog cover", () => {
  const out = verifySettlementRecord({
    ...base, market: "FULL_GAME_SPREAD", selection: base.awayName,
    line: 4.5, result: "win",
  });
  assert.equal(out.status, "verified");
});

test("handles an exact spread push", () => {
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_SPREAD", selection: base.homeName, line: -4,
  }).expectedResult, "push");
});

test("handles over, under, and exact-total pushes", () => {
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_TOTAL", selection: "Over", line: 43.5,
  }).expectedResult, "win");
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_TOTAL", selection: "Under", line: 44.5,
  }).expectedResult, "win");
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_TOTAL", selection: "Under", line: 44,
  }).expectedResult, "push");
});

test("verifies two-way moneyline and treats a tie as a push", () => {
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_MONEYLINE", selection: base.homeName, line: null,
  }).expectedResult, "win");
  assert.equal(expectedSettlement({
    ...base, finalAwayScore: 24, market: "FULL_GAME_MONEYLINE",
    selection: base.homeName, line: null,
  }).expectedResult, "push");
});

test("surfaces a stored-result mismatch", () => {
  const out = verifySettlementRecord({
    ...base, market: "FULL_GAME_TOTAL", selection: "Over", line: 43.5,
    result: "loss",
  });
  assert.equal(out.status, "mismatch");
  assert.equal(out.expectedResult, "win");
  assert.equal(out.actualResult, "loss");
});

test("fails closed on unsupported markets", () => {
  const out = expectedSettlement({
    ...base, market: "PLAYER_PASS_YARDS", selection: "Over", line: 250.5,
  });
  assert.deepEqual(out, {
    status: "unverifiable", expectedResult: null, reason: "unsupported_market",
  });
});

test("fails closed on an ambiguous selection", () => {
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_SPREAD", selection: "Mystery Team", line: 3.5,
  }).reason, "invalid_selection");
});

test("fails closed on missing lines and non-integer scores", () => {
  assert.equal(expectedSettlement({
    ...base, market: "FULL_GAME_TOTAL", selection: "Over", line: null,
  }).reason, "missing_or_invalid_line");
  assert.equal(expectedSettlement({
    ...base, finalHomeScore: 24.5, market: "FULL_GAME_MONEYLINE",
    selection: base.homeName, line: null,
  }).reason, "invalid_final_score");
});

test("fails closed on unknown stored result values", () => {
  const out = verifySettlementRecord({
    ...base, market: "FULL_GAME_TOTAL", selection: "Over", line: 43.5,
    result: "void",
  });
  assert.equal(out.status, "unverifiable");
  assert.equal(out.reason, "missing_void_evidence");
});

test("audit summary retains every mismatch and unverifiable row", () => {
  const summary = verifySettlementRecords([
    { ...base, market: "FULL_GAME_SPREAD", selection: base.homeName,
      line: -3.5, result: "win" },
    { ...base, market: "FULL_GAME_TOTAL", selection: "Over",
      line: 43.5, result: "loss" },
    { ...base, market: "Q1_TOTAL", selection: "Over", line: 10.5,
      result: "win" },
  ]);
  assert.deepEqual(
    { total: summary.total, verified: summary.verified,
      mismatches: summary.mismatches, unverifiable: summary.unverifiable },
    { total: 3, verified: 1, mismatches: 1, unverifiable: 1 },
  );
  assert.deepEqual(summary.discrepancies.map((r) => r.index), [1, 2]);
});

test("batch verifier rejects non-array input", () => {
  assert.throws(() => verifySettlementRecords(null), /records must be an array/);
});

test('void requires contract-linked rule and source evidence, not scores', () => {
  const row = {...base, market:'FULL_GAME_TOTAL',selection:'Over',line:44,
    contractId:'fixture-1',result:'void',gameStatus:'cancelled',finalHomeScore:null,
    voidEvidence:{decision:'void',contractId:'fixture-1',sourceRef:'fixture-source',ruleRef:'fixture-rule'}};
  assert.equal(verifySettlementRecord(row).status, 'verified');
  assert.equal(verifySettlementRecord({...row, result:'push'}).status, 'mismatch');
  for (const field of ['sourceRef','ruleRef','contractId']) {
    assert.equal(verifySettlementRecord({...row,voidEvidence:{...row.voidEvidence,[field]:''}}).status,'unverifiable');
  }
  assert.equal(verifySettlementRecord({...row,contractId:'other'}).status,'unverifiable');
});

test('non-final status never implies push or void', () => {
  for (const gameStatus of [undefined,'scheduled','live','postponed','cancelled']) {
    const r = expectedSettlement({...base,gameStatus,market:'FULL_GAME_TOTAL',selection:'Over',line:44});
    assert.equal(r.reason,'game_not_final');
  }
});

test('moneyline requires explicit two-way tie rule and null line', () => {
  const row = {...base,market:'FULL_GAME_MONEYLINE',selection:base.homeName,line:null};
  for (const tieRule of [undefined,'loss','three_way']) {
    assert.equal(expectedSettlement({...row,tieRule}).status,'unverifiable');
  }
  assert.equal(expectedSettlement({...row,line:0}).reason,'moneyline_requires_null_line');
});

test('zero lines push exactly; quarter lines and invalid numbers fail closed', () => {
  for (const selection of [base.homeName,base.awayName]) {
    assert.equal(expectedSettlement({...base,finalAwayScore:24,market:'FULL_GAME_SPREAD',selection,line:0}).expectedResult,'push');
  }
  for (const line of [NaN,Infinity,44.25,'44',null]) {
    assert.equal(expectedSettlement({...base,market:'FULL_GAME_TOTAL',selection:'Over',line}).status,'unverifiable');
  }
});

test('sparse and malformed batch rows remain explicit', () => {
  assert.equal(verifySettlementRecords(new Array(2)).unverifiable,2);
  for (const row of [null,undefined,[],{},false]) assert.equal(verifySettlementRecord(row).status,'unverifiable');
});
