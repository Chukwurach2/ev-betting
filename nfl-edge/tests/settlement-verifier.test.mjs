import test from "node:test";
import assert from "node:assert/strict";

import {
  expectedSettlement,
  verifySettlementRecord,
  verifySettlementRecords,
} from "../lib/settlement-verifier.mjs";

const base = {
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
  }).reason, "selection_not_in_game");
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
  assert.equal(out.reason, "invalid_stored_result");
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
