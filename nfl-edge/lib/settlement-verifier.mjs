const RESULTS = new Set(["win", "loss", "push", "void"]);
const MARKETS = new Set([
  "FULL_GAME_SPREAD",
  "FULL_GAME_TOTAL",
  "FULL_GAME_MONEYLINE",
]);

function invalid(reason) {
  return { status: "unverifiable", expectedResult: null, reason };
}

function validScore(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

/**
 * Independently reconstruct the expected full-game settlement from immutable
 * contract inputs and final scores. This function is pure and fails closed.
 */
export function expectedSettlement(record) {
  if (!record || typeof record !== "object") return invalid("invalid_record");

  const {
    market,
    selection,
    line,
    homeName,
    awayName,
    finalHomeScore,
    finalAwayScore,
  } = record;

  if (!MARKETS.has(market)) return invalid("unsupported_market");
  if (typeof homeName !== "string" || typeof awayName !== "string" ||
      !homeName || !awayName || homeName === awayName) {
    return invalid("invalid_teams");
  }
  if (market === "FULL_GAME_TOTAL" ? !["Over", "Under"].includes(selection)
    : selection !== homeName && selection !== awayName) return invalid("invalid_selection");
  if (market === "FULL_GAME_MONEYLINE") {
    if (line !== null) return invalid("moneyline_requires_null_line");
    if (record.tieRule !== "push") return invalid("unsupported_or_missing_tie_rule");
  } else if (!Number.isFinite(line) || !Number.isSafeInteger(line * 2) ||
    (market === "FULL_GAME_TOTAL" && line < 0)) {
    return invalid("missing_or_invalid_line");
  }
  // Evidence is supplied by a trusted read-only adapter, never inferred from
  // the stored result or a cancellation label. This is not source authentication.
  if (record.voidEvidence != null) {
    const e = record.voidEvidence;
    if (e.decision !== "void" || !["sourceRef", "ruleRef", "contractId"].every(
      key => typeof e[key] === "string" && e[key].trim()) ||
      e.contractId !== record.contractId) return invalid("invalid_void_evidence");
    return {status:"resolved", expectedResult:"void", reason:null};
  }
  if (record.result === "void") return invalid("missing_void_evidence");
  if (record.gameStatus !== "final") return invalid("game_not_final");
  if (!validScore(finalHomeScore) || !validScore(finalAwayScore)) {
    return invalid("invalid_final_score");
  }

  if (market === "FULL_GAME_SPREAD") {
    if (selection !== homeName && selection !== awayName) {
      return invalid("selection_not_in_game");
    }
    if (!Number.isFinite(line)) return invalid("missing_or_invalid_line");
    const selectedMargin = selection === homeName
      ? finalHomeScore - finalAwayScore
      : finalAwayScore - finalHomeScore;
    const adjusted = selectedMargin + line;
    return {
      status: "resolved",
      expectedResult: adjusted === 0 ? "push" : adjusted > 0 ? "win" : "loss",
      reason: null,
    };
  }

  if (market === "FULL_GAME_TOTAL") {
    if (selection !== "Over" && selection !== "Under") {
      return invalid("invalid_total_selection");
    }
    if (!Number.isFinite(line)) return invalid("missing_or_invalid_line");
    const difference = finalHomeScore + finalAwayScore - line;
    const expectedResult = difference === 0
      ? "push"
      : selection === "Over"
        ? (difference > 0 ? "win" : "loss")
        : (difference < 0 ? "win" : "loss");
    return { status: "resolved", expectedResult, reason: null };
  }

  if (selection !== homeName && selection !== awayName) {
    return invalid("selection_not_in_game");
  }
  const selectedScore = selection === homeName ? finalHomeScore : finalAwayScore;
  const opponentScore = selection === homeName ? finalAwayScore : finalHomeScore;
  return {
    status: "resolved",
    expectedResult: selectedScore === opponentScore
      ? "push"
      : selectedScore > opponentScore ? "win" : "loss",
    reason: null,
  };
}

/** Compare a stored settlement with the independently reconstructed result. */
export function verifySettlementRecord(record) {
  const expected = expectedSettlement(record);
  if (expected.status === "unverifiable") return expected;
  if (!RESULTS.has(record.result)) {
    return { ...expected, status: "unverifiable", reason: "invalid_stored_result" };
  }
  return {
    ...expected,
    status: record.result === expected.expectedResult ? "verified" : "mismatch",
    actualResult: record.result,
    reason: record.result === expected.expectedResult ? null : "result_mismatch",
  };
}

/** Produce an audit summary without mutating or suppressing any discrepancy. */
export function verifySettlementRecords(records) {
  if (!Array.isArray(records)) throw new TypeError("records must be an array");
  const details = Array.from(records, (record, index) => ({
    index,
    ...verifySettlementRecord(record),
  }));
  return {
    total: details.length,
    verified: details.filter((r) => r.status === "verified").length,
    mismatches: details.filter((r) => r.status === "mismatch").length,
    unverifiable: details.filter((r) => r.status === "unverifiable").length,
    discrepancies: details.filter((r) => r.status !== "verified"),
  };
}
