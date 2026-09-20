import { createHash } from 'node:crypto';
import { verifySettlementRecord } from './settlement-verifier.mjs';

const text = x => typeof x === 'string' && x.trim().length > 0;
const time = x => typeof x === 'string' && /(?:Z|[+-]\d{2}:\d{2})$/.test(x)
  ? Date.parse(x) : NaN;
const implied = x => typeof x === 'number' && Number.isFinite(x) && Math.abs(x) >= 100
  ? (x > 0 ? 100 / (x + 100) : -x / (-x + 100)) : null;
const fail = reason => ({ status: 'excluded', reason });

// Adapter contract, not provider-ID resolution: canonicalGameId must already
// have been resolved independently for each snapshot by the export owner.
function pairProbability(pair, row) {
  if (!Array.isArray(pair) || pair.length !== 2) return null;
  const [a, b] = pair;
  if (!a || !b) return null;
  const keys = ['canonicalGameId', 'market', 'rulesRef', 'bookKey', 'snapshotRef', 'observedAt'];
  if (!keys.every(k => text(a[k]) && a[k] === b[k])) return null;
  if (!['canonicalGameId', 'market', 'rulesRef'].every(k => a[k] === row[k])) return null;
  if (a.selection !== row.selection || a.line !== row.line) return null;
  const other = row.market === 'FULL_GAME_TOTAL'
    ? (row.selection === 'Over' ? 'Under' : 'Over')
    : (row.selection === row.homeName ? row.awayName : row.homeName);
  const oppositeLine = row.market === 'FULL_GAME_SPREAD' ? -row.line : row.line;
  if (b.selection !== other || b.line !== oppositeLine) return null;
  const p = implied(a.americanOdds), q = implied(b.americanOdds);
  return p === null || q === null ? null : p / (p + q);
}

function measure(row, duplicates) {
  if (!row || !['positionId', 'contractId', 'canonicalGameId', 'rulesRef', 'settlementSourceRef'].every(k => text(row[k]))) {
    return fail('missing_identity_or_provenance');
  }
  if (duplicates.has(row.positionId)) return fail('duplicate_position_id');
  const settlement = verifySettlementRecord(row);
  if (settlement.status !== 'verified') return fail('settlement_' + (settlement.reason || settlement.status));
  if (row.result === 'void') return fail('void');
  // Integer lines and moneyline ties need an independently frozen push-mass
  // convention. Do not present two-way normalization as unconditional EV.
  if (row.result === 'push') return fail('push');
  if (row.market === 'FULL_GAME_MONEYLINE' || Number.isInteger(row.line)) return fail('push_mass_not_supported');
  const entry = pairProbability(row.entryPair, row);
  const close = pairProbability(row.closePair, row);
  if (entry === null) return fail('invalid_entry_pair');
  if (close === null) return fail('missing_or_invalid_exact_close_pair');
  if (!text(row.closeEvidence?.sourceRef) || row.closeEvidence?.snapshotRef !== row.closePair[0].snapshotRef ||
      row.closeEvidence?.kind !== 'frozen_designated_close') return fail('missing_close_designation');
  const e = time(row.entryPair[0].observedAt), c = time(row.closePair[0].observedAt);
  const d = time(row.decisionAt), k = time(row.kickoffAt), s = time(row.settledAt);
  if (![e, c, d, k, s].every(Number.isFinite) || !(e <= d && d < c && c < k && k <= s)) return fail('invalid_chronology');
  return { status: 'measured', entryFairProbability: entry, closeFairProbability: close,
    clvProbabilityDelta: close - entry, clvPercentagePoints: 100 * (close - entry) };
}

/** Zero I/O. Hash raw UTF-8 export bytes before parsing. Trust comes from an
 * independently retained manifest; a supplied hash is not source authentication.
 * Reports descriptive price movement only, never ROI, EV or a promotion decision.
 */
export function measureFrozenSettledClv(raw, manifest) {
  if (typeof raw !== 'string' || manifest?.schema !== 'frozen-settled-clv-v1' ||
      !text(manifest.sourceRef) || !/^[a-f0-9]{64}$/.test(manifest.sha256) ||
      !Number.isFinite(time(manifest.frozenAt)) || !Number.isSafeInteger(manifest.rowCount) || manifest.rowCount < 0) {
    throw new Error('invalid_manifest');
  }
  const sha256 = createHash('sha256').update(raw, 'utf8').digest('hex');
  if (sha256 !== manifest.sha256) throw new Error('fingerprint_mismatch');
  const rows = JSON.parse(raw);
  if (!Array.isArray(rows) || rows.length !== manifest.rowCount) throw new Error('row_count_mismatch');
  const counts = new Map();
  for (const row of rows) counts.set(row?.positionId, (counts.get(row?.positionId) || 0) + 1);
  const duplicates = new Set([...counts].filter(([, n]) => n > 1).map(([id]) => id));
  const details = Array.from(rows, (row, index) => {
    const result = measure(row, duplicates);
    if (result.status === 'measured' && time(row.settledAt) > time(manifest.frozenAt)) {
      return { index, positionId: row.positionId, ...fail('settled_after_freeze') };
    }
    return { index, positionId: row?.positionId ?? null, ...result };
  });
  return { mode: 'SHADOW', measurementOnly: true, productionEligible: false,
    schema: manifest.schema, sourceRef: manifest.sourceRef, sha256, frozenAt: manifest.frozenAt,
    total: details.length, measured: details.filter(r => r.status === 'measured').length,
    excluded: details.filter(r => r.status === 'excluded').length, details };
}
