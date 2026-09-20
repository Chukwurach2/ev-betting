// Pure exact-line closing-line-value verifier.
//
// This module performs no I/O and intentionally fails closed. A closing
// probability is accepted only when it can be reconstructed from a complete
// same-book, same-timestamp, same-contract two-way market. Positive CLV means
// the closing market assigned more fair probability to the taken selection.

const EPS = 1e-9;

export function americanToImplied(odds) {
  const value = Number(odds);
  if (!Number.isFinite(value) || value === 0 || (value > -100 && value < 100)) {
    return null;
  }
  return value > 0 ? 100 / (value + 100) : -value / (-value + 100);
}

function sameLine(a, b) {
  if (a == null || b == null) return a == null && b == null;
  const x = Number(a), y = Number(b);
  return Number.isFinite(x) && Number.isFinite(y) && Math.abs(x - y) <= EPS;
}

function keyPart(value) {
  return value == null ? '' : String(value).trim().toLowerCase();
}

function isOppositeContract(market, selected, other) {
  const kind = keyPart(market);
  if (keyPart(selected.selection) === keyPart(other.selection)) return false;
  if (kind.includes('total')) return sameLine(selected.line, other.line);
  if (kind.includes('spread')) {
    const a = Number(selected.line), b = Number(other.line);
    return Number.isFinite(a) && Number.isFinite(b) && Math.abs(a + b) <= EPS;
  }
  if (kind.includes('moneyline')) return selected.line == null && other.line == null;
  return false;
}

function groupKey(q) {
  return `${keyPart(q.book_key)}|${String(q.observed_at || '')}`;
}

export function verifyExactLineClose(taken, closeQuotes, {bookKey = null} = {}) {
  const takenFair = Number(taken?.taken_fair_probability);
  if (!(takenFair > 0 && takenFair < 1)) {
    return {status: 'invalid_taken_probability'};
  }
  if (!taken?.market || !taken?.selection || !Array.isArray(closeQuotes)) {
    return {status: 'invalid_contract'};
  }

  const targetBook = bookKey == null ? null : keyPart(bookKey);
  const candidates = closeQuotes.filter((q) =>
    keyPart(q.market) === keyPart(taken.market) &&
    keyPart(q.selection) === keyPart(taken.selection) &&
    sameLine(q.line, taken.line) &&
    (targetBook == null || keyPart(q.book_key) === targetBook));

  const complete = [];
  for (const selected of candidates) {
    if (!selected.book_key || !selected.observed_at) continue;
    const peers = closeQuotes.filter((q) =>
      groupKey(q) === groupKey(selected) &&
      keyPart(q.market) === keyPart(taken.market) &&
      isOppositeContract(taken.market, selected, q));
    if (peers.length !== 1) continue;
    const pSelected = americanToImplied(selected.american_odds);
    const pOther = americanToImplied(peers[0].american_odds);
    if (pSelected == null || pOther == null || pSelected + pOther <= 0) continue;
    complete.push({
      book_key: selected.book_key,
      observed_at: selected.observed_at,
      closing_fair_probability: pSelected / (pSelected + pOther),
    });
  }

  if (complete.length === 0) return {status: 'missing_exact_line_close'};
  if (complete.length > 1) return {status: 'ambiguous_exact_line_close', matches: complete.length};

  const close = complete[0];
  return {
    status: 'ok',
    sign_convention: 'closing_fair_probability_minus_taken_fair_probability',
    book_key: close.book_key,
    observed_at: close.observed_at,
    taken_fair_probability: takenFair,
    closing_fair_probability: close.closing_fair_probability,
    clv_prob_points: close.closing_fair_probability - takenFair,
  };
}

