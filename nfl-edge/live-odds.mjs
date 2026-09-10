export const NY_BOOKS = Object.freeze({
  draftkings: 'DraftKings Sportsbook',
  fanduel: 'FanDuel Sportsbook'
});

export const UNSUPPORTED_FULL_GAME_MARKETS = Object.freeze({
  h2h: 'unsupported_market: NFL tie/push settlement has not been certified by sportsbook'
});

function validAmerican(value) {
  return Number.isInteger(value) && (value <= -100 || value >= 100);
}

function implied(value) {
  return value > 0 ? 100 / (100 + value) : -value / (100 - value);
}

function freshTimestamp(value, now) {
  const observed = Date.parse(value);
  return Number.isFinite(observed) && observed <= now && now - observed <= 900000;
}

function settlement(market, line) {
  const integer = Number.isInteger(line);
  if (market === 'FULL_GAME_TOTAL') {
    return integer
      ? 'Regulation plus overtime; push when final combined score equals line; book rules govern voids'
      : 'Regulation plus overtime; no push at half-point line; book rules govern voids';
  }
  return integer
    ? 'Regulation plus overtime; push when adjusted margin equals zero; book rules govern voids'
    : 'Regulation plus overtime; no push at half-point spread; book rules govern voids';
}

function selections({event, book, market, line, sides, observedAt}) {
  const probabilities = sides.map(side => implied(side.price));
  const denominator = probabilities.reduce((a, b) => a + b, 0);
  return sides.map((side, index) => ({
    event_id: event.id,
    home_team: event.home_team,
    away_team: event.away_team,
    kickoff: event.commence_time,
    sportsbook: NY_BOOKS[book.key],
    book_key: book.key,
    market,
    selection: side.name,
    line: side.point,
    american_odds: side.price,
    fair_probability: probabilities[index] / denominator,
    fair_method: 'paired_same_book_conditional_on_no_push',
    observed_at: observedAt,
    settlement_rules: settlement(market, line),
    ny_licensed: true
  }));
}

export function pairedFullGameQuotes(events, now = Date.now()) {
  const quotes = [];
  for (const event of Array.isArray(events) ? events : []) {
    const kickoff = Date.parse(event.commence_time);
    if (!event.id || !Number.isFinite(kickoff) || kickoff <= now) continue;
    for (const book of event.bookmakers || []) {
      if (!NY_BOOKS[book.key]) continue;
      for (const offered of book.markets || []) {
        if (!['totals', 'spreads'].includes(offered.key)) continue;
        if (!freshTimestamp(offered.last_update, now)) continue;
        const outcomes = (offered.outcomes || []).filter(o =>
          typeof o.name === 'string' && Number.isFinite(o.point) && validAmerican(o.price));
        if (offered.key === 'totals') {
          const lines = new Map();
          for (const outcome of outcomes) {
            if (!['Over', 'Under'].includes(outcome.name)) continue;
            const pair = lines.get(outcome.point) || {};
            pair[outcome.name] = outcome;
            lines.set(outcome.point, pair);
          }
          for (const [line, pair] of lines) {
            if (!pair.Over || !pair.Under) continue;
            quotes.push(...selections({event, book, market: 'FULL_GAME_TOTAL', line,
              sides: [pair.Over, pair.Under], observedAt: offered.last_update}));
          }
        } else {
          const home = outcomes.find(o => o.name === event.home_team);
          const away = outcomes.find(o => o.name === event.away_team);
          if (!home || !away || Math.abs(home.point + away.point) > 1e-9) continue;
          quotes.push(...selections({event, book, market: 'FULL_GAME_SPREAD', line: Math.abs(home.point),
            sides: [home, away], observedAt: offered.last_update}));
        }
      }
    }
  }
  return quotes;
}
