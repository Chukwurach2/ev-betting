import {test} from 'node:test';
import assert from 'node:assert/strict';
import {pairedFullGameQuotes, UNSUPPORTED_FULL_GAME_MARKETS} from '../live-odds.mjs';

const now = Date.parse('2026-09-10T12:00:00Z');
const event = {
  id: 'game-1', home_team: 'Home', away_team: 'Away', commence_time: '2026-09-10T20:00:00Z',
  bookmakers: [{key: 'draftkings', markets: [
    {key: 'totals', last_update: '2026-09-10T11:55:00Z', outcomes: [
      {name: 'Over', point: 44, price: -110}, {name: 'Under', point: 44, price: -110}]},
    {key: 'spreads', last_update: '2026-09-10T11:56:00Z', outcomes: [
      {name: 'Home', point: -3.5, price: -105}, {name: 'Away', point: 3.5, price: -115}]},
    {key: 'h2h', last_update: '2026-09-10T11:56:00Z', outcomes: [
      {name: 'Home', price: -120}, {name: 'Away', price: 105}]}
  ]}]
};

test('pairs exact same-book full-game totals and spreads', () => {
  const quotes = pairedFullGameQuotes([event], now);
  assert.equal(quotes.length, 4);
  assert.deepEqual(new Set(quotes.map(q => q.market)), new Set(['FULL_GAME_TOTAL', 'FULL_GAME_SPREAD']));
  assert.ok(quotes.every(q => q.ny_licensed && q.fair_method === 'paired_same_book_conditional_on_no_push'));
  assert.match(quotes.find(q => q.market === 'FULL_GAME_TOTAL').settlement_rules, /push/);
});

test('rejects stale, non-NY, mismatched and post-kickoff contracts', () => {
  const stale = structuredClone(event); stale.bookmakers[0].markets[0].last_update = '2026-09-10T11:40:00Z';
  const offshore = structuredClone(event); offshore.bookmakers[0].key = 'offshore';
  const mismatch = structuredClone(event); mismatch.bookmakers[0].markets[1].outcomes[1].point = 4.5;
  const started = structuredClone(event); started.commence_time = '2026-09-10T11:59:00Z';
  assert.equal(pairedFullGameQuotes([stale], now).filter(q => q.market === 'FULL_GAME_TOTAL').length, 0);
  assert.equal(pairedFullGameQuotes([offshore], now).length, 0);
  assert.equal(pairedFullGameQuotes([mismatch], now).filter(q => q.market === 'FULL_GAME_SPREAD').length, 0);
  assert.equal(pairedFullGameQuotes([started], now).length, 0);
});

test('moneyline remains unsupported until NFL tie settlement is certified', () => {
  assert.match(UNSUPPORTED_FULL_GAME_MARKETS.h2h, /^unsupported_market:/);
  assert.equal(pairedFullGameQuotes([event], now).some(q => q.market === 'FULL_GAME_MONEYLINE'), false);
});
