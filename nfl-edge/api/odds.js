import {timingSafeEqual} from 'node:crypto';
import {pairedFullGameQuotes, UNSUPPORTED_FULL_GAME_MARKETS} from '../live-odds.mjs';

function authorized(req) {
  const secret = process.env.CRON_SECRET;
  const supplied = req.headers.authorization || '';
  if (!secret || !supplied.startsWith('Bearer ')) return false;
  const actual = Buffer.from(supplied.slice(7));
  const expected = Buffer.from(secret);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'private, no-store');
  if (req.method !== 'GET') return res.status(405).json({error: 'Method not allowed'});
  if (!process.env.CRON_SECRET) return res.status(503).json({error: 'Collector authorization is not configured'});
  if (!authorized(req)) return res.status(401).json({error: 'Unauthorized'});
  if (!process.env.THE_ODDS_API_KEY) return res.status(503).json({error: 'Odds provider is not configured'});
  try {
    const source = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds';
    const url = new URL(source);
    url.search = new URLSearchParams({
      apiKey: process.env.THE_ODDS_API_KEY,
      bookmakers: 'draftkings,fanduel',
      markets: 'spreads,totals',
      oddsFormat: 'american',
      dateFormat: 'iso'
    }).toString();
    const response = await fetch(url, {signal: AbortSignal.timeout(12000)});
    if (!response.ok) return res.status(503).json({provider: 'the_odds_api', error: `Provider HTTP ${response.status}`});
    const events = await response.json();
    if (!Array.isArray(events)) throw new Error('Invalid provider payload');
    const checkedAt = new Date().toISOString();
    const quotes = pairedFullGameQuotes(events, Date.parse(checkedAt));
    return res.status(200).json({
      provider: 'the_odds_api',
      checked_at: checkedAt,
      quotes_verified: quotes.length > 0,
      automatic_picks: false,
      supported_markets: ['FULL_GAME_SPREAD', 'FULL_GAME_TOTAL'],
      unsupported_markets: UNSUPPORTED_FULL_GAME_MARKETS,
      request_cost: response.headers.get('x-requests-last'),
      quota_remaining: response.headers.get('x-requests-remaining'),
      source_url: source,
      quotes
    });
  } catch {
    return res.status(503).json({provider: 'the_odds_api', error: 'Provider request failed'});
  }
}
