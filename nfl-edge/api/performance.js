import {Client} from 'pg';

const ENGINE_VERSION = 'v1.3-consensus-lobo-3pp-4pct-15m';

// Read-only track record for the SHADOW consensus engine: settled-pick
// aggregates, CLV, calibration buckets, and drawdown. Research output only;
// every row is mode='shadow' by construction. Performance is fixed-unit;
// stored research sizing is neither exposed nor used. Empty states when nothing is
// settled yet — the engine never invents history.

import {summarize} from '../lib/summarize.mjs';

const SQL = `
SELECT pick_id, market, selection, line, book_key, american_odds, decimal_odds,
       consensus_fair_prob,
       (consensus_fair_prob - taken_fair_prob) AS probability_edge,
       edge AS expected_value, 1.0::numeric AS stake_units,
       observed_at, created_at,
       settled_at, result, clv_prob_points, engine_version
FROM public.nfl_edge_picks
WHERE mode = 'shadow' AND engine_version = $1
  AND result IN ('win', 'loss', 'push')
ORDER BY settled_at ASC
`;

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'private, no-store');
  if (req.method !== 'GET') return res.status(405).json({error: 'Method not allowed'});
  if (!process.env.NFL_EDGE_DATABASE_URL) {
    return res.status(503).json({error: 'Database is not configured'});
  }
  const client = new Client({
    connectionString: process.env.NFL_EDGE_DATABASE_URL,
    ssl: {rejectUnauthorized: true},
    statement_timeout: 8000,
  });
  try {
    await client.connect();
    const {rows} = await client.query(SQL, [ENGINE_VERSION]);
    return res.status(200).json({
      mode: 'shadow',
      engine: ENGINE_VERSION,
      disclaimer: 'Shadow research output. Not a wager recommendation.',
      // Raw settled rows power the weekly forward-shadow report (7-day
      // windows, CLV confidence intervals, gate progress). Shadow research
      // data only; no PII.
      picks: rows,
      ...summarize(rows),
    });
  } catch {
    return res.status(503).json({error: 'Track record unavailable'});
  } finally {
    await client.end().catch(() => {});
  }
}
