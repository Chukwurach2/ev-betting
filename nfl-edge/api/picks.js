import {Client} from 'pg';

const ENGINE_VERSION = 'v1.2-consensus-lobo-4pct-15m';

// Public read-only feed of the latest SHADOW picks from the consensus edge
// engine. These are research outputs, never production wagers: every row is
// mode='shadow' by construction (see ops/picks.py assert_shadow).
// The deployment itself sits behind Vercel access protection.

const SQL = `
SELECT pick_id, engine_version, mode, provider_event_id, home_team, away_team,
       kickoff, market, selection, line, sportsbook, book_key, american_odds,
       decimal_odds, consensus_fair_prob, edge, kelly_fraction, stake_units,
       consensus_books, observed_at, created_at,
       challenger_version, challenger_fair_prob, challenger_pred_margin,
       challenger_pred_total
FROM public.nfl_edge_picks
WHERE mode = 'shadow' AND engine_version = $1 AND kickoff > now()
ORDER BY created_at DESC
LIMIT 100
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
      count: rows.length,
      disclaimer: 'Shadow research output. Not a wager recommendation.',
      picks: rows,
    });
  } catch {
    return res.status(503).json({error: 'Picks feed unavailable'});
  } finally {
    await client.end().catch(() => {});
  }
}
