import {Client} from 'pg';

// Build info plus LIVE pipeline health. The pipeline section is coarse by
// design: per-component ok/stale/idle/missing and an overall status, no
// secrets, no row-level data. Heartbeats are written by the scheduled
// workers (see ops/heartbeat.py and ops/health.py for the same thresholds).

const THRESHOLDS_MIN = {
  schedule_sync: 26 * 60,
  collector: 75,
  picks: 75,
  settlement: 30 * 60,
};
const CORE = new Set(['schedule_sync', 'collector', 'picks']);
const CREDITS_WARN_BELOW = 60;

function evaluate(heartbeats, upcomingGames, nowMs) {
  const inSeason = upcomingGames > 0;
  const components = {};
  let status = 'ok';
  for (const [name, limitMin] of Object.entries(THRESHOLDS_MIN)) {
    const hb = heartbeats[name];
    if (!inSeason) {
      components[name] = {status: 'idle'};
      continue;
    }
    if (!hb) {
      components[name] = {status: 'missing'};
    } else {
      const ageMin = (nowMs - new Date(hb.last_ok_at).getTime()) / 60000;
      components[name] = {
        status: ageMin <= limitMin ? 'ok' : 'stale',
        age_minutes: Math.round(ageMin),
      };
    }
    const s = components[name].status;
    if (s === 'stale' || s === 'missing') {
      status = CORE.has(name) ? 'down' : 'degraded';
    }
  }
  const remaining = heartbeats.collector?.detail?.credits_remaining;
  if (inSeason && typeof remaining === 'number' && remaining < CREDITS_WARN_BELOW) {
    components.odds_credits = {status: 'low', remaining};
    if (status === 'ok') status = 'degraded';
  }
  return {status, in_season: inSeason, components};
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'GET') return res.status(405).json({error: 'Method not allowed'});
  const info = {
    version: '2.5.0',
    status: 'limited',
    source_commit: process.env.VERCEL_GIT_COMMIT_SHA || null,
    capabilities: {
      schedule: true,
      device_journal: true,
      cloud_sync: 'requires_signed_in_verification',
      odds_key_configured: Boolean(process.env.THE_ODDS_API_KEY),
      scheduler_secret_configured: Boolean(process.env.CRON_SECRET),
      automatic_picks: false,
    },
    blockers: [
      'Market-level validation has not passed',
      'Prediction publisher and checkpoint scheduler are not running',
      'Authenticated cross-device acceptance remains unverified',
    ],
  };
  const done = (pipeline) =>
    res.status(200).json({...info, pipeline, checked_at: new Date().toISOString()});
  if (!process.env.NFL_EDGE_DATABASE_URL) {
    return done({status: 'unknown', reason: 'database not configured'});
  }
  const client = new Client({
    connectionString: process.env.NFL_EDGE_DATABASE_URL,
    ssl: {rejectUnauthorized: true},
    statement_timeout: 8000,
  });
  try {
    await client.connect();
    let heartbeats = {};
    try {
      const {rows} = await client.query(
        'SELECT component, last_ok_at, detail FROM public.nfl_edge_heartbeats');
      for (const r of rows) heartbeats[r.component] = r;
    } catch {
      heartbeats = {};
    }
    let upcoming = 0;
    try {
      const {rows} = await client.query(
        "SELECT count(*)::int AS n FROM public.games WHERE kickoff BETWEEN now() AND now() + interval '45 days'");
      upcoming = rows[0]?.n || 0;
    } catch {
      upcoming = 0;
    }
    return done(evaluate(heartbeats, upcoming, Date.now()));
  } catch {
    return done({status: 'unknown', reason: 'health query failed'});
  } finally {
    await client.end().catch(() => {});
  }
}
