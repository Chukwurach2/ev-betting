import {Client} from 'pg';

// Pipeline-health companion to /api/performance for the weekly forward-shadow
// report. Read-only aggregates over the last 7 days; no row-level data.
// Exists so a "great" CLV week cannot hide a partial-data week: every weekly
// report must lead with the HEALTH verdict computed here.
//
// Metric definitions and the HEALTHY/DEGRADED rule are locked in
// docs/weekly-forward-shadow-report-spec.md — keep them in sync.

import {percentile, evaluateVerdict} from '../lib/report-health.mjs';

const Q_CHECKPOINTS = `
SELECT decision_window,
       count(*)::int AS planned,
       count(*) FILTER (WHERE status = 'captured')::int AS captured,
       count(*) FILTER (WHERE status = 'missed')::int AS missed,
       count(*) FILTER (WHERE status = 'unavailable')::int AS unavailable,
       count(*) FILTER (WHERE status IN ('failed','abandoned'))::int AS failed,
       count(*) FILTER (WHERE status = 'running')::int AS running
FROM public.nfl_edge_checkpoints
WHERE target_at >= now() - interval '7 days'
GROUP BY decision_window
ORDER BY decision_window`;

const Q_MAT_LAG = `
SELECT EXTRACT(EPOCH FROM (min(q.collected_at) - COALESCE(c.ended_at, c.started_at))) AS lag_s
FROM public.nfl_edge_checkpoints c
JOIN public.nfl_edge_odds_quotes q ON q.checkpoint_key = c.checkpoint_key
WHERE c.status = 'captured' AND c.target_at >= now() - interval '7 days'
GROUP BY c.checkpoint_key`;

const Q_CLOSES = `
SELECT count(*)::int AS settled,
       count(*) FILTER (WHERE clv_prob_points IS NOT NULL)::int AS with_close
FROM public.nfl_edge_picks
WHERE mode = 'shadow' AND result IN ('win','loss','push')
  AND settled_at >= now() - interval '7 days'`;

const Q_QUOTE_AGE = `
SELECT EXTRACT(EPOCH FROM (created_at - observed_at)) AS age_s
FROM public.nfl_edge_picks
WHERE mode = 'shadow' AND created_at >= now() - interval '7 days'`;

const Q_PICKS_BREAKDOWN = `
SELECT p.market, p.book_key, c.decision_window AS window, count(*)::int AS n
FROM public.nfl_edge_picks p
LEFT JOIN public.nfl_edge_checkpoints c ON c.checkpoint_key = p.checkpoint_key
WHERE p.mode = 'shadow' AND p.created_at >= now() - interval '7 days'
GROUP BY p.market, p.book_key, c.decision_window
ORDER BY n DESC`;

const Q_DUP_QUOTES = `
SELECT count(*)::int AS n FROM (
  SELECT 1 FROM public.nfl_edge_odds_quotes
  WHERE collected_at >= now() - interval '7 days'
  GROUP BY checkpoint_key, provider_event_id, market, selection, book_key, line, observed_at
  HAVING count(*) > 1
) d`;

const Q_DUP_PICKS = `
SELECT count(*)::int AS n FROM (
  SELECT 1 FROM public.nfl_edge_picks
  WHERE mode = 'shadow' AND created_at >= now() - interval '7 days'
  GROUP BY provider_event_id, market, selection, line, book_key, observed_at
  HAVING count(*) > 1
) d`;

const Q_SETTLE_ANOMALIES = `
SELECT count(*)::int AS n FROM public.nfl_edge_picks
WHERE mode = 'shadow'
  AND ((result IS NOT NULL AND settled_at IS NULL)
       OR (settled_at IS NOT NULL AND settled_at < created_at))`;

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'private, no-store');
  if (req.method !== 'GET') return res.status(405).json({error: 'Method not allowed'});
  if (!process.env.NFL_EDGE_DATABASE_URL) {
    return res.status(503).json({error: 'Database is not configured'});
  }
  const client = new Client({
    connectionString: process.env.NFL_EDGE_DATABASE_URL,
    ssl: {rejectUnauthorized: true},
    statement_timeout: 15000,
  });
  try {
    await client.connect();
    const cp = (await client.query(Q_CHECKPOINTS)).rows;
    const lags = (await client.query(Q_MAT_LAG)).rows.map((r) => Number(r.lag_s));
    const closes = (await client.query(Q_CLOSES)).rows[0] || {settled: 0, with_close: 0};
    const ages = (await client.query(Q_QUOTE_AGE)).rows.map((r) => Number(r.age_s));
    const breakdown = (await client.query(Q_PICKS_BREAKDOWN)).rows;
    const dupQuotes = Number((await client.query(Q_DUP_QUOTES)).rows[0]?.n || 0);
    const dupPicks = Number((await client.query(Q_DUP_PICKS)).rows[0]?.n || 0);
    const anomalies = Number((await client.query(Q_SETTLE_ANOMALIES)).rows[0]?.n || 0);

    const planned = cp.reduce((a, r) => a + r.planned, 0);
    const captured = cp.reduce((a, r) => a + r.captured, 0);
    const {verdict, reasons} = evaluateVerdict({
      planned_checkpoints: planned,
      captured_checkpoints: captured,
      duplicate_quote_groups: dupQuotes,
      duplicate_pick_groups: dupPicks,
      settlement_anomalies: anomalies,
      settled_picks: closes.settled,
      picks_with_valid_close: closes.with_close,
    });

    return res.status(200).json({
      mode: 'shadow',
      window_days: 7,
      generated_at: new Date().toISOString(),
      disclaimer: 'Shadow research output. Not a wager recommendation.',
      verdict,
      verdict_reasons: reasons,
      checkpoints: {
        by_window: cp.map((r) => ({...r, capture_rate: r.planned > 0 ? r.captured / r.planned : null})),
        overall: {
          planned,
          captured,
          capture_rate: planned > 0 ? captured / planned : null,
        },
      },
      materialization_lag_seconds: {
        n: lags.length,
        median: percentile(lags, 50),
        p95: percentile(lags, 95),
      },
      closes: {
        settled: closes.settled,
        with_valid_close: closes.with_close,
        pct_valid: closes.settled > 0 ? closes.with_close / closes.settled : null,
        missed_closes: closes.settled - closes.with_close,
      },
      quote_age_at_pick_seconds: {
        n: ages.length,
        median: percentile(ages, 50),
        p95: percentile(ages, 95),
      },
      picks_by_market_window_book: breakdown,
      idempotency: {
        duplicate_quote_groups: dupQuotes,
        duplicate_pick_groups: dupPicks,
        settlement_anomalies: anomalies,
      },
    });
  } catch (e) {
    return res.status(503).json({error: 'Pipeline-health report unavailable'});
  } finally {
    await client.end().catch(() => {});
  }
}
