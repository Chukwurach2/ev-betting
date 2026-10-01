import {Client} from 'pg';

// Weekly collection report for the NFL prospective prop data layer.
// Read-only aggregates over the trailing N days (default 7); no row-level
// data, no picks, no signals. Exists to answer, every week:
//   expected vs successful snapshots, games captured, player-market
//   observations, multi-book coverage by market/checkpoint, missing/failed
//   calls + reasons, actual credits consumed, credits per usable
//   observation, closing-price capture rate, settlement completeness,
//   timestamp/identity-integrity failures.
// Research-data accounting only. Not a wager recommendation.

const Q_COMPLETENESS = `
SELECT checkpoint_name,
       COUNT(*)::int AS due,
       COUNT(*) FILTER (WHERE status = 'ok')::int AS captured,
       COUNT(*) FILTER (WHERE status = 'empty_response')::int AS empty,
       COUNT(*) FILTER (WHERE status = 'no_event_id')::int AS no_event_id,
       COUNT(*) FILTER (WHERE status = 'ambiguous_match')::int AS ambiguous_match,
       COUNT(*) FILTER (WHERE status NOT IN ('ok', 'empty_response'))::int AS missed
FROM public.nfl_prop_collection_attempts
WHERE attempted_at >= now() - ($1 || ' days')::interval
GROUP BY checkpoint_name
ORDER BY checkpoint_name`;

const Q_FAILURES = `
SELECT status, COALESCE(detail, '') AS detail, COUNT(*)::int AS n
FROM public.nfl_prop_collection_attempts
WHERE attempted_at >= now() - ($1 || ' days')::interval
  AND status NOT IN ('ok', 'empty_response')
GROUP BY status, detail
ORDER BY n DESC
LIMIT 25`;

const Q_GAMES = `
SELECT COUNT(DISTINCT canonical_game_id)::int AS games_captured,
       COUNT(*)::int AS snapshots
FROM public.nfl_prop_snapshots
WHERE captured_at >= now() - ($1 || ' days')::interval`;

const Q_QUOTES = `
SELECT market,
       COUNT(*)::int AS quotes,
       COUNT(DISTINCT participant_raw)::int AS participants,
       COUNT(DISTINCT snapshot_key)::int AS snapshots
FROM public.nfl_prop_quotes
WHERE captured_at >= now() - ($1 || ' days')::interval
GROUP BY market
ORDER BY market`;

const Q_COVERAGE = `
SELECT s.checkpoint_name, c.market,
       ROUND(AVG(c.book_count)::numeric, 2) AS avg_books,
       COUNT(*)::int AS n_positions,
       COUNT(*) FILTER (WHERE c.book_count >= 2)::int AS n_multi_book,
       COUNT(*) FILTER (WHERE c.book_count >= 3)::int AS n_three_plus
FROM public.nfl_prop_consensus c
JOIN public.nfl_prop_snapshots s ON s.snapshot_key = c.snapshot_key
WHERE s.captured_at >= now() - ($1 || ' days')::interval
GROUP BY s.checkpoint_name, c.market
ORDER BY s.checkpoint_name, c.market`;

const Q_CREDITS_ATTEMPTS = `
SELECT COALESCE(SUM(credits_consumed), 0)::int AS credits_consumed
FROM public.nfl_prop_collection_attempts
WHERE attempted_at >= now() - ($1 || ' days')::interval`;

const Q_CREDITS_BILLED = `
SELECT endpoint,
       COUNT(*)::int AS n_calls,
       COALESCE(SUM(billed_credits), 0)::int AS billed_credits
FROM public.ncaaf_quota_log
WHERE logged_at >= now() - ($1 || ' days')::interval
  AND endpoint LIKE 'nfl\\_prop\\_%' ESCAPE '\\'
GROUP BY endpoint
ORDER BY billed_credits DESC`;

const Q_COST_MODEL = `
SELECT endpoint, n_markets, n_calls,
       ROUND(avg_billed::numeric, 2) AS avg_billed,
       ROUND(p50_billed::numeric, 2) AS p50_billed,
       ROUND(p90_billed::numeric, 2) AS p90_billed
FROM public.nfl_prop_cost_model
ORDER BY endpoint, n_markets`;

const Q_SETTLEMENTS = `
SELECT COUNT(DISTINCT canonical_game_id)::int AS games_with_settlements,
       COUNT(*)::int AS settlement_rows,
       COUNT(*) FILTER (WHERE status = 'settled')::int AS settled,
       COUNT(*) FILTER (WHERE status = 'push')::int AS pushes,
       COUNT(*) FILTER (WHERE status = 'void')::int AS voids
FROM public.nfl_prop_settlements`;

const Q_TS_VIOLATIONS = `
SELECT COUNT(*)::int AS n
FROM public.nfl_prop_snapshots
WHERE captured_at >= now() - ($1 || ' days')::interval
  AND (captured_at < checkpoint_target_at OR captured_at >= kickoff)`;

const Q_HEARTBEAT = `
SELECT last_ok_at, detail
FROM public.nfl_edge_heartbeats
WHERE component = 'prop_collector'`;

// Planned-but-never-attempted checkpoints. The completeness query above only
// sees checkpoints a collector run actually touched: when a dispatch is
// skipped before any run exists (e.g. the watchdog's no-dispatch rule under
// the GitHub Actions spending block), no attempt row is written and the
// checkpoint silently vanishes from "due". This query reconstructs the plan
// deterministically from captured games (kickoff) x the frozen checkpoint
// offsets (ops/nfl_prop_collect.py DEFAULT_CHECKPOINTS), so such gaps stay
// visible. The 2h grace keeps in-flight windows from counting as missed.
const Q_NEVER_ATTEMPTED = `
WITH games AS (
  SELECT canonical_game_id, MAX(kickoff) AS kickoff
  FROM public.nfl_prop_snapshots
  WHERE captured_at >= now() - ($1 || ' days')::interval
  GROUP BY canonical_game_id
),
offsets(cp, mins) AS (
  VALUES ('T-24h', 1440), ('T-12h', 720), ('T-6h', 360),
         ('T-3h', 180), ('T-90m', 90), ('Close', 5)
),
expected AS (
  SELECT g.canonical_game_id, o.cp AS checkpoint_name,
         (g.kickoff - (o.mins || ' minutes')::interval) AS target_at
  FROM games g CROSS JOIN offsets o
)
SELECT e.checkpoint_name, COUNT(*)::int AS never_attempted
FROM expected e
WHERE e.target_at >= now() - ($1 || ' days')::interval
  AND e.target_at < now() - interval '2 hours'
  AND NOT EXISTS (
    SELECT 1 FROM public.nfl_prop_collection_attempts a
    WHERE a.canonical_game_id = e.canonical_game_id
      AND a.checkpoint_name = e.checkpoint_name
  )
GROUP BY e.checkpoint_name
ORDER BY e.checkpoint_name`;

// Pure merge: attempt-based completeness rows + never-attempted rows ->
// rows carrying both the legacy attempt-based capture_rate and the honest
// plan-based capture_rate_planned. Exported for unit tests.
export function mergeCompleteness(completenessRows, neverAttemptedRows) {
  const neverMap = new Map(
    (neverAttemptedRows || []).map((r) => [
      r.checkpoint_name,
      Number(r.never_attempted) || 0,
    ]),
  );
  const byCheckpoint = (completenessRows || []).map((r) => {
    const neverAttempted = neverMap.get(r.checkpoint_name) || 0;
    const planned = Number(r.due) + neverAttempted;
    return {
      ...r,
      never_attempted: neverAttempted,
      planned,
      capture_rate_planned: planned > 0 ? Number(r.captured) / planned : null,
      capture_rate: Number(r.due) > 0 ? Number(r.captured) / Number(r.due) : null,
    };
  });
  const totals = byCheckpoint.reduce(
    (a, r) => ({
      due: a.due + Number(r.due),
      captured: a.captured + Number(r.captured),
      planned: a.planned + r.planned,
      neverAttempted: a.neverAttempted + r.never_attempted,
    }),
    {due: 0, captured: 0, planned: 0, neverAttempted: 0},
  );
  return {byCheckpoint, totals};
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'private, no-store');
  if (req.method !== 'GET') return res.status(405).json({error: 'Method not allowed'});
  if (!process.env.NFL_EDGE_DATABASE_URL) {
    return res.status(503).json({error: 'Database is not configured'});
  }
  const days = Math.min(Math.max(parseInt(req.query.days, 10) || 7, 1), 90);
  const client = new Client({
    connectionString: process.env.NFL_EDGE_DATABASE_URL,
    ssl: {rejectUnauthorized: true},
    statement_timeout: 15000,
  });
  try {
    await client.connect();
    const completeness = (await client.query(Q_COMPLETENESS, [days])).rows;
    const failures = (await client.query(Q_FAILURES, [days])).rows;
    const games = (await client.query(Q_GAMES, [days])).rows[0] || {games_captured: 0, snapshots: 0};
    const quotes = (await client.query(Q_QUOTES, [days])).rows;
    const coverage = (await client.query(Q_COVERAGE, [days])).rows;
    const creditsAttempts = Number((await client.query(Q_CREDITS_ATTEMPTS, [days])).rows[0]?.credits_consumed || 0);
    const creditsBilled = (await client.query(Q_CREDITS_BILLED, [days])).rows;
    const costModel = (await client.query(Q_COST_MODEL)).rows;
    const settlements = (await client.query(Q_SETTLEMENTS)).rows[0] || {};
    const tsViolations = Number((await client.query(Q_TS_VIOLATIONS, [days])).rows[0]?.n || 0);
    const hb = (await client.query(Q_HEARTBEAT)).rows[0] || null;
    const neverAttempted = (await client.query(Q_NEVER_ATTEMPTED, [days])).rows;

    const totalQuotes = quotes.reduce((a, r) => a + Number(r.quotes), 0);
    const billedTotal = creditsBilled.reduce((a, r) => a + Number(r.billed_credits), 0);
    const {byCheckpoint, totals} = mergeCompleteness(completeness, neverAttempted);
    const closeRow = byCheckpoint.find((r) => r.checkpoint_name === 'Close');

    return res.status(200).json({
      mode: 'research-data',
      window_days: days,
      generated_at: new Date().toISOString(),
      disclaimer: 'Research-data accounting only. Not a wager recommendation.',
      snapshots: {
        by_checkpoint: byCheckpoint,
        overall: {
          due: totals.due,
          captured: totals.captured,
          capture_rate: totals.due > 0 ? totals.captured / totals.due : null,
          planned: totals.planned,
          never_attempted: totals.neverAttempted,
          capture_rate_planned:
            totals.planned > 0 ? totals.captured / totals.planned : null,
        },
        plan_note:
          'planned = attempt rows + never_attempted; never_attempted = expected ' +
          'checkpoints (snapshot kickoffs x frozen DEFAULT_CHECKPOINTS offsets) ' +
          'with target in window, >2h old, and no attempt row (e.g. skipped dispatches)',
      },
      closing_price_capture_rate:
        closeRow && closeRow.due > 0 ? closeRow.captured / closeRow.due : null,
      closing_price_capture_rate_planned:
        closeRow && closeRow.planned > 0
          ? closeRow.captured / closeRow.planned
          : null,
      games: games,
      quotes_by_market: quotes,
      total_quotes: totalQuotes,
      multibook_coverage: coverage.map((r) => ({
        ...r,
        multi_book_rate: r.n_positions > 0 ? r.n_multi_book / r.n_positions : null,
      })),
      failures,
      credits: {
        consumed_attempts_accounting: creditsAttempts,
        billed_by_endpoint: creditsBilled,
        billed_total: billedTotal,
        per_usable_quote: totalQuotes > 0 ? billedTotal / totalQuotes : null,
        per_snapshot: games.snapshots > 0 ? billedTotal / games.snapshots : null,
      },
      empirical_cost_model: costModel,
      settlements: {
        ...settlements,
        note: 'settlements populated by a later free settlement job; nulls mean not yet run',
      },
      integrity: {
        timestamp_violations: tsViolations,
        ambiguous_matches: completeness.reduce((a, r) => a + r.ambiguous_match, 0),
        no_event_id: completeness.reduce((a, r) => a + r.no_event_id, 0),
      },
      latest_heartbeat: hb
        ? {last_ok_at: hb.last_ok_at, detail: hb.detail}
        : null,
    });
  } catch (e) {
    return res.status(503).json({error: 'Prop collection report unavailable'});
  } finally {
    await client.end().catch(() => {});
  }
}
