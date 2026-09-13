import {Client} from 'pg';
import {buildEngineVerdict} from '../lib/sunday-board-verdict.js';

// Sunday Board: decision-support data board for the 2026-09-13 slate.
// READ-ONLY. Stored quotes only — never pulls provider quota.
//
// This is NOT a picks list. It shows per-game price data (spread/total
// across books), best available price per side, line movement since the
// opener capture, Pinnacle vs consensus as the sharp marker, and the
// engine's assessment of each side against its 4% bar. The authoritative
// engine verdict comes from the actual nfl_edge_picks table (expect zero
// qualifying shadow picks), never from a re-implementation.
//
// Copy rules (hard): the response carries "Shadow research — not a wager
// recommendation." Nothing here is framed as an action: no "take", "play",
// "bet", "pick" (verb), or "recommendation" (verb).

const DAY_START = '2026-09-13T00:00:00Z';
// Include the complete US Sunday slate, including Sunday Night Football\n// after UTC midnight. The following Monday-night game is later than this bound.\nconst DAY_END = '2026-09-14T12:00:00Z';
const ENGINE_VERSION = 'v1.2-consensus-lobo-4pct-15m';
const MIN_EDGE = 0.04; // 4% bar, mirrors ops/picks.py v1.2
const MIN_AMERICAN_ODDS = -150; // user-authorized price floor
const MIN_BOOKS = 3; // strict LOBO needs 3+ distinct books per line group
const PINNACLE_KEY = 'pinnacle';

const Q_GAMES = `
SELECT game_id, week, home_team, away_team, kickoff
FROM public.games
WHERE kickoff >= $1 AND kickoff < $2
ORDER BY kickoff ASC`;

const Q_LATEST = `
SELECT DISTINCT ON (q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.book_key)
  q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.line,
  q.sportsbook, q.book_key, q.american_odds, q.fair_probability,
  q.observed_at, q.collected_at, c.decision_window
FROM public.nfl_edge_odds_quotes q
LEFT JOIN public.nfl_edge_checkpoints c ON c.checkpoint_key = q.checkpoint_key
WHERE q.kickoff >= $1 AND q.kickoff < $2
ORDER BY q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.book_key, q.observed_at DESC`;

const Q_OPENER = `
SELECT DISTINCT ON (q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.book_key)
  q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.line,
  q.sportsbook, q.book_key, q.american_odds, q.fair_probability, q.observed_at
FROM public.nfl_edge_odds_quotes q
JOIN public.nfl_edge_checkpoints c ON c.checkpoint_key = q.checkpoint_key
WHERE q.kickoff >= $1 AND q.kickoff < $2 AND c.decision_window = 'Opener'
ORDER BY q.home_team, q.away_team, q.kickoff, q.market, q.selection, q.book_key, q.observed_at ASC`;

const Q_ENGINE_PICKS = `
SELECT count(*)::int AS n
FROM public.nfl_edge_picks
WHERE engine_version = $3 AND mode = 'shadow'
  AND kickoff >= $1 AND kickoff < $2`;

function americanToDecimal(odds) {
  const o = Number(odds);
  return o > 0 ? 1 + o / 100 : 1 + 100 / Math.abs(o);
}

function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function lineKey(line) {
  return String(Number(line));
}

// Team abbreviations differ between the provider feed and the games table
// (same normalization the frontend uses); match on the normalized pair so
// kickoff-timestamp drift cannot orphan quotes from their game.
const TEAM_NORM = {LA: 'LAR', ARI: 'AZ'};
function normTeam(t) {
  return TEAM_NORM[t] || t;
}
function matchupKey(home, away) {
  return `${normTeam(away)}|${normTeam(home)}`;
}

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
    const games = (await client.query(Q_GAMES, [DAY_START, DAY_END])).rows;
    const latest = (await client.query(Q_LATEST, [DAY_START, DAY_END])).rows;
    const opener = (await client.query(Q_OPENER, [DAY_START, DAY_END])).rows;
    const enginePicks = Number(
      (await client.query(Q_ENGINE_PICKS, [DAY_START, DAY_END, ENGINE_VERSION])).rows[0]?.n || 0
    );

    // Dedupe games the same way the frontend does: one row per
    // (week, away, home), preferring game_id starting with 'nfl-'.
    const gameMap = new Map();
    for (const g of games) {
      const k = `${g.week}|${matchupKey(g.home_team, g.away_team)}`;
      const old = gameMap.get(k);
      if (!old || String(g.game_id).startsWith('nfl-')) gameMap.set(k, g);
    }
    const slate = [...gameMap.values()].sort((a, b) => new Date(a.kickoff) - new Date(b.kickoff));

    const byGame = new Map();
    for (const q of latest) {
      const k = matchupKey(q.home_team, q.away_team);
      if (!byGame.has(k)) byGame.set(k, []);
      byGame.get(k).push(q);
    }
    const openerByGame = new Map();
    for (const q of opener) {
      const k = matchupKey(q.home_team, q.away_team);
      if (!openerByGame.has(k)) openerByGame.set(k, []);
      openerByGame.get(k).push(q);
    }

    const bestPrices = [];
    let gamesWithQuotes = 0;

    const gameCards = slate.map((g) => {
      const k = matchupKey(g.home_team, g.away_team);
      const quotes = byGame.get(k) || [];
      const openers = openerByGame.get(k) || [];
      if (quotes.length) gamesWithQuotes += 1;

      const markets = {};
      for (const market of ['FULL_GAME_SPREAD', 'FULL_GAME_TOTAL']) {
        const mq = quotes.filter((q) => q.market === market);
        const mo = openers.filter((q) => q.market === market);
        if (!mq.length) {
          markets[market] = {status: 'no_data'};
          continue;
        }
        // Group by (selection, line): consensus is only valid over identical lines.
        const groups = new Map();
        for (const q of mq) {
          const gk = `${q.selection}|${lineKey(q.line)}`;
          if (!groups.has(gk)) groups.set(gk, {selection: q.selection, line: Number(q.line), rows: []});
          groups.get(gk).rows.push(q);
        }
        const sides = [];
        for (const grp of groups.values()) {
          const books = [...new Set(grp.rows.map((r) => r.book_key))];
          const fairAll = grp.rows.map((r) => Number(r.fair_probability));
          const consensus = median(fairAll);
          // LOBO edge per book: each book judged against the others only.
          let best = null;
          for (const r of grp.rows) {
            const others = grp.rows.filter((x) => x.book_key !== r.book_key);
            const otherBooks = new Set(others.map((x) => x.book_key));
            let edge = null;
            if (books.length >= MIN_BOOKS && otherBooks.size >= MIN_BOOKS - 1 && Number(r.american_odds) >= MIN_AMERICAN_ODDS) {
              const lob = median(others.map((x) => Number(x.fair_probability)));
              edge = lob * americanToDecimal(r.american_odds) - 1;
            }
            const row = {
              book: r.sportsbook,
              book_key: r.book_key,
              price: Number(r.american_odds),
              fair_prob: Number(r.fair_probability),
              edge_pp: edge == null ? null : Math.round(edge * 10000) / 100,
              observed_at: r.observed_at,
            };
            if (edge != null && (best == null || edge > best.edge)) best = {row, edge};
          }
          const pin = grp.rows.find((r) => r.book_key === PINNACLE_KEY);
          const openerLines = mo
            .filter((o) => o.selection === grp.selection)
            .map((o) => Number(o.line));
          const openerLine = median(openerLines);
          const edgePp = best ? Math.round(best.edge * 10000) / 100 : null;
          const clearsBar = edgePp != null && edgePp >= MIN_EDGE * 100;
          const assessment =
            books.length < MIN_BOOKS
              ? `DOES NOT QUALIFY — fewer than ${MIN_BOOKS} books at this line, no consensus`
              : best == null
                ? 'DOES NOT QUALIFY — no book clears the price floor at this line'
                : clearsBar
                  ? 'QUALIFIES — edge at or above the 4% bar'
                  : `DOES NOT QUALIFY — best edge ${edgePp.toFixed(2)}pp is below the 4% bar`;
          sides.push({
            selection: grp.selection,
            line: grp.line,
            books: grp.rows
              .map((r) => ({book: r.sportsbook, price: Number(r.american_odds), fair_prob: Number(r.fair_probability)}))
              .sort((a, b) => americanToDecimal(b.price) - americanToDecimal(a.price)),
            best_price: best
              ? {book: best.row.book, price: best.row.price, edge_pp: edgePp}
              : null,
            consensus_fair_prob: consensus == null ? null : Math.round(consensus * 10000) / 10000,
            pinnacle:
              pin == null
                ? null
                : {line: Number(pin.line), price: Number(pin.american_odds), fair_prob: Number(pin.fair_probability)},
            movement:
              openerLine == null
                ? null
                : {opener_line: openerLine, latest_line: grp.line},
            edge_estimate_pp: edgePp,
            clears_bar: clearsBar,
            assessment,
          });
          if (best) {
            bestPrices.push({
              game: `${g.away_team} @ ${g.home_team}`,
              kickoff: g.kickoff,
              market,
              selection: grp.selection,
              line: grp.line,
              book: best.row.book,
              price: best.row.price,
              edge_pp: edgePp,
              clears_bar: clearsBar,
            });
          }
        }
        sides.sort((a, b) => (b.edge_estimate_pp ?? -Infinity) - (a.edge_estimate_pp ?? -Infinity));
        markets[market] = {status: 'ok', sides};
      }
      markets['FULL_GAME_MONEYLINE'] = {status: 'no_data', note: 'No stored moneyline quotes for this game.'};

      return {
        game_id: g.game_id,
        away_team: g.away_team,
        home_team: g.home_team,
        kickoff: g.kickoff,
        has_data: quotes.length > 0,
        quote_count: quotes.length,
        markets,
      };
    });

    bestPrices.sort((a, b) => b.edge_pp - a.edge_pp);

    return res.status(200).json({
      mode: 'shadow',
      engine: ENGINE_VERSION,
      edge_bar_pp: MIN_EDGE * 100,
      slate_date: '2026-09-13',
      generated_at: new Date().toISOString(),
      disclaimer: 'Shadow research — not a wager recommendation.',
      engine_verdict: {
        qualifying_shadow_picks: enginePicks,
        verdict: buildEngineVerdict({enginePicks, gamesWithQuotes, slateGames: slate.length}),
      },
      data_coverage: {
        games: slate.length,
        games_with_quotes: gamesWithQuotes,
        games_without_quotes: slate.length - gamesWithQuotes,
      },
      best_prices: bestPrices.slice(0, 15),
      games: gameCards,
    });
  } catch (e) {
    console.error('Sunday board unavailable', {
      name: e?.name || null,
      code: e?.code || null,
      message: String(e?.message || 'unknown').slice(0, 240),
    });
    return res.status(503).json({error: 'Sunday board unavailable'});
  } finally {
    await client.end().catch(() => {});
  }
}
