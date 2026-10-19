import {Client} from 'pg';
import {nbaGames,NBA_EVENTS_URL} from '../lib/nba-feed.mjs';
let nbaSchedule, nbaPending;
async function schedule(){
 if(nbaSchedule&&Date.now()-nbaSchedule.at<300000)return nbaSchedule.events;
 if(!process.env.THE_ODDS_API_KEY)throw Error('Schedule not connected');
 nbaPending??=(async()=>{const url=new URL(NBA_EVENTS_URL);url.searchParams.set('apiKey',process.env.THE_ODDS_API_KEY);const r=await fetch(url,{signal:AbortSignal.timeout(10000)});if(!r.ok)throw Error('Schedule unavailable');const events=await r.json();nbaGames(events);nbaSchedule={at:Date.now(),events};return events;})();
 try{return await nbaPending;}finally{nbaPending=null;}
}
import {buildGameCards,NY_BOOKS,normalizeBook} from '../lib/game-cards.mjs';
export default async function handler(req,res){
 res.setHeader('Cache-Control','private, no-store');
 if(req.method!=='GET')return res.status(405).json({error:'Method not allowed'});
 const sport=req.query?.sport||'nfl';
 if(!['nfl','nba'].includes(sport))return res.status(400).json({error:'Choose NFL or NBA'});
 if(!process.env.NFL_EDGE_DATABASE_URL)return res.status(503).json({error:'Picks data is not connected'});
 const client=new Client({connectionString:process.env.NFL_EDGE_DATABASE_URL,ssl:{rejectUnauthorized:true},statement_timeout:8000});
 try{
  await client.connect();
  const games=sport==='nba'?nbaGames(await schedule()):(await client.query(`SELECT DISTINCT ON (home_team,away_team,kickoff) game_id,home_team,away_team,kickoff,week FROM public.games WHERE season=2026 AND sport='nfl' AND kickoff>now() AND kickoff<now()+interval '8 days' ORDER BY home_team,away_team,kickoff,game_id`)).rows.sort((a,b)=>new Date(a.kickoff)-new Date(b.kickoff));
  const quoteTable=sport==='nba'?'nba_edge_odds_quotes':'nfl_edge_odds_quotes';
  const quotes=(await client.query(`SELECT DISTINCT ON (home_team,away_team,kickoff,market,selection,line,book_key) home_team,away_team,kickoff,market,selection,line,sportsbook,book_key,american_odds,fair_probability,observed_at FROM public.${quoteTable} WHERE kickoff>now() AND kickoff<now()+interval '8 days' AND observed_at>=now()-interval '15 minutes' ORDER BY home_team,away_team,kickoff,market,selection,line,book_key,observed_at DESC LIMIT 12000`)).rows.map(q=>({...q,sport}));
  const requested=String(req.headers['x-edge-books']||'').split(',').filter(k=>NY_BOOKS[k]).map(normalizeBook);
  // Preferences travel in a header; never cache one user's filtered response for another.
  return res.status(200).json({mode:'paper',sport,generated_at:new Date().toISOString(),games:buildGameCards(games,quotes,{offeredBooks:requested.length?requested:null}),disclaimer:'Exploratory market leans. Not proven returns. Recheck the exact price.'});
 }catch{return res.status(503).json({error:'Game cards temporarily unavailable'});}
 finally{await client.end().catch(()=>{});}
}
