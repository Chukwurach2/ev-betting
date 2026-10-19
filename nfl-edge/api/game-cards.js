import {Client} from 'pg';
import {buildGameCards,NY_BOOKS,normalizeBook} from '../lib/game-cards.mjs';
export default async function handler(req,res){
 res.setHeader('Cache-Control','private, no-store');
 if(req.method!=='GET')return res.status(405).json({error:'Method not allowed'});
 if(!process.env.NFL_EDGE_DATABASE_URL)return res.status(503).json({error:'Picks data is not connected'});
 const client=new Client({connectionString:process.env.NFL_EDGE_DATABASE_URL,ssl:{rejectUnauthorized:true},statement_timeout:8000});
 try{
  await client.connect();
  const games=(await client.query(`SELECT DISTINCT ON (home_team,away_team,kickoff) game_id,home_team,away_team,kickoff,week FROM public.games WHERE season=2026 AND sport='nfl' AND kickoff>now() AND kickoff<now()+interval '8 days' ORDER BY home_team,away_team,kickoff,game_id`)).rows.sort((a,b)=>new Date(a.kickoff)-new Date(b.kickoff));
  const quotes=(await client.query(`SELECT DISTINCT ON (home_team,away_team,kickoff,market,selection,line,book_key) home_team,away_team,kickoff,market,selection,line,sportsbook,book_key,american_odds,fair_probability,observed_at FROM public.nfl_edge_odds_quotes WHERE kickoff>now() AND kickoff<now()+interval '8 days' AND observed_at>=now()-interval '15 minutes' ORDER BY home_team,away_team,kickoff,market,selection,line,book_key,observed_at DESC LIMIT 12000`)).rows;
  const requested=String(req.headers['x-edge-books']||'').split(',').filter(k=>NY_BOOKS[k]).map(normalizeBook);
  // Preferences travel in a header; never cache one user's filtered response for another.
  return res.status(200).json({mode:'paper',generated_at:new Date().toISOString(),games:buildGameCards(games,quotes,{offeredBooks:requested.length?requested:null}),disclaimer:'Exploratory market leans. Not proven returns. Recheck the exact price.'});
 }catch{return res.status(503).json({error:'Game cards temporarily unavailable'});}
 finally{await client.end().catch(()=>{});}
}
