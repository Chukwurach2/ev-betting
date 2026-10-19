import {createHash} from 'node:crypto';
import {NY_BOOKS,normalizeBook} from './game-cards.mjs';
const digest=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const text=x=>typeof x==='string'&&x.trim().length>0;
export function paperRecords(feed,{now=Date.now()}={}){
 if(!feed||feed.mode!=='paper'||!['nfl','nba'].includes(feed.sport)||!Array.isArray(feed.games))throw Error('Expected a paper NFL or NBA card feed');
 const captured_at=new Date(now).toISOString(),records=[];
 for(const g of feed.games){
  if(!text(g.game_id)||!text(g.home_team)||!text(g.away_team)||!Number.isFinite(Date.parse(g.kickoff)))throw Error('Invalid game identity');
  if(g.sport&&g.sport!==feed.sport)throw Error('Sport mismatch');
  if(Date.parse(g.kickoff)<=now)continue;
  if(!Array.isArray(g.picks)||g.picks.length>4)throw Error('Invalid per-game picks');
  const identity={sport:feed.sport,game_id:g.game_id,home_team:g.home_team,away_team:g.away_team,kickoff:new Date(g.kickoff).toISOString()};
  let recorded=0,rejected=0;
  const seen=new Set();
  for(const p of g.picks){
   const observed=Date.parse(p.observed_at),odds=p.american_odds,prob=p.fair_probability;
   const valid=NY_BOOKS[p.book_key]&&['FULL_GAME_SPREAD','FULL_GAME_TOTAL','Q1_TOTAL'].includes(p.market)&&text(p.selection)&&typeof p.line==='number'&&Number.isFinite(p.line)&&Math.abs(p.line%1)===.5&&Number.isInteger(odds)&&Math.abs(odds)>=100&&typeof prob==='number'&&prob>0&&prob<1&&Number.isInteger(p.reference_books)&&p.reference_books>=2&&Number.isFinite(observed)&&observed<=now&&now-observed<=900000;
   if(!valid){rejected++;continue;}
   if(feed.sport==='nba'&&p.market==='Q1_TOTAL'){rejected++;continue;}
   const contract=JSON.stringify([p.market,p.selection,p.line]);
   if(seen.has(contract)){rejected++;continue;}seen.add(contract);
   const decimal=odds>0?1+odds/100:1+100/Math.abs(odds);
   const evidence={...identity,market:p.market,selection:p.selection,line:p.line,book_key:normalizeBook(p.book_key),american_odds:odds,fair_probability:prob,observed_at:new Date(observed).toISOString(),reference_books:p.reference_books};
   records.push({schema:'phone-paper-v1',id:digest(['observation',evidence]),kind:'observation',...evidence,captured_at,expires_at:new Date(observed+900000).toISOString(),expected_value:prob*decimal-1,break_even_decimal:1/prob,method:'phone-market-consensus-v1',validation:'exploratory',settlement_status:'unresolved',stake:null,accepted_price_verified:false,limitations:'Displayed market estimate only; reference composition and rule equivalence not certified. No wager, fill or profitability evidence.'});recorded++;
  }
  const coverage={...identity,recorded,rejected,shortfall:4-recorded};
  // Coverage is an audit of this capture, not a repeat bet or a performance observation.
  records.push({schema:'phone-paper-v1',id:digest(['coverage',coverage,captured_at]),kind:'coverage',...coverage,captured_at});
 }
 return records;
}
