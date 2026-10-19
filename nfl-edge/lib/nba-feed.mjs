// Provider contract: https://the-odds-api.com/liveapi/guides/v4/
export const NBA_EVENTS_URL='https://api.the-odds-api.com/v4/sports/basketball_nba/events';
export function nbaGames(events,now=Date.now()){
 if(!Array.isArray(events))throw Error('Invalid schedule');
 return events.filter(e=>e.sport_key==='basketball_nba'&&typeof e.id==='string'&&e.id&&typeof e.home_team==='string'&&typeof e.away_team==='string'&&Date.parse(e.commence_time)>now&&Date.parse(e.commence_time)<now+8*86400000).map(e=>({sport:'nba',game_id:'nba-'+e.id,provider_event_id:e.id,home_team:e.home_team,away_team:e.away_team,kickoff:e.commence_time})).sort((a,b)=>Date.parse(a.kickoff)-Date.parse(b.kickoff));
}
export function pairedNbaQuotes(events){
 const result=[],implied=p=>p>0?100/(100+p):-p/(100-p);
 for(const e of events){
  if(e.sport_key!=='basketball_nba'||!e.id||!e.home_team||!e.away_team||!Number.isFinite(Date.parse(e.commence_time)))continue;
  for(const b of e.bookmakers||[])for(const m of b.markets||[]){
   if(!b.key||!['spreads','totals'].includes(m.key)||!Number.isFinite(Date.parse(m.last_update)))continue;
   const sides=m.outcomes||[];if(sides.length!==2)continue;
   if(sides.some(x=>!Number.isInteger(x.price)||Math.abs(x.price)<100||!Number.isFinite(x.point)||Math.abs(x.point%1)!==.5))continue;
   if(m.key==='totals'&&(new Set(sides.map(x=>x.name)).size!==2||!sides.every(x=>['Over','Under'].includes(x.name))||sides[0].point!==sides[1].point||sides[0].point<0))continue;
   if(m.key==='spreads'&&(!sides.some(x=>x.name===e.home_team)||!sides.some(x=>x.name===e.away_team)||Math.abs(sides[0].point+sides[1].point)>1e-9))continue;
   const overround=sides.reduce((sum,x)=>sum+implied(x.price),0);
   for(const x of sides)result.push({sport:'nba',provider_event_id:e.id,home_team:e.home_team,away_team:e.away_team,kickoff:e.commence_time,sportsbook:b.title||b.key,book_key:b.key,market:m.key==='totals'?'FULL_GAME_TOTAL':'FULL_GAME_SPREAD',selection:x.name,line:x.point,american_odds:x.price,fair_probability:implied(x.price)/overround,observed_at:m.last_update,settlement_rules:'Full game including overtime; half-point line; operator void rules require review'});
  }
 }
 return result;
}
