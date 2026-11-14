// Provider contract: https://the-odds-api.com/liveapi/guides/v4/
export const NBA_EVENTS_URL='https://api.the-odds-api.com/v4/sports/basketball_nba/events';
export function nbaGames(events,now=Date.now()){
 if(!Array.isArray(events))throw Error('Invalid schedule');
 const games = new Map();
 const ambiguous = new Set();
 for (const event of events) {
  const kickoff = Date.parse(event?.commence_time);
  if (event?.sport_key !== 'basketball_nba' || typeof event.id !== 'string' || !event.id ||
      typeof event.home_team !== 'string' || typeof event.away_team !== 'string' ||
      !(kickoff > now && kickoff < now + 8 * 86400000)) continue;
  const previous = games.get(event.id);
  if (previous) {
   if (previous.home_team !== event.home_team || previous.away_team !== event.away_team ||
       Date.parse(previous.kickoff) !== kickoff) ambiguous.add(event.id);
   continue;
  }
  games.set(event.id, {
   sport:'nba', game_id:'nba-'+event.id, provider_event_id:event.id,
   home_team:event.home_team, away_team:event.away_team, kickoff:event.commence_time
  });
 }
 for (const id of ambiguous) games.delete(id);
 return [...games.values()].sort((a,b)=>Date.parse(a.kickoff)-Date.parse(b.kickoff));
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
