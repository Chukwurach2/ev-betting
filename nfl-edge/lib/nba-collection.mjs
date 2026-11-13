import {NBA_EVENTS_URL,nbaGames} from './nba-feed.mjs';
const oddsUrl='https://api.the-odds-api.com/v4/sports/basketball_nba/odds';
export async function fetchNbaSnapshot({apiKey,fetcher=fetch,reserve=50,now=Date.now()}){
 if(!apiKey||!Number.isSafeInteger(reserve)||reserve<0)throw Error('Invalid NBA collection configuration');
 const request=async(base,params={})=>{
  const url=new URL(base);for(const [k,v] of Object.entries({apiKey,...params}))url.searchParams.set(k,v);
  try{const response=await fetcher(url,{signal:AbortSignal.timeout(15000)});if(!response.ok)throw Error();return response;}catch{throw Error('NBA provider request failed; no retry attempted');}
 };
 const check=await request(NBA_EVENTS_URL);
 const raw=check.headers.get('x-requests-remaining');
 // Empty/missing/fractional quota is unknown, never zero coerced into permission.
 const remaining=typeof raw==='string'&&/^\d+$/.test(raw)?Number(raw):NaN;
 if(!Number.isSafeInteger(remaining)||remaining<reserve+2)throw Error('NBA quota unknown or below reserve; no odds request made');
 let schedule;try{schedule=nbaGames(await check.json(),now);}catch{throw Error('NBA schedule invalid; no odds request made');}
 if(!schedule.length)return {events:[],remaining_before:remaining,maximum_odds_requests:0,status:'no_upcoming_events'};
 // Fixed two markets and <=10 selected books: budget two credits per request.
 const response=await request(oddsUrl,{regions:'us',markets:'spreads,totals',oddsFormat:'american',bookmakers:'draftkings,fanduel,betmgm,espnbet,williamhill_us,fanatics'});
 let events;try{events=await response.json();if(!Array.isArray(events))throw Error();}catch{throw Error('NBA odds payload invalid; no prices published');}
 const scheduled=new Map();
 for(const game of schedule){
  const id=game.provider_event_id;
  if(scheduled.has(id))scheduled.set(id,null);
  else scheduled.set(id,game);
 }
 const counts=new Map();
 for(const event of events){
  if(event?.sport_key==='basketball_nba')counts.set(event.id,(counts.get(event.id)||0)+1);
 }
 const matched=events.filter(event=>{
  const game=scheduled.get(event?.id);
  return game&&event.sport_key==='basketball_nba'&&counts.get(event.id)===1
   &&event.home_team===game.home_team&&event.away_team===game.away_team
   &&Date.parse(event.commence_time)===Date.parse(game.kickoff);
 });
 return {events:matched,remaining_before:remaining,maximum_odds_requests:1,status:'fetched'};
}
