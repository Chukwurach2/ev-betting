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
 let schedule;try{schedule=await check.json();nbaGames(schedule,now);}catch{throw Error('NBA schedule invalid; no odds request made');}
 if(!nbaGames(schedule,now).length)return {events:[],remaining_before:remaining,maximum_odds_requests:0,status:'no_upcoming_events'};
 // Fixed two markets and <=10 selected books: budget two credits per request.
 const response=await request(oddsUrl,{regions:'us',markets:'spreads,totals',oddsFormat:'american',bookmakers:'draftkings,fanduel,betmgm,espnbet,williamhill_us,fanatics'});
 let events;try{events=await response.json();if(!Array.isArray(events))throw Error();}catch{throw Error('NBA odds payload invalid; no prices published');}
 return {events,remaining_before:remaining,maximum_odds_requests:1,status:'fetched'};
}
