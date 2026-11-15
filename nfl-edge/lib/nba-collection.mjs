import {NBA_EVENTS_URL,nbaGames} from './nba-feed.mjs';
const oddsUrl='https://api.the-odds-api.com/v4/sports/basketball_nba/odds';
function readQuota(headers, name) {
 const value = headers.get(name);
 if (typeof value !== 'string' || !/^\d+$/.test(value)) return null;
 const amount = Number(value);
 return Number.isSafeInteger(amount) ? amount : null;
}

export async function fetchNbaSnapshot({apiKey,fetcher=fetch,reserve=50,now=Date.now()}){
 if(!apiKey||!Number.isSafeInteger(reserve)||reserve<0)throw Error('Invalid NBA collection configuration');
 const request=async(base,params={})=>{
  const url=new URL(base);for(const [k,v] of Object.entries({apiKey,...params}))url.searchParams.set(k,v);
  try{const response=await fetcher(url,{signal:AbortSignal.timeout(15000)});if(!response.ok)throw Error();return response;}catch{throw Error('NBA provider request failed; no retry attempted');}
 };
 const check=await request(NBA_EVENTS_URL);
 const remaining=readQuota(check.headers,'x-requests-remaining');
 // Missing or malformed quota is unknown.
 if(remaining===null||remaining<reserve+2)throw Error('NBA quota unknown or below reserve; no odds request made');
 let games;try{games=nbaGames(await check.json(),now);}catch{throw Error('NBA schedule invalid; no odds request made');}
 if(!games.length)return {events:[],remaining_before:remaining,remaining_after:null,credits_charged:null,maximum_odds_requests:0,status:'no_upcoming_events'};
 // Fixed two markets and <=10 selected books: budget two credits per request.
 const response=await request(oddsUrl,{regions:'us',markets:'spreads,totals',oddsFormat:'american',bookmakers:'draftkings,fanduel,betmgm,espnbet,williamhill_us,fanatics'});
 let events;try{events=await response.json();if(!Array.isArray(events))throw Error();}catch{throw Error('NBA odds payload invalid; no prices published');}
 const remainingAfter=readQuota(response.headers,'x-requests-remaining');
 const charged=readQuota(response.headers,'x-requests-last');
 if(remainingAfter===null||remainingAfter<reserve||charged===null||charged>2||
    (events.length>0&&charged===0))throw Error('NBA odds quota receipt outside bounds; no prices published');
 return {events,remaining_before:remaining,remaining_after:remainingAfter,credits_charged:charged,maximum_odds_requests:1,status:'fetched'};
}
