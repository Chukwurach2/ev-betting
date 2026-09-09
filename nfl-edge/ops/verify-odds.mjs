// Bounded build-time coverage check: one free events request and at most one
// totals_q1 request. API credentials never appear in output or error messages.
export function pairedQuotes(event, now=Date.now()) {
  const rows=[];
  for(const book of event.bookmakers||[]){
    if(!['draftkings','fanduel'].includes(book.key))continue;
    for(const market of book.markets||[]){
      if(market.key!=='totals_q1')continue;
      const t=Date.parse(market.last_update);
      if(!Number.isFinite(t)||t>now||now-t>900000)continue;
      const lines=new Map();
      for(const o of market.outcomes||[]){
        if(!['Over','Under'].includes(o.name)||!Number.isFinite(o.point)||Math.abs(o.point%1)!==.5||!Number.isInteger(o.price)||!(o.price<=-100||o.price>=100))continue;
        const pair=lines.get(o.point)||{};pair[o.name]=o.price;lines.set(o.point,pair);
      }
      for(const [line,pair] of lines){
        if(pair.Over==null||pair.Under==null)continue;
        const implied=o=>o>0?100/(100+o):-o/(100-o);
        const over=implied(pair.Over),under=implied(pair.Under);
        rows.push({sportsbook:book.title,book_key:book.key,market:'Q1_TOTAL',line,over_odds:pair.Over,under_odds:pair.Under,over_fair_probability:over/(over+under),under_fair_probability:under/(over+under),observed_at:market.last_update});
      }
    }
  }
  return rows;
}
export async function verifyOdds(apiKey, fetcher=fetch){
  const receipt={checked_at:new Date().toISOString(),provider:'the_odds_api',quotes_verified:false,automatic_picks:false,maximum_paid_requests:1};
  if(!apiKey)return {...receipt,status:'not_configured',reason:'Build environment has no odds key'};
  const base='https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events';
  async function request(path,params){
    const url=new URL(path);url.search=new URLSearchParams({...params,apiKey}).toString();
    const r=await fetcher(url,{signal:AbortSignal.timeout(12000)});
    if(!r.ok)throw Error('Odds provider HTTP '+r.status);
    return {body:await r.json(),remaining:r.headers.get('x-requests-remaining'),cost:r.headers.get('x-requests-last')};
  }
  try{
    const {body:events}=await request(base,{dateFormat:'iso'});
    if(!Array.isArray(events))throw Error('Invalid event response');
    const now=Date.now();
    const event=events.filter(e=>Date.parse(e.commence_time)>now&&Date.parse(e.commence_time)-now<=48*3600000).sort((a,b)=>Date.parse(a.commence_time)-Date.parse(b.commence_time))[0];
    if(!event)return {...receipt,status:'no_upcoming_event',reason:'No NFL event within 48 hours'};
    const {body,remaining,cost}=await request(base+'/'+encodeURIComponent(event.id)+'/odds',{bookmakers:'draftkings,fanduel',markets:'totals_q1',oddsFormat:'american',dateFormat:'iso'});
    if(body.id!==event.id||Date.parse(body.commence_time)<=Date.now())throw Error('Event changed or started');
    const pairs=pairedQuotes(body);
    return {...receipt,status:pairs.length?'verified':'no_fresh_supported_quotes',quotes_verified:pairs.length>0,event:{id:event.id,home:event.home_team,away:event.away_team,kickoff:event.commence_time},market:'totals_q1',pairs,quota_remaining:remaining,request_cost:cost,source_url:base+'/'+event.id+'/odds'};
  }catch(e){return {...receipt,status:'unavailable',reason:/^Odds provider HTTP \d+$/.test(e.message)?e.message:'Provider coverage check failed'};}
}
