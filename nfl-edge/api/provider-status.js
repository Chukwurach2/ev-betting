let cached;
let pending;
export default async function handler(req,res){
  res.setHeader('Cache-Control','public, max-age=0, s-maxage=300');
  if(req.method!=='GET')return res.status(405).json({error:'Method not allowed'});
  if(!process.env.THE_ODDS_API_KEY)return res.status(503).json({provider:'the_odds_api',authenticated:false,error:'Odds key is not configured'});
  if(cached&&Date.now()-cached.at<300000)return res.status(cached.status).json(cached.body);
  pending??=(async()=>{
    try{
      const url=new URL('https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events');
      url.searchParams.set('apiKey',process.env.THE_ODDS_API_KEY);
      const response=await fetch(url,{signal:AbortSignal.timeout(10000)});
      if(!response.ok)return {at:Date.now(),status:503,body:{provider:'the_odds_api',authenticated:false,upstream_status:response.status,error:'Provider authentication or availability check failed'}};
      const events=await response.json();
      if(!Array.isArray(events))throw Error('Invalid events');
      return {at:Date.now(),status:200,body:{provider:'the_odds_api',authenticated:true,events_available:events.length,quotes_verified:false,automatic_picks:false,checked_at:new Date().toISOString()}};
    }catch{return {at:Date.now(),status:503,body:{provider:'the_odds_api',authenticated:false,error:'Provider temporarily unavailable'}};}
  })();
  try{cached=await pending;return res.status(cached.status).json(cached.body);}finally{pending=null;}
}
