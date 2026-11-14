// Curated research is separate from model predictions and personal wagers.
export const LEAN_BOOKS={fanduel:'FanDuel',draftkings:'DraftKings',betmgm:'BetMGM',williamhill_us:'Caesars',fanatics:'Fanatics',thescorebet:'theScore Bet',ballybet:'Bally Bet'};
const text=(x)=>typeof x==='string'&&x.trim().length>0;
function sourceTime(value){
 if(typeof value!=='string')return false;
 const match=/^(\d{4}-\d{2}-\d{2})T(?:[01]\d|2[0-3]):\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})$/.exec(value);
 if(!match||!Number.isFinite(Date.parse(value)))return false;
 // Date.parse can roll impossible dates into the following month.
 const date=new Date(match[1]);
 return Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===match[1];
}
export function validateLean(p){
 if(!p||!['nfl','nba'].includes(p.sport)||!Object.hasOwn(LEAN_BOOKS,p.book_key))throw Error('Unsupported sport or offer venue');
 for(const k of ['id','game_id','away_team','home_team','selection','market','rules','source','method','rationale','conditions','correlation','fee_assumptions'])if(!text(p[k]))throw Error('Missing '+k);
 if(p.rights!=='private_original_analysis')throw Error('Private original analysis required');
 for(const k of ['kickoff','observed_at','expires_at'])if(!sourceTime(p[k]))throw Error('Missing timestamp '+k);
 if(p.reference_at!==null&&!sourceTime(p.reference_at))throw Error('Invalid reference timestamp');
 if(p.reference_at===null&&!['conditional','invalidated'].includes(p.review_status))throw Error('Unknown reference age requires conditional review');
 const offer=Date.parse(p.observed_at),reference=p.reference_at===null?offer:Date.parse(p.reference_at);
 const expiry=Date.parse(p.expires_at);
 if(expiry>Math.min(offer,reference)+15*60000||expiry<=Math.max(offer,reference))throw Error('Invalid expiry');
 if(!Number.isInteger(p.american_odds)||Math.abs(p.american_odds)<100||!Number.isInteger(p.minimum_odds)||Math.abs(p.minimum_odds)<100)throw Error('Invalid odds');
 if(!(p.fair_probability>0&&p.fair_probability<1)||!['provider_derived','independent_estimate'].includes(p.fair_value_type))throw Error('Invalid fair value');
 if(!['conditional','observed','invalidated'].includes(p.review_status))throw Error('Invalid review status');
 return p;
}
export function leanState(p,now=Date.now()){
 try{validateLean(p);}catch{return 'unavailable';}
 if(p.review_status==='invalidated')return 'invalidated';
 if(Date.parse(p.observed_at)>now||Date.parse(p.reference_at)>now)return 'unavailable';
 if(Date.parse(p.expires_at)<=now||Date.parse(p.kickoff)<=now)return 'expired';
 if(p.reference_at===null||p.review_status==='conditional'||p.offer_verified!==true||p.availability_checked!==true)return 'conditional';
 return 'observed';
}
export function visibleLeans(rows,{sport,books,now=Date.now()}){
 const seen=new Set();
 return rows.map(r=>r.payload).filter(p=>{
  const state=leanState(p,now);if(state==='unavailable'||p.sport!==sport)return false;
  const key=[p.sport,p.game_id,p.market,p.selection,p.line??''].join('|');if(seen.has(key))return false;seen.add(key);return books.includes(p.book_key);
 }).map(p=>({...p,state:leanState(p,now)}));
}
