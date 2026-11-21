// Offline manual evidence bridge; never changes stored identities or quote times.
export function reviewedEventMapping(o,r,m,now){
 const https=x=>{try{return new URL(x).protocol==='https:';}catch{return false;}};
 if(!m||m.review_status!=='reviewed'||typeof m.id!=='string'||!m.id||typeof m.reason!=='string'||!m.reason.trim()||!https(m.source_url)||m.source_url!==r.source_url)return false;
 for(const k of ['sport','game_id','home_team','away_team'])if(m[k]!==o[k]||o[k]!==r[k])return false;
 if(!r.external_game_id||m.external_game_id!==r.external_game_id)return false;
 if(typeof o.kickoff!=='string'||typeof r.kickoff!=='string'||m.observation_kickoff!==o.kickoff||m.official_kickoff!==r.kickoff)return false;
 const a=Date.parse(o.kickoff),b=Date.parse(r.kickoff),review=Date.parse(m.reviewed_at),resultAt=Date.parse(r.observed_at);
 // Same UTC event day and <=15-minute discrepancy are only a guardrail,
 // never an automatic tolerance. Every bridge still requires exact evidence.
 return Number.isFinite(a)&&Number.isFinite(b)&&a!==b&&Math.abs(a-b)<=900000&&o.kickoff.slice(0,10)===r.kickoff.slice(0,10)&&Number.isFinite(review)&&Number.isFinite(resultAt)&&review>=resultAt&&review<=now;
}
