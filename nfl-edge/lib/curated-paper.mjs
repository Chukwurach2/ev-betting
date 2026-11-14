import {createHash} from 'node:crypto';
import {validateLean} from './private-leans.mjs';
// Historical private analysis; never refresh or certify an offer during import.
export function curatedPaperRecords(payloads,{now=Date.now()}={}){
 if(!Array.isArray(payloads))throw Error('Expected private original-analysis array');
 const ids=new Map(),out=[];
 for(const payload of payloads){
  const p=validateLean(payload);
  if(Date.parse(p.observed_at)>now)throw Error('Future observation');
  const hash=createHash('sha256').update(JSON.stringify(p)).digest('hex');
  if(ids.has(p.id)){if(ids.get(p.id)!==hash)throw Error('Conflicting curated ID');continue;}
  ids.set(p.id,hash);
  const decimal=p.american_odds>0?1+p.american_odds/100:1+100/Math.abs(p.american_odds);
  out.push({...structuredClone(p),schema:'curated-paper-v1',kind:'observation',id:'curated:'+p.id,line:p.line??null,reference_freshness:p.reference_at===null?'unknown_curated_reference_age':'curated_reference_time_preserved',expected_value:p.fair_probability*decimal-1,captured_at:new Date(now).toISOString(),source_payload_sha256:hash,original_analysis:structuredClone(p),stake:null,accepted_price_verified:false,settlement_status:'unresolved'});
 }
 return out;
}
