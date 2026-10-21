// Offline hypothetical returns at observed quotes; never accepted-wager accounting.
import {evaluatePaperOutcome} from './paper-outcomes.mjs';
const identity=o=>JSON.stringify([o.sport,o.game_id,o.kickoff,o.market,o.selection,o.line,o.book_key]);
const event=o=>JSON.stringify([o.sport,o.game_id]);
function timing(o){const h=(Date.parse(o.kickoff)-Date.parse(o.observed_at))/3600000;return h<=1?'0–1h':h<=6?'1–6h':h<=24?'6–24h':'>24h';}
export function paperPerformance(records,{results=[],contracts=[]}={}, {now=Date.now(),feePerUnit=0}={}){
 if(!Number.isFinite(feePerUnit)||feePerUnit<0)throw Error('feePerUnit must be a nonnegative fixed cost per settled 1-unit paper entry');
 const observations=records.filter(o=>o.kind==='observation');
 const unique=new Map();let invalid=0;
 for(const o of observations){
  if(!o.id||!o.book_key||!Number.isFinite(Date.parse(o.observed_at))||!Number.isFinite(Date.parse(o.kickoff))||Date.parse(o.observed_at)>=Date.parse(o.kickoff)||!Number.isInteger(o.american_odds)||Math.abs(o.american_odds)<100){invalid++;continue;}
  const k=identity(o),p=unique.get(k);
  if(!p||Date.parse(o.observed_at)<Date.parse(p.observed_at)||(o.observed_at===p.observed_at&&o.id<p.id))unique.set(k,o);
 }
 const variants=new Map();for(const o of unique.values()){const k=event(o);if(!variants.has(k))variants.set(k,new Set());variants.get(k).add(JSON.stringify([o.kickoff,o.home_team,o.away_team]));}
 const rows=[...unique.values()].sort((a,b)=>Date.parse(a.observed_at)-Date.parse(b.observed_at)||a.id.localeCompare(b.id)).map(o=>{
  const rs=results.filter(r=>r.sport===o.sport&&r.game_id===o.game_id&&r.kickoff===o.kickoff);
  const cs=contracts.filter(c=>c.observation_id===o.id);
  const evaluated=o.schema==='curated-paper-v1'&&(o.review_status!=='observed'||!o.offer_verified||!o.availability_checked||o.reference_at===null)?{status:'unresolved',reason:'curated_review_incomplete'}:variants.get(event(o)).size>1?{status:'unresolved',reason:'conflicting_event_identity'}:rs.length>1||cs.length>1?{status:'unresolved',reason:'ambiguous_result_or_rule'}:evaluatePaperOutcome(o,rs[0],cs[0],{now});
  const settled=evaluated.status==='outcome_evaluated';
  const profit=settled?(evaluated.outcome==='win'?(o.american_odds>0?o.american_odds/100:100/Math.abs(o.american_odds)):-1)-feePerUnit:null;
  return {...o,result:evaluated,paper_profit_units:profit,timing_bucket:timing(o),positive_estimate:typeof o.expected_value==='number'&&o.expected_value>0};
 });
 const unresolved_reasons={};for(const r of rows)if(r.result.status==='unresolved')unresolved_reasons[r.result.reason]=(unresolved_reasons[r.result.reason]||0)+1;
 const identity_conflicts=[...variants.entries()].filter(([,v])=>v.size>1).map(([key,v])=>({event_key:JSON.parse(key),variants:[...v].map(x=>{const [kickoff,home_team,away_team]=JSON.parse(x);return {kickoff,home_team,away_team};}),affected_entries:rows.filter(r=>event(r)===key).length}));
 function summarize(xs){const done=xs.filter(x=>x.paper_profit_units!==null),wins=done.filter(x=>x.result.outcome==='win').length;const net=done.reduce((s,x)=>s+x.paper_profit_units,0);return {entries:xs.length,unique_events:new Set(xs.map(event)).size,resolved:done.length,unresolved:xs.length-done.length,wins,losses:done.length-wins,accuracy:done.length?wins/done.length:null,hypothetical_net_units:done.length?net:null,hypothetical_roi:done.length?net/done.length:null};}
 function group(key){const groups={};for(const r of rows){const k=String(key(r));(groups[k]??=[]).push(r);}return Object.fromEntries(Object.entries(groups).map(([k,v])=>[k,summarize(v)]));}
 return {schema:'paper-performance-v1',mode:'paper',generated_at:new Date(now).toISOString(),policy:'earliest observed quote per sport/event/start/contract/book; retrospective descriptive cohort, not a validated strategy',fee_per_settled_unit:feePerUnit,input_observations:observations.length,conflicting_event_ids:identity_conflicts.length,identity_conflicts,unresolved_reasons,invalid_observations:invalid,repeated_observations_removed:observations.length-invalid-rows.length,overall:summarize(rows),positive_estimates:summarize(rows.filter(r=>r.positive_estimate)),by_review_status:group(r=>r.review_status||'market_observation'),by_book:group(r=>r.book_key),by_sport:group(r=>r.sport),by_market:group(r=>r.market),by_timing:group(r=>r.timing_bucket),by_method:group(r=>r.method||'unknown'),by_reference_quality:group(r=>r.reference_freshness||'unknown'),rows,limitations:'Hypothetical fixed 1-unit quote returns only; no accepted wager, liquidity, tax or CLV. Opposing selections, books and same-game entries are correlated. Unresolved entries are excluded from return denominators and reported explicitly. Zero default fee is an assumption, not verified friction. Props/pushes/voids remain unresolved until supported.'};
}
