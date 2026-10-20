// Offline outcome evaluation only. Never writes wagers or reports realized returns.
export function evaluatePaperOutcome(observation,result,contract,{now=Date.now()}={}){
 const pending=reason=>({schema:'phone-paper-outcome-v1',observation_id:observation?.id||null,status:'unresolved',reason,stake:null,profit_loss:null});
 const o=observation;
 if(o?.kind!=='observation'||!['nfl','nba'].includes(o.sport)||!o.id)return pending('unsupported_observation');
 if(!['FULL_GAME_SPREAD','FULL_GAME_TOTAL'].includes(o.market)||typeof o.line!=='number'||!Number.isFinite(o.line)||Math.abs(o.line%1)!==.5)return pending('unsupported_contract');
 if(!result||result.status!=='final')return pending('final_result_required');
 for(const key of ['sport','game_id','home_team','away_team'])if(!o[key]||o[key]!==result[key])return pending('event_identity_mismatch');
 const kickoff=Date.parse(o.kickoff),start=Date.parse(result.kickoff),at=Date.parse(result.observed_at),quote=Date.parse(o.observed_at);
 if(!Number.isFinite(kickoff)||start!==kickoff||!Number.isFinite(at)||at<kickoff||at>now||!Number.isFinite(quote)||quote>=kickoff)return pending('invalid_event_timing');
 const url=x=>{try{return new URL(x).protocol==='https:';}catch{return false;}};
 if(!url(result.source_url))return pending('result_source_required');
 if(contract?.observation_id!==o.id||contract.rule_key!=='full_game_including_overtime'||!url(contract.source_url))return pending('exact_contract_evidence_required');
 if(result.period!=='full_game'||result.includes_overtime!==true)return pending('result_period_mismatch');
 const home=result.home_score,away=result.away_score;
 if(!Number.isSafeInteger(home)||!Number.isSafeInteger(away)||home<0||away<0||home>1000||away>1000||(o.sport==='nba'&&home===away))return pending('invalid_final_score');
 let margin;
 if(o.market==='FULL_GAME_TOTAL'){
  if(!['Over','Under'].includes(o.selection))return pending('unsupported_selection');
  margin=(home+away-o.line)*(o.selection==='Over'?1:-1);
 }else{
  if(![o.home_team,o.away_team].includes(o.selection))return pending('unsupported_selection');
  margin=(o.selection===o.home_team?home-away:away-home)+o.line;
 }
 return {schema:'phone-paper-outcome-v1',observation_id:o.id,status:'outcome_evaluated',outcome:margin>0?'win':'loss',result_source:result.source_url,result_observed_at:result.observed_at,rule_source:contract.source_url,rule_key:contract.rule_key,stake:null,profit_loss:null,limitations:'Paper contract outcome only. Exact result and rules require manual source verification; no accepted wager, fee reconciliation, ROI or CLV implied.'};
}
