import test from 'node:test';import assert from 'node:assert/strict';import {evaluatePaperOutcome as evaluate} from '../lib/paper-outcomes.mjs';
const now=Date.parse('2026-10-20T23:00:00Z');
const o={id:'synthetic-observation',kind:'observation',book_key:'synthetic-book',sport:'nba',game_id:'synthetic-game',home_team:'Home',away_team:'Away',kickoff:'2026-10-20T19:00:00Z',observed_at:'2026-10-20T18:40:00Z',market:'FULL_GAME_SPREAD',selection:'Away',line:3.5};
const r={...o,status:'final',period:'full_game',includes_overtime:true,home_score:110,away_score:108,observed_at:'2026-10-20T22:00:00Z',source_url:'https://example.invalid/synthetic-result'};
const c={book_key:o.book_key,observation_id:o.id,rule_key:'full_game_including_overtime',source_url:'https://example.invalid/synthetic-rule'};
const check=(a=o,b=r,d=c)=>evaluate(a,b,d,{now});
test('scores both sides of exact spread with no wager profit',()=>{assert.equal(check().outcome,'win');assert.equal(check({...o,selection:'Home',line:-3.5}).outcome,'loss');assert.equal(check().profit_loss,null);assert.equal(check().stake,null);});
test('scores exact half-point totals',()=>{assert.equal(check({...o,market:'FULL_GAME_TOTAL',selection:'Over',line:217.5}).outcome,'win');assert.equal(check({...o,market:'FULL_GAME_TOTAL',selection:'Under',line:217.5}).outcome,'loss');});
test('live, missing, void and abandoned results stay unresolved',()=>{for(const status of ['live','void','abandoned',undefined])assert.equal(check(o,{...r,status}).status,'unresolved');assert.equal(check(o,null).reason,'final_result_required');});
test('sport event team and kickoff mismatches fail closed',()=>{for(const patch of [{sport:'nfl'},{game_id:'other'},{home_team:'Other'},{kickoff:'2026-10-20T19:10:00Z'}])assert.equal(check(o,{...r,...patch}).status,'unresolved');});
test('missing or differently linked rule evidence never resolves',()=>{for(const d of [null,{...c,observation_id:'other'},{...c,rule_key:'regulation_only'},{...c,source_url:''}])assert.equal(check(o,r,d).status,'unresolved');});
test('invalid timing scores periods and selections stay unresolved',()=>{for(const patch of [{observed_at:'2026-10-21T23:00:00Z'},{home_score:1.5},{home_score:108},{includes_overtime:false},{period:'q1'},{source_url:''}])assert.equal(check(o,{...r,...patch}).status,'unresolved');for(const patch of [{line:3},{market:'Q1_TOTAL'},{selection:'unknown'},{observed_at:o.kickoff}])assert.equal(check({...o,...patch}).status,'unresolved');});
test('NFL final ties are supported but never mapped across sports',()=>{assert.equal(check({...o,sport:'nfl'},{...r,sport:'nfl',home_score:20,away_score:20}).outcome,'win');});

test('same observation ID cannot transfer another book rule',()=>{assert.equal(check(o,r,{...c,book_key:'other-book'}).reason,'exact_contract_evidence_required');assert.equal(check().outcome,'win');});
test('missing or blank book identity never certifies a contract',()=>{for(const book_key of [undefined,null,'',' ']){assert.equal(check(o,r,{...c,book_key}).reason,'exact_contract_evidence_required');assert.equal(check({...o,book_key},r,{...c,book_key}).reason,'exact_contract_evidence_required');}});

const no={...o,sport:'nfl',game_id:'synthetic-nfl',home_team:'HOM',away_team:'AWY',selection:'Synthetic Away Club',line:10.5};
const nr={...r,sport:'nfl',game_id:no.game_id,home_team:no.home_team,away_team:no.away_team,home_score:31,away_score:24};
const nm={id:'synthetic-selection-review',review_status:'reviewed',sport:no.sport,game_id:no.game_id,observation_id:no.id,selection:no.selection,side:'away',team_key:no.away_team,reviewed_at:'2026-10-20T22:30:00Z',source_url:'https://example.invalid/synthetic-team-identity'};
const nc={...c,selection_mapping:nm};
test('NFL full display name needs explicit event-bound selection review',()=>{
 assert.equal(check(no,nr,c).reason,'unsupported_selection');
 const out=check(no,nr,nc);assert.equal(out.outcome,'win');assert.equal(out.selection_team_key,'AWY');assert.equal(out.selection_mapping_id,nm.id);assert.equal(out.selection_mapping_source,nm.source_url);assert.equal(out.stake,null);assert.equal(out.profit_loss,null);
 assert.equal(check({...no,line:6.5},nr,nc).outcome,'loss');
});
test('reviewed home orientation and exact canonical selection stay distinct',()=>{
 const home={...no,selection:'Synthetic Home Club',line:-10.5};
 const hc={...nc,selection_mapping:{...nm,selection:home.selection,side:'home',team_key:'HOM'}};
 assert.equal(check(home,nr,hc).outcome,'loss');
 assert.equal(check({...home,line:-6.5},nr,hc).outcome,'win');
 const direct=check({...no,selection:'AWY'},nr,c);assert.equal(direct.outcome,'win');assert.equal('selection_mapping_id' in direct,false);
});
test('selection evidence cannot cross event, sport, observation, text or side',()=>{
 for(const patch of [{id:''},{review_status:'conditional'},{sport:'nba'},{game_id:'other'},{observation_id:'other'},{selection:'different'},{side:'home'},{team_key:'HOM'},{side:'draw'},{source_url:''},{source_url:'http://example.invalid'},{reviewed_at:'bad'},{reviewed_at:'2026-10-20T18:00:00Z'},{reviewed_at:'2026-10-21T00:00:00Z'}])
  assert.equal(check(no,nr,{...nc,selection_mapping:{...nm,...patch}}).reason,'unsupported_selection',JSON.stringify(patch));
});
test('missing selection cannot be invented by a reviewed mapping',()=>{
 for(const selection of [undefined,null,'',' '])assert.equal(check({...no,selection},nr,{...nc,selection_mapping:{...nm,selection}}).reason,'unsupported_selection');
});
test('selection review does not bypass exact-book or final/period gates',()=>{
 assert.equal(check(no,nr,{...nc,book_key:'other'}).reason,'exact_contract_evidence_required');
 assert.equal(check(no,{...nr,status:'live'},nc).reason,'final_result_required');
 assert.equal(check(no,{...nr,includes_overtime:false},nc).reason,'result_period_mismatch');
 const nba={...no,sport:'nba'},nbar={...nr,sport:'nba'};assert.equal(check(nba,nbar,{...nc,selection_mapping:{...nm,sport:'nba'}}).reason,'unsupported_selection');
});
test('total selection remains exact and does not use team mapping',()=>{
 const total={...no,market:'FULL_GAME_TOTAL',selection:'Under',line:55.5};
 const out=check(total,nr,nc);assert.equal(out.outcome,'win');assert.equal('selection_mapping_id' in out,false);
 assert.equal(check({...total,selection:'Synthetic Away Club'},nr,nc).reason,'unsupported_selection');
});
