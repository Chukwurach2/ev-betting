import test from 'node:test';
import assert from 'node:assert/strict';
import {validateLean,leanState,visibleLeans} from '../lib/private-leans.mjs';
const now=Date.parse('2026-10-20T14:00:00Z');
const fixture=()=>({id:'synthetic',game_id:'synthetic-nba',sport:'nba',away_team:'A',home_team:'B',kickoff:'2026-10-20T19:00:00Z',selection:'Over',market:'FULL_GAME_TOTAL',line:210.5,book_key:'draftkings',american_odds:-110,minimum_odds:-105,fair_probability:.53,fair_value_type:'provider_derived',observed_at:'2026-10-20T13:59:00Z',reference_at:'2026-10-20T13:58:00Z',expires_at:'2026-10-20T14:13:00Z',rights:'private_original_analysis',review_status:'conditional',source:'Synthetic fixture',method:'Synthetic estimate',rationale:'Test only',conditions:'Availability unknown',correlation:'Same game',fee_assumptions:'Zero direct fee; tax excluded',rules:'Full game including OT'});
test('conditional remains conditional without independent checks',()=>{assert.equal(leanState(fixture(),now),'conditional');assert.equal(leanState({...fixture(),review_status:'observed'},now),'conditional');});
test('age, future times, expiry and kickoff fail closed',()=>{assert.equal(leanState(fixture(),now+14*60000),'expired');assert.equal(leanState({...fixture(),observed_at:'2026-10-20T14:01:00Z'},now),'unavailable');assert.equal(leanState({...fixture(),kickoff:'2026-10-20T13:00:00Z'},now),'expired');assert.throws(()=>validateLean({...fixture(),expires_at:'2026-10-20T14:14:00Z'}));});
test('venue, sport, rights and provenance required',()=>{for(const patch of [{book_key:'betonline'},{sport:'ncaaf'},{rights:'public'},{reference_at:''},{conditions:''}])assert.throws(()=>validateLean({...fixture(),...patch}));});
test('sport and book filtering with duplicate selection suppression',()=>{const rows=[fixture(),{...fixture(),id:'second',book_key:'fanduel'},{...fixture(),id:'football',sport:'nfl'}].map(payload=>({payload}));assert.equal(visibleLeans(rows,{sport:'nba',books:['draftkings','fanduel'],now}).length,1);assert.equal(visibleLeans(rows,{sport:'nba',books:[],now}).length,0);});
test('invalidated never becomes an active observation',()=>assert.equal(leanState({...fixture(),review_status:'invalidated'},now),'invalidated'));

test('unknown reference freshness is explicitly conditional',()=>{assert.equal(leanState({...fixture(),reference_at:null},now),'conditional');assert.throws(()=>validateLean({...fixture(),reference_at:null,review_status:'observed'}));});

test('withdrawal with unknown reference age is valid and supersedes its prior lean',()=>{
 const prior=fixture(),withdrawal={...prior,id:'withdrawn',reference_at:null,review_status:'invalidated'};
 assert.equal(leanState(withdrawal,now),'invalidated');
 const rows=visibleLeans([withdrawal,prior].map(payload=>({payload})),{sport:'nba',books:['draftkings'],now});
 assert.equal(rows.length,1);assert.equal(rows[0].state,'invalidated');
});
test('book preference cannot resurrect a superseded quote',()=>{
 const prior=fixture(),withdrawal={...prior,id:'withdrawn',book_key:'fanduel',review_status:'invalidated'};
 assert.deepEqual(visibleLeans([withdrawal,prior].map(payload=>({payload})),{sport:'nba',books:['draftkings'],now}),[]);
});
test('newer expired research cannot expose an older still-fresh quote',()=>{
 const prior=fixture(),latest={...prior,id:'expired',book_key:'fanduel',expires_at:'2026-10-20T13:59:00Z'};
 assert.deepEqual(visibleLeans([latest,prior].map(payload=>({payload})),{sport:'nba',books:['draftkings'],now}),[]);
});

test('observed state requires explicit boolean verification of offer and availability',()=>{
 const verified={...fixture(),review_status:'observed',offer_verified:true,availability_checked:true};
 assert.equal(leanState(verified,now),'observed');
 for(const field of ['offer_verified','availability_checked']){
  for(const value of ['false',1,{},null]){
   assert.equal(leanState({...verified,[field]:value},now),'conditional');
  }
 }
});
test('inherited object keys are not authorized offer venues',()=>{
 for(const book_key of ['constructor','__proto__','toString']){
  assert.throws(()=>validateLean({...fixture(),book_key}),/venue/);
 }
});

test('publication only accepts an exact retry after an ID conflict',async()=>{
 const {insertPrivateLean}=await import('../ops/private-lean-insert.mjs');
 const payload=fixture();
 for(const [insertCount,matchCount] of [[1,0],[0,1],[0,0]]){
  const calls=[];
  const db={query:async(sql,params)=>{
   calls.push({sql,params});
   return {rowCount:calls.length===1?insertCount:matchCount};
  }};
  const publish=insertPrivateLean(db,'synthetic-owner',payload);
  if(!insertCount&&!matchCount)await assert.rejects(publish,/already in use/);
  else await publish;
  assert.equal(calls.length,insertCount?1:2);
  if(!insertCount){
   assert.match(calls[1].sql,/id=\$1 AND user_id=\$2 AND payload=\$3::jsonb/);
   assert.deepEqual(calls[1].params,[payload.id,'synthetic-owner',JSON.stringify(payload)]);
  }
 }
});
