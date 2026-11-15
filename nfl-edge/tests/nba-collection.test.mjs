import test from 'node:test';import assert from 'node:assert/strict';import {fetchNbaSnapshot} from '../lib/nba-collection.mjs';
const now=Date.parse('2026-10-19T12:00:00Z'),event={id:'n',sport_key:'basketball_nba',home_team:'A',away_team:'B',commence_time:'2026-10-20T23:00:00Z'};
function fake({remaining='52',remainingAfter='50',charged='2',ok=true,events=[event],odds=events,failure=false}={}){
 const calls=[];
 return {calls,fetcher:async url=>{
  const isOdds=url.pathname.endsWith('/odds');
  calls.push(url.pathname);
  if(failure)throw Error('Do not leak secret-key-in-URL');
  const headers=new Headers();
  const balance=isOdds?remainingAfter:remaining;
  if(balance!==null)headers.set('x-requests-remaining',balance);
  if(isOdds&&charged!==null)headers.set('x-requests-last',charged);
  return {ok,headers,json:async()=>isOdds?odds:events};
 }};
}
test('free quota preflight precedes one odds request and preserves reserve',async()=>{const f=fake();const r=await fetchNbaSnapshot({...f,apiKey:'test',now});assert.equal(r.maximum_odds_requests,1);assert.equal(r.remaining_before,52);assert.equal(r.remaining_after,50);assert.equal(r.credits_charged,2);assert.ok(f.calls[0].endsWith('/events'));assert.ok(f.calls[1].endsWith('/odds'));assert.equal(f.calls.length,2);});
test('unknown malformed or insufficient quota makes zero odds requests',async()=>{for(const remaining of [null,'','-1','51','52.5','unknown']){const f=fake({remaining});await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),/quota/);assert.equal(f.calls.length,1);}});
test('authentication and transport failure stop before odds without leaking URL',async()=>{for(const options of [{ok:false},{failure:true}]){const f=fake(options);await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),e=>!e.message.includes('secret-key')&&e.message.includes('provider request failed'));assert.equal(f.calls.length,1);}});
test('no upcoming NBA games spends no odds credits',async()=>{const f=fake({events:[]});const r=await fetchNbaSnapshot({...f,apiKey:'test',now});assert.equal(r.maximum_odds_requests,0);assert.equal(f.calls.length,1);});
test('malformed events payload fails closed before odds',async()=>{const f=fake({events:{}});await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),/schedule invalid/);assert.equal(f.calls.length,1);});

test('rejects unverified or out-of-budget odds receipts without retry',async()=>{
 for (const options of [{remainingAfter:null},{remainingAfter:'49'},{remainingAfter:'50.5'},
                        {charged:null},{charged:'3'},{charged:'0'},{charged:'2.5'}]){
  const f=fake(options);
  await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),/odds quota receipt outside bounds/);
  assert.equal(f.calls.length,2);
 }
});
test('accepts a free empty response with verified reserve',async()=>{
 const f=fake({odds:[],charged:'0',remainingAfter:'52'});
 const r=await fetchNbaSnapshot({...f,apiKey:'test',now});
 assert.deepEqual(r.events,[]);
 assert.equal(r.credits_charged,0);
 assert.equal(r.remaining_after,52);
});
