import test from 'node:test';import assert from 'node:assert/strict';import {fetchNbaSnapshot} from '../lib/nba-collection.mjs';
const now=Date.parse('2026-10-19T12:00:00Z'),event={id:'n',sport_key:'basketball_nba',home_team:'A',away_team:'B',commence_time:'2026-10-20T23:00:00Z'};
function fake({remaining='52',ok=true,events=[event],failure=false}={}){const calls=[];return {calls,fetcher:async url=>{calls.push(url.pathname);if(failure)throw Error('Do not leak secret-key-in-URL');return {ok,headers:new Headers(remaining===null?{}:{'x-requests-remaining':remaining}),json:async()=>events};}};}
test('free quota preflight precedes one odds request and preserves reserve',async()=>{const f=fake();const r=await fetchNbaSnapshot({...f,apiKey:'test',now});assert.equal(r.maximum_odds_requests,1);assert.equal(r.remaining_before,52);assert.ok(f.calls[0].endsWith('/events'));assert.ok(f.calls[1].endsWith('/odds'));assert.equal(f.calls.length,2);});
test('unknown malformed or insufficient quota makes zero odds requests',async()=>{for(const remaining of [null,'','-1','51','52.5','unknown']){const f=fake({remaining});await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),/quota/);assert.equal(f.calls.length,1);}});
test('authentication and transport failure stop before odds without leaking URL',async()=>{for(const options of [{ok:false},{failure:true}]){const f=fake(options);await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),e=>!e.message.includes('secret-key')&&e.message.includes('provider request failed'));assert.equal(f.calls.length,1);}});
test('no upcoming NBA games spends no odds credits',async()=>{const f=fake({events:[]});const r=await fetchNbaSnapshot({...f,apiKey:'test',now});assert.equal(r.maximum_odds_requests,0);assert.equal(f.calls.length,1);});
test('malformed events payload fails closed before odds',async()=>{const f=fake({events:{}});await assert.rejects(fetchNbaSnapshot({...f,apiKey:'test',now}),/schedule invalid/);assert.equal(f.calls.length,1);});

test('NBA request stays in one US region, two markets and selected eligible operators',async()=>{
 const requests=[];
 const fetcher=async url=>{requests.push(new URL(url));return {ok:true,headers:new Headers({'x-requests-remaining':'52'}),json:async()=>[event]};};
 await fetchNbaSnapshot({apiKey:'synthetic-only',fetcher,now});
 assert.equal(requests.length,2);
 const params=requests[1].searchParams;
 assert.equal(params.get('regions'),'us');
 assert.equal(params.get('markets'),'spreads,totals');
 assert.equal(params.get('oddsFormat'),'american');
 assert.deepEqual(new Set(params.get('bookmakers').split(',')),new Set(['draftkings','fanduel','betmgm','espnbet','williamhill_us','fanatics']));
});
test('unsafe integer, signed and raw whitespace quota values cannot authorize an odds request',async()=>{
 for(const remaining of ['9007199254740992','+52',' 52','52 ','5.2e1']){
  const f=fake({remaining});const rawFetcher=async url=>({...await f.fetcher(url),headers:{get:()=>remaining}});await assert.rejects(fetchNbaSnapshot({fetcher:rawFetcher,apiKey:'test',now}),/quota/);assert.equal(f.calls.length,1);
 }
});
test('failed charged request is not retried and does not expose credential URL',async()=>{
 let calls=0;
 const fetcher=async()=>{calls++;if(calls===2)throw Error('synthetic-key-in-provider-url');return {ok:true,headers:new Headers({'x-requests-remaining':'52'}),json:async()=>[event]};};
 await assert.rejects(fetchNbaSnapshot({apiKey:'synthetic-only',fetcher,now}),e=>e.message==='NBA provider request failed; no retry attempted'&&!e.message.includes('synthetic-key'));
 assert.equal(calls,2);
});

test('standard Headers normalization yields a known integer rather than an unknown raw value',async()=>{
 const f=fake({remaining:' 52 '});const r=await fetchNbaSnapshot({...f,apiKey:'test',now});
 assert.equal(r.remaining_before,52);assert.equal(f.calls.length,2);
});
