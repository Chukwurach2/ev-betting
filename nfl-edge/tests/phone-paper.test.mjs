import test from 'node:test';import assert from 'node:assert/strict';import {paperRecords} from '../lib/phone-paper.mjs';
const now=Date.parse('2026-10-19T15:00:00Z');
const pick={market:'FULL_GAME_TOTAL',selection:'Over',line:51.5,book_key:'draftkings',american_odds:110,fair_probability:.5,reference_books:2,observed_at:'2026-10-19T14:59:00Z',expected_value:99};
const feed=(picks=[pick],sport='nfl')=>({mode:'paper',sport,games:[{game_id:'g',home_team:'A',away_team:'B',kickoff:'2026-10-20T00:15:00Z',picks}]});
test('paper observation recomputes EV and never claims accepted execution',()=>{const r=paperRecords(feed(),{now})[0];assert.ok(Math.abs(r.expected_value-.05)<1e-10);assert.equal(r.stake,null);assert.equal(r.accepted_price_verified,false);assert.equal(r.settlement_status,'unresolved');});
test('repeated quote has stable identity; revised fair value is new evidence',()=>{const r=paperRecords(feed(),{now})[0],later=paperRecords(feed(),{now:now+1000})[0];assert.equal(r.id,later.id);assert.notEqual(r.id,paperRecords(feed([{...pick,fair_probability:.51}]),{now})[0].id);});
test('stale future excluded venues and unsupported contracts are counted as rejected',()=>{for(const changes of [{observed_at:'2026-10-19T14:00:00Z'},{observed_at:'2026-10-19T15:01:00Z'},{book_key:'betrivers'},{book_key:'betonlineag'},{line:51},{fair_probability:null},{reference_books:1}]){const r=paperRecords(feed([{...pick,...changes}]),{now});assert.equal(r.length,1);assert.equal(r[0].rejected,1);assert.equal(r[0].shortfall,4);}});
test('empty games preserve coverage rather than invent a paper bet',()=>{const r=paperRecords(feed([]),{now});assert.equal(r[0].kind,'coverage');assert.equal(r[0].recorded,0);});
test('sport identities remain isolated and unsupported NBA quarter contracts rejected',()=>{assert.notEqual(paperRecords(feed(),{now})[0].id,paperRecords(feed([pick],'nba'),{now})[0].id);assert.equal(paperRecords(feed([{...pick,market:'Q1_TOTAL'}],'nba'),{now}).length,1);});
test('same selection at another venue is not another paper pick',()=>{const r=paperRecords(feed([pick,{...pick,book_key:'fanduel'}]),{now});assert.equal(r.filter(x=>x.kind==='observation').length,1);assert.equal(r.at(-1).rejected,1);});
test('bad game envelopes fail before any recording',()=>{assert.throws(()=>paperRecords({...feed(),mode:'live'},{now}));const f=feed();f.games[0].sport='nba';assert.throws(()=>paperRecords(f,{now}));});

const refs=()=>[{book_key:'fanduel',american_odds:-110,fair_probability:.5,observed_at:'2026-10-19T14:55:00Z'},{book_key:'betmgm',american_odds:-110,fair_probability:.5,observed_at:'2026-10-19T14:58:00Z'}];
test('preserves supporting source timestamps and oldest-reference expiry',()=>{
 const p={...pick,reference_quotes:refs(),expires_at:'2026-10-19T15:12:00Z'};
 const r=paperRecords(feed([p]),{now})[0];assert.equal(r.expires_at,'2026-10-19T15:10:00.000Z');assert.deepEqual(r.reference_quotes.map(x=>x.book_key),['fanduel','betmgm']);assert.equal(r.reference_freshness,'source_times_preserved');
});
test('expired explicit or supporting deadlines reject a fresh offer',()=>{
 for(const patch of [{expires_at:'2026-10-19T15:00:00Z'},{expires_at:'bad'},{reference_quotes:[{...refs()[0],observed_at:'2026-10-19T14:44:00Z'},refs()[1]]}])assert.equal(paperRecords(feed([{...pick,...patch}]),{now})[0].rejected,1);
});
test('duplicate aliased, offered, future and mismatched references fail closed',()=>{
 const cases=[[{...refs()[0],book_key:'draftkings'},refs()[1]],[{...refs()[0],book_key:'espnbet'},{...refs()[1],book_key:'thescorebet'}],[{...refs()[0],observed_at:'2026-10-19T15:01:00Z'},refs()[1]],refs().slice(0,1)];
 for(const reference_quotes of cases)assert.equal(paperRecords(feed([{...pick,reference_quotes}]),{now})[0].rejected,1);
});
test('legacy inputs explicitly disclose unknown reference freshness',()=>assert.equal(paperRecords(feed(),{now})[0].reference_freshness,'unknown_legacy_input'));
