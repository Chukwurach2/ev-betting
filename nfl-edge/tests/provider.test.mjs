import {test} from 'node:test';
import assert from 'node:assert/strict';
import {pairedQuotes,verifyOdds} from '../ops/verify-odds.mjs';
const now=Date.parse('2026-09-09T16:00:00Z');
const market={key:'totals_q1',last_update:'2026-09-09T15:59:00Z',outcomes:[{name:'Over',point:7.5,price:-110},{name:'Under',point:7.5,price:-110}]};
const event={bookmakers:[{key:'draftkings',title:'DraftKings',markets:[market]}]};
test('same-book exact-line pairs de-vig consistently',()=>{const [p]=pairedQuotes(event,now);assert.equal(p.over_fair_probability,.5);assert.equal(p.under_fair_probability,.5);assert.equal(p.observed_at,market.last_update);});
test('unpaired, stale or mismatched lines are rejected',()=>{for(const m of [{...market,last_update:'2026-09-09T15:00:00Z'},{...market,outcomes:market.outcomes.slice(0,1)},{...market,outcomes:[market.outcomes[0],{...market.outcomes[1],point:8.5}]}])assert.equal(pairedQuotes({bookmakers:[{...event.bookmakers[0],markets:[m]}]},now).length,0);});
test('provider errors never reveal the API key',async()=>{const secret='fixture-secret';const r=await verifyOdds(secret,async()=>{throw Error('https://example.invalid?apiKey='+secret)});assert.equal(r.status,'unavailable');assert.ok(!JSON.stringify(r).includes(secret));});
