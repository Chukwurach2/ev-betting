import assert from 'node:assert/strict';
import test from 'node:test';

test('phone removes expired entries while a refresh is pending', async t => {
 const now=Date.parse('2026-11-15T19:00:00Z');
 t.mock.timers.enable({apis:['Date','setTimeout','setInterval'],now});
 const elements=new Map();
 const game={away_team:'A',home_team:'B',kickoff:new Date(now+2000).toISOString(),fresh_quotes:4,
  picks:[{market:'FULL_GAME_TOTAL',selection:'Over',line:45.5,book_key:'fanduel',
   sportsbook:'FanDuel',american_odds:-110,fair_probability:.51,expected_value:-.02,
   observed_at:new Date(now-120000).toISOString(),expires_at:new Date(now+1000).toISOString(),
   status:'Price watch',reference_books:2,rationale:'Paired reference prices',limitations:'Paper price'}]};
 const before=Object.fromEntries(['document','window','localStorage','fetch'].map(k=>[k,Object.getOwnPropertyDescriptor(globalThis,k)]));
 t.after(()=>{
  for(const [key,descriptor] of Object.entries(before)){
   if(descriptor)Object.defineProperty(globalThis,key,descriptor);
   else delete globalThis[key];
  }
 });
 const node=id=>{
  if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',addEventListener(){},
   replaceChildren(){this.innerHTML='';}});
  return elements.get(id);
 };
 globalThis.document={getElementById:node,querySelectorAll:()=>[],addEventListener(){}};
 globalThis.window={addEventListener(){}};
 globalThis.localStorage={getItem:()=>null};
 let requests=0;
 globalThis.fetch=async()=>{
  requests++;
  if(requests===1)return {ok:true,json:async()=>({games:[game]})};
  return new Promise(()=>{});
 };
 await import('../ny-picks.js');
 await new Promise(resolve=>setImmediate(resolve));
 assert.match(node('nyCards').innerHTML,/Price watch/);
 void node('nyRefresh').onclick();
 assert.equal(requests,2);
 t.mock.timers.tick(1000);
 assert.doesNotMatch(node('nyCards').innerHTML,/Price watch/);
 assert.match(node('nyCards').innerHTML,/No fresh supported prices/);
 assert.match(node('nySummary').textContent,/0 research leans/);
 t.mock.timers.tick(1000);
 assert.doesNotMatch(node('nyCards').innerHTML,/game-desk-header/);
 assert.match(node('nySummary').textContent,/0 games/);
 assert.equal(requests,2);
});
