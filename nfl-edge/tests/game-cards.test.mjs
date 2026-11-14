import test from 'node:test';import assert from 'node:assert/strict';import {buildGameCards} from '../lib/game-cards.mjs';
const now=Date.parse('2026-10-19T12:00:00Z'),game={game_id:'g',provider_event_id:'fixture-id',home_team:'SF',away_team:'WAS',kickoff:'2026-10-20T00:15:00Z'};
const quote=(book,price=-110,p=.55,overrides={})=>({...game,book_key:book,market:'FULL_GAME_TOTAL',selection:'Over',line:51.5,american_odds:price,fair_probability:p,observed_at:'2026-10-19T11:59:00Z',...overrides});
test('NY selection reprices instead of inheriting an unavailable best price',()=>{const qs=[quote('betonlineag',150),quote('betrivers',140),quote('fanduel',110),quote('draftkings',100),quote('pinnacle',-120)];const card=buildGameCards([game],qs,{now,offeredBooks:['draftkings']})[0];assert.equal(card.picks.length,1);assert.equal(card.picks[0].sportsbook,'DraftKings');assert.equal(card.picks[0].american_odds,100);assert.equal(card.shortfall,3);});
test('stale, future and wrong-line references cannot create an edge',()=>{for(const changes of [{observed_at:'2026-10-19T10:00:00Z'},{observed_at:'2026-10-19T12:01:00Z'},{line:52.5}]){const card=buildGameCards([game],[quote('fanduel'),quote('pinnacle'),quote('draftkings',-110,.55,changes)],{now})[0];assert.equal(card.picks.length,0);}});
test('exclude offered operator aliases from its reference set',()=>{const card=buildGameCards([game],[quote('espnbet'),quote('thescorebet'),quote('pinnacle')],{now})[0];assert.equal(card.picks.length,0);});
test('integer lines and missing probabilities do not produce leans',()=>{const qs=['fanduel','pinnacle','draftkings'].map(b=>quote(b,-110,.55,{line:51}));assert.equal(buildGameCards([game],qs,{now})[0].picks.length,0);});
test('deduplicate alternatives and preserve negative-edge watches',()=>{const qs=['fanduel','draftkings','pinnacle'].map(b=>quote(b,-150,.5));const card=buildGameCards([game],qs,{now})[0];assert.equal(card.picks.length,1);assert.equal(card.picks[0].status,'Price watch');assert.ok(card.picks[0].expected_value<0);});
test('maximum four exact selections and completed games excluded',()=>{const qs=Array.from({length:5},(_,i)=>['fanduel','pinnacle','draftkings'].map(b=>quote(b,110,.55,{line:50.5+i}))).flat();assert.equal(buildGameCards([game],qs,{now})[0].picks.length,4);assert.equal(buildGameCards([{...game,kickoff:'2026-10-18T00:00:00Z'}],qs,{now}).length,0);});
const {twoLegPlan}=await import('../lib/game-cards.mjs');
test('two-leg arb remains positive after cent rounding',()=>{const p=twoLegPlan({oddsA:110,oddsB:110,budget:100});assert.equal(p.stakeA,50);assert.equal(p.worst,5);});
test('middle both-win profit is conditional and has a probability hurdle',()=>{const p=twoLegPlan({oddsA:-110,oddsB:-110,budget:100});assert.ok(p.worst<0);assert.ok(p.both>0);assert.ok(Math.abs(p.middleBreakEven-.047619)<.00001);});
test('costs can eliminate an apparent arb',()=>{assert.ok(twoLegPlan({oddsA:110,oddsB:110,budget:100,cost:10}).worst<0);assert.throws(()=>twoLegPlan({oddsA:0,oddsB:110,budget:100}));});

test('lean expiry follows oldest supporting reference, not latest offered quote',()=>{const qs=[quote('fanduel',110,.5),quote('draftkings',-110,.52,{observed_at:'2026-10-19T11:46:00Z'}),quote('pinnacle',-110,.54)];const p=buildGameCards([game],qs,{now,offeredBooks:['fanduel']})[0].picks[0];assert.equal(p.expires_at,'2026-10-19T12:01:00.000Z');assert.equal(p.reference_quotes.length,2);assert.ok(p.reference_quotes.every(r=>r.book_key!=='fanduel'));assert.deepEqual(p.reference_probability_range,{min:.52,max:.54});});
test('expired supporting source cannot sustain a newly priced offer',()=>{const qs=[quote('fanduel',110,.5,{observed_at:'2026-10-19T12:00:00Z'}),quote('draftkings',-110,.52,{observed_at:'2026-10-19T11:46:00Z'}),quote('pinnacle',-110,.54)];assert.equal(buildGameCards([game],qs,{now:now+61000,offeredBooks:['fanduel']})[0].picks.length,0);});

test('numeric and string lines cannot count one reference operator twice',()=>{
 for(const sport of ['nfl','nba']){
  const current={...game,sport};
  const qs=[
   quote('fanduel',110,.5,{sport}),
   quote('espnbet',-110,.52,{sport,line:51.5}),
   quote('thescorebet',-110,.54,{sport,line:'51.5',observed_at:'2026-10-19T11:59:10Z'}),
  ];
  const opts={now,offeredBooks:['fanduel']};
  assert.equal(buildGameCards([current],qs,opts)[0].picks.length,0);
  qs.push(quote('pinnacle',-110,.56,{sport}));
  const [pick]=buildGameCards([current],qs,opts)[0].picks;
  assert.equal(pick.reference_books,2);
  const [score]=pick.reference_quotes.filter(r=>r.book_key==='thescorebet');
  assert.equal(score.fair_probability,.54);
  assert.equal(pick.fair_probability,.55);
 }
});

test('positive estimates that fail a reference omission stay conditional',()=>{
 for(const sport of ['nfl','nba']){
  const qs=[
   quote('fanduel',100,.53,{sport}),
   quote('draftkings',-110,.5,{sport}),
   quote('pinnacle',-110,.56,{sport}),
  ];
  const [pick]=buildGameCards([{...game,sport}],qs,{now,offeredBooks:['fanduel']})[0].picks;
  assert.ok(pick.expected_value>0);
  assert.equal(pick.status,'Reference-sensitive lean');
  assert.equal(pick.reference_omission_ev_range.min,0);
  assert.ok(pick.reference_omission_ev_range.max>0);
  qs[1].fair_probability=.51;
  const [robust]=buildGameCards([{...game,sport}],qs,{now,offeredBooks:['fanduel']})[0].picks;
  assert.equal(robust.status,'Exploratory edge');
  assert.ok(robust.reference_omission_ev_range.min>0);
 }
});
