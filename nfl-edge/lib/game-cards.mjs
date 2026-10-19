// Exploratory presentation layer. Does not change the frozen research engine.
export const NY_BOOKS = Object.freeze({fanduel:'FanDuel',draftkings:'DraftKings',betmgm:'BetMGM',williamhill_us:'Caesars',caesars:'Caesars',fanatics:'Fanatics',thescorebet:'theScore Bet',espnbet:'theScore Bet',ballybet:'Bally Bet'});
export const normalizeBook = key => ({espnbet:'thescorebet',caesars:'williamhill_us'}[key] || key);
const median=xs=>{const a=[...xs].sort((a,b)=>a-b),m=Math.floor(a.length/2);return a.length%2?a[m]:(a[m-1]+a[m])/2;};
const dec=x=>x>0?1+x/100:1+100/Math.abs(x);
const valid=x=>Number.isFinite(Number(x))&&(Number(x)>=100||Number(x)<=-100);
export function buildGameCards(games,quotes,{now=Date.now(),blocked=['betrivers'],offeredBooks=null,maxAge=15*60000}={}){
 const allowed=q=>NY_BOOKS[q.book_key]&&!blocked.includes(q.book_key)&&(!offeredBooks||offeredBooks.includes(normalizeBook(q.book_key)));
 return games.filter(g=>Date.parse(g.kickoff)>now).map(g=>{
  const raw=quotes.filter(q=>(q.sport||'nfl')===(g.sport||'nfl')&&q.home_team===g.home_team&&q.away_team===g.away_team&&Date.parse(q.kickoff)===Date.parse(g.kickoff));
  const fresh=raw.filter(q=>{const age=now-Date.parse(q.observed_at);return Number.isFinite(age)&&age>=0&&age<=maxAge&&valid(q.american_odds)&&Number(q.fair_probability)>0&&Number(q.fair_probability)<1;});
  const latest=new Map();for(const q of fresh){const k=JSON.stringify([q.market,q.selection,q.line,normalizeBook(q.book_key)]),old=latest.get(k);if(!old||Date.parse(q.observed_at)>Date.parse(old.observed_at))latest.set(k,q);}
  const candidates=[];
  for(const q of latest.values()){
   // Only priced half-point spread/total contracts: avoid silently treating pushes as losses.
   if(!allowed(q)||!/(?:SPREAD|TOTAL)$/.test(q.market)||q.line==null||Math.abs(Number(q.line)%1)!==.5)continue;
   const refs=[...latest.values()].filter(r=>r.market===q.market&&r.selection===q.selection&&Number(r.line)===Number(q.line)&&normalizeBook(r.book_key)!==normalizeBook(q.book_key));
   if(refs.length<2)continue;
   const fair=median(refs.map(r=>Number(r.fair_probability))),price=Number(q.american_odds),roi=fair*dec(price)-1;
   candidates.push({market:q.market,selection:q.selection,line:Number(q.line),sportsbook:NY_BOOKS[q.book_key],book_key:q.book_key,american_odds:price,fair_probability:fair,expected_value:roi,break_even_decimal:1/fair,observed_at:q.observed_at,reference_books:refs.length,status:roi>0?'Exploratory edge':'Price watch',rationale:`Median stored fair probability from ${refs.length} other books on this exact selection and line. Offered operator excluded.`,limitations:'Market estimate, not a validated model. No fee or tax adjustment. Limits and accepted price unverified.'});
  }
  candidates.sort((a,b)=>b.expected_value-a.expected_value);
  const picks=[],seen=new Set();for(const p of candidates){const k=JSON.stringify([p.market,p.selection,p.line]);if(seen.has(k))continue;seen.add(k);picks.push(p);if(picks.length===4)break;}
  return {...g,picks,coverage_target:4,shortfall:4-picks.length,stored_quotes:raw.length,fresh_quotes:fresh.length};
 });
}
export function twoLegPlan({oddsA,oddsB,budget,cost=0}){
 if(!valid(oddsA)||!valid(oddsB)||!Number.isFinite(budget)||budget<=0||!Number.isFinite(cost)||cost<0||cost>=budget)throw Error('Enter valid odds, a positive budget, and costs below the budget.');
 const a=dec(Number(oddsA)),b=dec(Number(oddsB)),cash=budget-cost;
 const stakeA=Math.floor(cash*b/(a+b)*100)/100,stakeB=Math.floor((cash-stakeA)*100)/100;
 const spent=stakeA+stakeB+cost,profitA=stakeA*a-spent,profitB=stakeB*b-spent,both=stakeA*a+stakeB*b-spent,worst=Math.min(profitA,profitB);
 return {stakeA,stakeB,spent,profitA,profitB,worst,both,middleBreakEven:worst>=0?0:-worst/(both-worst)};
}
