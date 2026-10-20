import {escapeHtml as esc} from './core.mjs';
import {visibleLeans,LEAN_BOOKS} from './lib/private-leans.mjs';
const fmt=x=>new Date(x).toLocaleString('en-US',{timeZone:'America/New_York',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}),odds=x=>x>0?'+'+x:String(x);
export function privateResearch(client){
 const root=document.createElement('section');root.setAttribute('aria-label','Private research leans');document.getElementById('nyCards').before(root);
 let user=null,rows=[],epoch=0,sport='nfl',status='Sign in under My Bets to see your private research leans.';
 function render(){
  const chosen=[...document.querySelectorAll('#nyBooks :checked')].map(x=>x.value);
  const picks=visibleLeans(rows,{sport,books:chosen});
  const item=p=>`<article class="lean-card"><div class="lean-top"><span class="lean-label">${esc(p.state)} · paper research</span></div><h4>${esc(p.away_team)} at ${esc(p.home_team)}</h4><b>${esc(p.selection)} ${esc(p.line??'')} · ${esc(p.market)}</b><p>${esc(LEAN_BOOKS[p.book_key])} ${esc(odds(p.american_odds))} · Minimum ${esc(odds(p.minimum_odds))}</p><p class="small">Fair probability ${(p.fair_probability*100).toFixed(1)}% · ${p.fair_value_type==='provider_derived'?'Provider-derived FV':'Independent estimate'} · ${esc(p.source)}</p><p class="small muted">Observed ${esc(fmt(p.observed_at))} ET · Expires ${esc(fmt(p.expires_at))} ET</p><details><summary>Evidence and conditions</summary><p>${esc(p.rationale)}</p><p>${esc(p.method)}</p><p>Reference quote time: ${p.reference_at?esc(fmt(p.reference_at))+' ET':'unknown — freshness unverified; conditional price watch only'}.</p><p>${esc(p.conditions)}</p><p>Correlation: ${esc(p.correlation)}</p><p>Fees: ${esc(p.fee_assumptions)}. Limits / liquidity: ${esc(p.liquidity||'unknown')}.</p><p>Contract: ${esc(p.rules)}</p><p>Exact book offer ${p.offer_verified?'checked':'not independently checked'}; availability ${p.availability_checked?'checked':'still conditional'}. Individual alternative; no automatic wager.</p></details></article>`;
  const active=picks.filter(p=>['conditional','observed'].includes(p.state)),history=picks.filter(p=>!['conditional','observed'].includes(p.state));
  root.innerHTML=`<div class="section-title">Private research</div><p class="small muted" role="status">${esc(status)}</p>${active.map(item).join('')}${history.length?`<details><summary>${history.length} expired / invalidated research entries</summary>${history.map(item).join('')}</details>`:''}`;
 }
 async function refresh(){
  const id=++epoch,owner=user?.id;rows=[];render();if(!owner)return;
  try{
   const session=await client.auth.getSession();if(id!==epoch)return;
   if(session.error||session.data?.user?.id!==owner)throw Error();
   const r=await client.from('research_leans').select('payload').eq('user_id',owner).order('published_at',{ascending:false}).limit(100).abortSignal(AbortSignal.timeout(15000));
   if(id!==epoch)return;if(r.error)throw Error();rows=r.data||[];status=rows.length?'Private curated research · conditional entries need the checks shown below.':'No private research published yet.';
  }catch{if(id!==epoch)return;rows=[];status='Private research unavailable. Sign in again or try Refresh.';}
  render();
 }
 document.querySelectorAll('[data-picks-sport]').forEach(b=>b.addEventListener('click',()=>{sport=b.dataset.picksSport;render();}));
 document.getElementById('nyBooks').addEventListener('change',render);
 document.getElementById('nyRefresh').addEventListener('click',refresh);
 document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
 setInterval(()=>{if(!document.hidden)refresh();},60000);
 return {setUser(value){user=value;rows=[];status=user?'Loading private research…':'Sign in under My Bets to see your private research leans.';refresh();}};
}
