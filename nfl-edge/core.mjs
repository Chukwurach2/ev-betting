export const VERSION='2.5.0';
export const validOdds=o=>Number.isInteger(Number(o)) && (typeof o==='number'||typeof o==='string') && String(o).trim()!=='' && (Number(o)<=-100||Number(o)>=100);
export const decimal=o=>Number(o)>0?1+Number(o)/100:1+100/Math.abs(Number(o));
export const implied=o=>1/decimal(o);
export const escapeHtml=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export const probability=p=>(typeof p==='number'||typeof p==='string')&&String(p).trim()!==''&&Number.isFinite(Number(p))&&Number(p)>0&&Number(p)<1;
export function pickStatus(p,now=Date.now()) {
 const t=Date.parse(p.kickoff),q=Date.parse(p.observed_at),s=Date.parse(p.created_at);
 if(!Number.isFinite(t)||!Number.isFinite(s)||s>now)return 'Invalid timestamp';
 if(t<=now||p.game_status!=='scheduled')return 'Game started or unavailable';
 if(p.production_validated!==true&&p.production_validated!=='true')return 'Model not validated';
 if(![true,'true'].includes(p.inputs_ready)||!p.settlement_rules||!p.source_url||![true,'true'].includes(p.ny_licensed))return 'Inputs incomplete';
 if(p.fair_method!=='paired_same_book_median')return 'Fair price unverified';
 if(p.market==='Q1_TOTAL'&&(!Number.isFinite(Number(p.line))||Math.abs(Number(p.line)%1)!==.5))return 'Unsupported settlement';
 if(!Number.isFinite(q)||q>now||q>=t||now-q>15*60*1000||now-s>15*60*1000)return 'Quote expired';
 if(!validOdds(p.american_odds)||!probability(p.model_probability)||!probability(p.fair_market_probability))return 'Invalid pricing';
 if(p.qualifies!==true||Number(p.american_odds)<-150||Number(p.model_probability)-Number(p.fair_market_probability)<.03-1e-10||Number(p.model_probability)*decimal(p.american_odds)-1<.04-1e-10)return 'No qualifying edge';
 return 'Qualified';
}
export function latestPredictions(rows){const m=new Map();for(const p of rows){const k=JSON.stringify([p.game_id,p.market,p.team,p.selection,p.line,p.settlement_rules]);const old=m.get(k);if(!old||Date.parse(p.created_at)>Date.parse(old.created_at)||(p.created_at===old.created_at&&Number(p.id)>Number(old.id)))m.set(k,p);}return [...m.values()];}
export function profit(b){if(b.status==='win')return Number(b.stake)*(decimal(b.american_odds)-1);if(b.status==='loss')return -Number(b.stake);return 0;}
export function rawClv(b){return validOdds(b.closing_american_odds)&&validOdds(b.american_odds)?100*(implied(b.closing_american_odds)-implied(b.american_odds)):null;}
export function validateBet(b){if(!b.game_label?.trim()||!b.selection?.trim()||!b.market||!b.sportsbook)throw Error('Enter game, market, selection and sportsbook.');if(!validOdds(b.american_odds))throw Error('American odds must be -100 or lower, or +100 or higher.');if(b.line!=null&&!Number.isFinite(Number(b.line)))throw Error('Line must be a finite number.');if(b.status&&!['open','win','loss','push','void'].includes(b.status))throw Error('Invalid result status.');if(!Number.isFinite(Number(b.stake))||Number(b.stake)<=0)throw Error('Stake must be greater than zero.');if(b.closing_american_odds!=null&&!validOdds(b.closing_american_odds))throw Error('Enter valid closing odds or leave blank.');}
export function parseJournal(raw){try{const x=JSON.parse(raw);if(!x||!Array.isArray(x.bets))throw Error();return {bankroll:Math.max(0,Number(x.bankroll)||0),bets:x.bets,queue:Array.isArray(x.queue)?x.queue:[]};}catch{return {bankroll:0,bets:[],queue:[]};}}
export function legacyBet(b){return {client_id:crypto.randomUUID(),game_label:b.game||b.game_label,market:b.market,selection:b.selection,sportsbook:b.book||b.sportsbook,american_odds:Number(b.odds??b.american_odds),stake:Number(b.stake),placed_at:b.placedAt||b.placed_at||new Date().toISOString(),status:({Win:'win',Loss:'loss',Push:'push',Open:'open'})[b.result]||b.status||'open',closing_american_odds:b.closeOdds===''||b.closeOdds==null?null:Number(b.closeOdds),notes:JSON.stringify({source:'legacy-import',grade:b.grade||null})};}
