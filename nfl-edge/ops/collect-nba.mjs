// Bounded, opt-in request using existing provider credits. No recurring schedule.
import {Client} from 'pg';
import {createHash} from 'node:crypto';
import {pairedNbaQuotes} from '../lib/nba-feed.mjs';
import {fetchNbaSnapshot} from '../lib/nba-collection.mjs';
if(!process.argv.includes('--confirm-quota-use'))throw Error('Pass --confirm-quota-use to permit one spreads/totals request (normally 2 provider credits).');
if(!process.env.THE_ODDS_API_KEY||!process.env.NFL_EDGE_DATABASE_URL)throw Error('Server-side provider and database configuration required');
const {events,status,remaining_before}=await fetchNbaSnapshot({apiKey:process.env.THE_ODDS_API_KEY});
const now=Date.now(),quotes=pairedNbaQuotes(events).filter(q=>Date.parse(q.kickoff)>now&&Date.parse(q.kickoff)<now+8*86400000&&Date.parse(q.observed_at)<=now&&now-Date.parse(q.observed_at)<=900000);
const db=new Client({connectionString:process.env.NFL_EDGE_DATABASE_URL,ssl:{rejectUnauthorized:true},statement_timeout:8000});
try{await db.connect();await db.query('BEGIN');for(const q of quotes){const columns=Object.keys(q).filter(k=>k!=='sport'),values=columns.map(k=>q[k]),id=createHash('sha256').update(JSON.stringify(values)).digest('hex');await db.query(`INSERT INTO public.nba_edge_odds_quotes (quote_id,${columns.join(',')}) VALUES (${[id,...values].map((_,i)=>'$'+(i+1)).join(',')}) ON CONFLICT (quote_id) DO NOTHING`,[id,...values]);}await db.query('COMMIT');console.log(JSON.stringify({mode:'paper',status,remaining_before,quotes_processed:quotes.length,events:events.length}));}catch{await db.query('ROLLBACK').catch(()=>{});throw Error('NBA quote storage failed');}finally{await db.end();}
