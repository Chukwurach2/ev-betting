// Run only against the user's private database with a privileged backend role.
import {readFile} from 'node:fs/promises';
import {validateLean} from '../lib/private-leans.mjs';
import {insertPrivateLean} from './private-lean-insert.mjs';
const file=process.argv[2],owner=process.env.RESEARCH_OWNER_ID;
if(!file||!owner||!process.env.NFL_EDGE_DATABASE_URL)throw Error('Private file, owner ID and backend database configuration required');
const payloads=JSON.parse(await readFile(file,'utf8'));
if(!Array.isArray(payloads)||payloads.length>100)throw Error('Expected at most 100 original research entries');
for(const p of payloads)validateLean(p);
const {default:pg}=await import('pg');
const db=new pg.Client({connectionString:process.env.NFL_EDGE_DATABASE_URL});
try{
 await db.connect();await db.query('BEGIN');
 for(const p of payloads)await insertPrivateLean(db,owner,p);
 await db.query('COMMIT');console.log('Private research publication complete. No wagers created.');
}catch{try{await db.query('ROLLBACK');}catch{}process.exitCode=1;console.error('Private publication failed; no source payload or credentials logged.');}
finally{await db.end();}
