// Offline, append-only research ledger. No sportsbook or database writes.
import {readFile,mkdir,open,unlink} from 'node:fs/promises';
import {dirname} from 'node:path';
import {paperRecords} from '../lib/phone-paper.mjs';
const input=process.argv[2];
if(!input)throw Error('Usage: node ops/record-phone-paper.mjs private-feed.json [ledger.jsonl]. Default output: ignored runs/phone-paper.jsonl');
const output=process.argv[3]||'runs/phone-paper.jsonl';
const records=paperRecords(JSON.parse(await readFile(input,'utf8')));
await mkdir(dirname(output),{recursive:true,mode:0o700});
const lock=output+'.lock';let handle;
try{
 handle=await open(lock,'wx',0o600);
 let prior='';try{prior=await readFile(output,'utf8');}catch(e){if(e.code!=='ENOENT')throw e;}
 const ids=new Set(prior.split('\n').filter(Boolean).map(line=>JSON.parse(line).id));
 const fresh=records.filter(r=>{if(ids.has(r.id))return false;ids.add(r.id);return true;});
 const ledger=await open(output,'a',0o600);try{if(fresh.length)await ledger.writeFile(fresh.map(r=>JSON.stringify(r)).join('\n')+'\n');}finally{await ledger.close();}
 console.log(JSON.stringify({mode:'paper',appended:fresh.length,observations:fresh.filter(r=>r.kind==='observation').length,coverage_records:fresh.filter(r=>r.kind==='coverage').length}));
}finally{if(handle){await handle.close();await unlink(lock);}}
