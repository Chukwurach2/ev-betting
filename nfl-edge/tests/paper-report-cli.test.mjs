import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,readFile,writeFile,rm,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
const execute=promisify(execFile);
const script=fileURLToPath(new URL('../ops/report-paper-performance.mjs',import.meta.url));
const observation={id:'synthetic',kind:'observation',sport:'nba',game_id:'synthetic-game',home_team:'Home',away_team:'Away',kickoff:'2026-10-20T20:00:00Z',observed_at:'2026-10-20T18:00:00Z',market:'FULL_GAME_TOTAL',selection:'Over',line:210.5,book_key:'example',american_odds:-110};
async function fixture(t){
 const dir=await mkdtemp(join(tmpdir(),'paper-cli-'));
 t.after(()=>rm(dir,{recursive:true,force:true}));
 const ledger=join(dir,'ledger.jsonl');await writeFile(ledger,JSON.stringify(observation)+'\n',{mode:0o600});
 return {dir,ledger};
}
test('dash evidence sentinel preserves output and cost positions and exclusive private creation',async t=>{
 const {dir,ledger}=await fixture(t),output=join(dir,'audit.json');
 await execute(process.execPath,[script,ledger,'-',output,'.03']);
 const bytes=await readFile(output,'utf8'),report=JSON.parse(bytes);
 assert.equal(report.fee_per_settled_unit,.03);
 assert.equal(report.overall.entries,1);assert.equal(report.overall.resolved,0);assert.equal(report.overall.unresolved,1);
 assert.equal(report.rows[0].paper_profit_units,null);assert.equal(report.overall.hypothetical_roi,null);
 assert.equal((await stat(output)).mode&0o777,0o600);
 await assert.rejects(execute(process.execPath,[script,ledger,'-',output,'0']),e=>e.stderr.includes('EEXIST'));
 assert.equal(await readFile(output,'utf8'),bytes);
});
test('explicit evidence paths still grade while invalid costs cannot create a report',async t=>{
 const {dir,ledger}=await fixture(t),proof=join(dir,'proof.json'),output=join(dir,'graded.json');
 const result={...observation,status:'final',period:'full_game',includes_overtime:true,home_score:110,away_score:108,observed_at:'2026-10-20T23:00:00Z',source_url:'https://example.invalid/result'};
 await writeFile(proof,JSON.stringify({results:[result],contracts:[{book_key:observation.book_key,observation_id:observation.id,rule_key:'full_game_including_overtime',source_url:'https://example.invalid/rules'}]}),{mode:0o600});
 await execute(process.execPath,[script,ledger,proof,output,'0']);
 const report=JSON.parse(await readFile(output,'utf8'));
 assert.equal(report.overall.resolved,1);assert.equal(report.overall.wins,1);assert.equal(report.overall.hypothetical_net_units,100/110);
 const invalid=join(dir,'invalid.json');
 await assert.rejects(execute(process.execPath,[script,ledger,'-',invalid,'-1']),e=>e.stderr.includes('nonnegative'));
 await assert.rejects(stat(invalid),{code:'ENOENT'});
});
