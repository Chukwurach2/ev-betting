import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {auditNbaSchedule} from '../ops/audit-nba-schedule.mjs';

const start='2026-11-17T00:00:00Z';
const official={home_team:'Cleveland Cavaliers',away_team:'Chicago Bulls',kickoff:start};
const feed=(games,sport='nba')=>({sport,games,generated_at:'2026-11-15T23:46:24Z'});

test('keeps the official tip separate when the provider time differs',()=>{
 const quote={...official,kickoff:'2026-11-17T00:10:00Z'};
 const r=auditNbaSchedule([official],feed([quote]));
 assert.equal(r.counts.shifted,1);
 assert.equal(r.games[0].provider_tip,quote.kickoff);
 assert.equal(r.games[0].official_tip,start);
});

test('rejects reversed, distant and duplicate candidates',()=>{
 const reversed={...official,home_team:official.away_team,away_team:official.home_team};
 const late={...official,kickoff:'2026-11-17T00:16:00Z'};
 assert.equal(auditNbaSchedule([official],feed([reversed,late])).counts.missing,1);
 assert.equal(auditNbaSchedule([official],feed([official,official])).counts.ambiguous,1);
 assert.equal(auditNbaSchedule([official],feed([official])).counts.matched,1);
});

test('flags listings between the official start and the later provider time',()=>{
 const provider={...official,kickoff:'2026-11-17T00:10:00Z',picks:[]};
 const nextGame={...official,kickoff:'2026-11-18T00:00:00Z'};
 const snapshot={...feed([provider,nextGame]),generated_at:start};
 const report=auditNbaSchedule([official,nextGame],snapshot);
 assert.equal(report.counts.matched,1);
 assert.equal(report.games.length,1);
 assert.deepEqual(report.listed_past_official_tip,[{
  home_team:official.home_team,away_team:official.away_team,
  official_tip:start,provider_tip:provider.kickoff,listed_entries:0
 }]);
 assert.deepEqual(auditNbaSchedule([official],{...snapshot,generated_at:provider.kickoff}).listed_past_official_tip,[]);
});

test('reports large same-day start disagreements without accepting another day or matchup',()=>{
 const late={...official,kickoff:'2026-11-17T01:40:00Z'};
 const early={...official,kickoff:'2026-11-16T23:40:00Z'};
 const rematch={...official,kickoff:'2026-11-18T00:05:00Z'};
 const reversed={...late,home_team:late.away_team,away_team:late.home_team};
 const report=auditNbaSchedule([official],feed([late,early,rematch,reversed]));
 assert.equal(report.counts.missing,1);
 assert.equal(report.games[0].provider_tip,null);
 assert.deepEqual(report.same_day_start_conflicts,[{
  home_team:official.home_team,away_team:official.away_team,official_tip:start,
  provider_listings:[
   {provider_tip:late.kickoff,delta_minutes:100},
   {provider_tip:early.kickoff,delta_minutes:-20}
  ]
 }]);
 assert.deepEqual(auditNbaSchedule([official],feed([official,late])).same_day_start_conflicts,[]);
});

test('rejects timezone-free tips and snapshots for another sport',()=>{
 assert.throws(()=>auditNbaSchedule([official],feed([official],'nfl')),/NBA phone snapshot/);
 assert.throws(()=>auditNbaSchedule([{...official,kickoff:start.slice(0,-1)}],feed([])),/reviewed schedule/);
});

test('CLI runs when invoked through a symlink',t=>{
 const root=mkdtempSync(join(tmpdir(),'nba-audit-test-'));
 t.after(()=>rmSync(root,{recursive:true,force:true}));
 const link=join(root,'audit.mjs'), schedule=join(root,'schedule.json'), snapshot=join(root,'snapshot.json');
 symlinkSync(fileURLToPath(new URL('../ops/audit-nba-schedule.mjs',import.meta.url)),link);
 writeFileSync(schedule,JSON.stringify([official]));
 writeFileSync(snapshot,JSON.stringify(feed([])));
 const result=JSON.parse(execFileSync(process.execPath,[link,schedule,snapshot],{encoding:'utf8'}));
 assert.equal(result.counts.missing,1);
});
