import {mkdir,copyFile} from 'node:fs/promises';
await mkdir('dist',{recursive:true});
for(const f of ['index.html','app.js','core.mjs','style.css'])await copyFile(f,'dist/'+f);
await copyFile('ops/schedule-2026.json','dist/schedule-2026.json');
console.log('Built static application. Serverless handlers remain under api/.');
const {writeFile}=await import('node:fs/promises');
const {verifyOdds}=await import('./verify-odds.mjs');
const coverage=await verifyOdds(process.env.THE_ODDS_API_KEY);
await writeFile('dist/odds-check.json',JSON.stringify(coverage));
console.log('Odds coverage:',coverage.status,'Fresh supported pairs:',coverage.pairs?.length||0);

await copyFile('model/reports/2026-09-10-quarter-validation.json','dist/quarter-validation.json');
