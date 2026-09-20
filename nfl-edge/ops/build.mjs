import {mkdir,copyFile} from 'node:fs/promises';
await mkdir('dist',{recursive:true});
for(const f of ['index.html','app.js','core.mjs','style.css'])await copyFile(f,'dist/'+f);
await copyFile('ops/schedule-2026.json','dist/schedule-2026.json');
console.log('Built static application. Serverless handlers remain under api/.');
const {writeFile}=await import('node:fs/promises');
const {verifyOdds}=await import('./verify-odds.mjs');
// Only an explicitly identified Vercel production build may spend credits.
// Preview, development, custom and unknown environments fail closed.
const coverage=process.env.VERCEL_ENV === 'production'
  ? await verifyOdds(process.env.THE_ODDS_API_KEY)
  : {status:'skipped_non_production',quotes_verified:false,automatic_picks:false,maximum_paid_requests:0};
await writeFile('dist/odds-check.json',JSON.stringify(coverage));
console.log('Odds coverage:',coverage.status,'Fresh supported pairs:',coverage.pairs?.length||0);

await copyFile('model/reports/2026-09-10-quarter-validation.json','dist/quarter-validation.json');
