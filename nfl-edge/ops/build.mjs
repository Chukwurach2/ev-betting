import {mkdir,copyFile} from 'node:fs/promises';
await mkdir('dist',{recursive:true});
for(const f of ['index.html','app.js','core.mjs','style.css'])await copyFile(f,'dist/'+f);
await copyFile('ops/schedule-2026.json','dist/schedule-2026.json');
console.log('Built static application. Serverless handlers remain under api/.');
