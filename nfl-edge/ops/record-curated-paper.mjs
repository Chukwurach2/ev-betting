import {readFile,mkdir,writeFile} from 'node:fs/promises';import {dirname} from 'node:path';
import {curatedPaperRecords} from '../lib/curated-paper.mjs';
const [input,output]=process.argv.slice(2);
if(!input||!output)throw Error('Usage: node ops/record-curated-paper.mjs private-original-analysis.json runs/curated-YYYYMMDD.jsonl');
const rows=curatedPaperRecords(JSON.parse(await readFile(input,'utf8')));
await mkdir(dirname(output),{recursive:true,mode:0o700});
await writeFile(output,rows.map(x=>JSON.stringify(x)).join('\n')+(rows.length?'\n':''),{mode:0o600,flag:'wx'});
console.log(JSON.stringify({mode:'paper',recorded:rows.length,warning:'Private historical research only; no publication or wagers.'}));
