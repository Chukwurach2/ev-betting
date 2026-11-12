import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

test('phone SQL discards future and expired quotes before choosing the latest row',()=>{
 const source=readFileSync(new URL('../api/game-cards.js',import.meta.url),'utf8');
 const quoteQuery=source.match(/const quotes=\(await client\.query\(`([^`]+)`\)/)?.[1];
 assert.ok(quoteQuery);
 assert.match(quoteQuery,/SELECT DISTINCT ON\s*\(/);
 const where=quoteQuery.match(/\bWHERE\b([\s\S]+)\bORDER BY\b/)?.[1];
 assert.ok(where);
 assert.match(where,/observed_at\s*>\s*now\(\)\s*-\s*interval\s*'15 minutes'/);
 assert.match(where,/observed_at\s*<=\s*now\(\)/);
 assert.match(quoteQuery,/ORDER BY[\s\S]+observed_at DESC/);
});
