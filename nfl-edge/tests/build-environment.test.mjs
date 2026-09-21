import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const source = readFileSync(new URL('../ops/build.mjs', import.meta.url), 'utf8');
const expression = source.match(/const coverage=([\s\S]*?);\nawait writeFile/)[1];
const run = new (Object.getPrototypeOf(async function() {}).constructor)(
  'process', 'verifyOdds', `return ${expression};`);

test('preview and undetermined environments never call the paid check', async () => {
  for (const value of ['preview', 'development', '', undefined, 'Production', 'custom']) {
    let calls = 0;
    const result = await run({env:{VERCEL_ENV:value,THE_ODDS_API_KEY:'fixture'}}, async () => { calls++; });
    assert.equal(calls, 0);
    assert.equal(result.maximum_paid_requests, 0);
    assert.equal(result.quotes_verified, false);
  }
});

test('explicit production retains the exact key and provider result', async () => {
  const receipt = {status:'fixture',pairs:[]};
  let calls = 0;
  const result = await run({env:{VERCEL_ENV:'production',THE_ODDS_API_KEY:'fixture'}}, async key => {
    calls++;
    assert.equal(key, 'fixture');
    return receipt;
  });
  assert.equal(calls, 1);
  assert.equal(result, receipt);
});
