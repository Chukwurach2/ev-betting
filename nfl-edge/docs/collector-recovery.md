# Fresh-price collector incident — 2026-10-19

## Verified evidence

The scheduled NFL collector failed in [run 39249776550](https://github.com/Chukwurach2/ev-betting/actions/runs/39249776550), job 118050441308, at 12:09:22 UTC. The secret-presence check, migrations and schedule sync passed. The free events request failed with HTTP 401 in model/provider_oddsapi.py before odds collection. A configured secret is not evidence of valid provider authentication. The response does not establish whether the key is expired, incorrect or otherwise unauthorized.

At the next database check, NFL's latest stored quote remained 2026-10-17T20:09:06Z with zero quotes in the 15-minute window; NBA's isolated quote table was empty. These are coverage gaps, not zero estimated edges. The NBA schedule had independently loaded through the Vercel runtime; that does not validate the GitHub worker's credential.

## Recovery

1. The account owner should verify the provider key in the provider dashboard and securely replace the repository Actions secret THE_ODDS_API_KEY under Settings → Secrets and variables → Actions. Never paste the key into an issue, chat, workflow log, source file or command argument. Do not assume Vercel environment updates also update GitHub Actions secrets.
2. Re-run the failed collector using its normal GitHub Actions control. Confirm the events request authenticates before attempting quota-consuming collection. Do not buy a new plan as an automatic remediation.
3. A successful run may collect no quotes when no checkpoint is due. Inspect capture times and normalized quotes, then GET /api/game-cards?sport=nfl; confirm actual fresh supported leans rather than treating a green workflow as delivery proof.
4. NBA has a separate bounded collector ops/collect-nba.mjs --confirm-quota-use and separate storage. It is not enabled by repairing the NFL workflow; wire it to a controlled runtime using existing server-side secrets and a declared quota budget. No continuous paid polling has been enabled.
5. Preserve the original source time and 15-minute expiry. Never redeploy just to refresh quotes, widen freshness, reuse NFL probabilities for NBA, or populate pending cards with old chat prices.

No credentials were read, copied or published during this diagnosis. No wagers, deposits, paid purchases or model promotion occurred. The immediate unblocking dependency is valid worker authentication; private original-research publication remains separate delivery work.
