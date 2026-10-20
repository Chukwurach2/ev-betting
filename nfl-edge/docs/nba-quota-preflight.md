# NBA bounded collection quota check

The opt-in collector now calls the documented free NBA events endpoint before
its single odds request. Failed authentication, unavailable transport, malformed
schedule or missing/noninteger quota headers stop collection before odds. No
upcoming NBA events within eight days means no odds request. The six-book,
two-market request budgets two credits; reported remaining credits must leave
at least50 after that request. No automatic retry or recurring polling exists.

This is a per-run check, not a shared atomic budget reservation. Other collectors
using the same key can spend concurrently. Existing NFL/college/prop workflows
have separate policies and are not modified here. Those older configurations
must not be assumed suitable for a free monthly plan. No paid tier is required
or purchased by this change. Keys/URLs are never printed, including exceptions.

Run remains explicitly opt-in:
`node ops/collect-nba.mjs --confirm-quota-use`
with existing server-side THE_ODDS_API_KEY and NFL_EDGE_DATABASE_URL. Do not
run it merely to test the guard: mocks cover that path without spending credits.
The current change does not activate NBA collection or certify any NBA strategy.

Provider contract: https://the-odds-api.com/liveapi/guides/v4/
Focused verification: ten NBA tests pass, covering quota boundaries, unknown
quota, authentication/transport failure, no upcoming events, malformed schedule,
paired prices, exact lines and sport isolation. No provider credits were used.
