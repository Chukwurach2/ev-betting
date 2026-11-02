# NFL quota guard CI review — 2026-11-01

PR39's previous green PR workflow ran checkpoint tests, but did not include
tests.test_ncaaf_collector, which contains the new NFL quota-header checks.
The module name reflects its original college-isolation scope; the assertions
also cover NFL unknown, malformed and below-reserve quota, plus the52-credit
boundary. A passing checkpoint suite alone did not establish those checks ran
on GitHub.

Added that existing offline module to the focused PR regression command.
No additional provider request, secret, polling schedule, production behavior,
model or financial formula is introduced by this CI increment.

Local verification:18collector plus38checkpoint tests pass. Git blob hashes
of the worker and both test files match PR39 head
beb406954c1e0ea2321013cc6ada781b2828dde0 exactly. Hosted CI must pass at the
new head before merge-readiness can be claimed. This is not a deployed guard,
a current quote receipt or permission to use expired pregame prices.

The one-request cap and50-credit reserve remain per-run protections, not a
shared monthly budget or a lock shared with the NBA collector. No automatic
merge, provider rerun or deployment is part of this review.
