# Private curated lean delivery

Curated original analysis is delivered under NY Picks through the existing app
sign-in. It is separate from the public market feed, frozen model and personal
journal. Nothing here places bets or writes bankrolls. Sign in under My Bets on
the phone; the same account receives its private research on every device.

`research_leans` has forced row-level security. Authenticated clients can SELECT
only rows whose user_id matches auth.user_id(). Anonymous reads and all phone
writes are denied. Only a privileged backend publisher inserts original analysis.
No observations, account identifiers or subscriber tables belong in this repository.

Apply migrations/021_private_research_leans.sql after testing on an isolated
branch. Publish a private JSON array with ops/publish-private-leans.mjs FILE;
NFL_EDGE_DATABASE_URL and RESEARCH_OWNER_ID stay in the private environment.
The reusable validator is lib/private-leans.mjs. Publication is append-only and
idempotent by observation ID. A revised or invalidated observation gets a new ID
and supersedes an earlier matching selection in the phone's newest-first read.

Each entry requires exact sport/event/kickoff, contract/line/selection/rules,
permitted offer book and exact odds, minimum price, provider-derived or independent
FV label, rationale, method, conditions, correlation and fee assumptions. Include
known liquidity. Never transfer a different venue's favorable price to an offer.
`rights: private_original_analysis` is a publisher attestation, not a license;
review source permissions before delivery and omit raw subscriber tables.

Original quote times survive publication. Expiry cannot exceed 15 minutes after
the oldest known offered/reference quote; publication does not refresh it. An
unknown reference timestamp must be null and review_status conditional (or invalidated for a withdrawal). Those
entries explicitly show unverified reference freshness and cannot become observed.
A conditional entry also remains conditional until exact offer and availability
checks are complete. Expired/invalidated records are collapsed research history,
not current picks. No automated EVSharps ingestion or continuous monitoring is
claimed. The app refreshes private storage once per visible minute and on return.

Validation: nine freshness/venue/deduplication tests plus the existing Node suite.
Database branch checks verify owner A/B isolation, no anonymous SELECT privilege,
and no authenticated INSERT privilege. Subscriber payloads remain private.


Withdrawal repair (2026-10-20): invalidation accepts an unknown reference time,
so a publisher can withdraw conditional research without fabricating timestamps.
Newest-first supersession happens before applying sportsbook preferences. Hiding
a newer observation at an unselected book must never resurrect an older quote.
Regression cases cover withdrawals and newer expired observations across books.
