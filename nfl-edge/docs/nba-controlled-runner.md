# Controlled NBA phone runner

GitHub Actions workflow `NBA bounded phone quote collection` connects the existing
quota-checked collector to existing Actions secrets. It has **no schedule, push
trigger or PR trigger**. Run workflow on main and explicitly select
confirm_quota_use to permit one spreads/totals request, budgeting two credits.
Leaving confirmation false or selecting another branch skips the job.

The workflow serializes its own runs, reads the exact workflow commit without
resolving unrelated submodules, installs runtime dependencies with lifecycle scripts disabled, runs offline NBA
tests, and then executes the collector with server-side secrets. The collector's
free-events preflight must authenticate and report enough credits to leave50.
It does not purchase credits, retry requests or activate continuous polling.
This lock is not a quota reservation against NFL/college/prop workers.

Migration020_nba_phone_quotes.sql must already be applied; it was installed in
the initial NBA phone release. No migrations or personal journal writes occur
in this runner. The final step checks the actual public NBA card response and
reports fresh lean counts, including an explicit no-supported-cards result.
Provider quote age and current reference support still govern availability.

Do not call the pipeline live until a real run has completed and fresh quotes
and phone cards are verified. This workflow is only a manual controlled path;
private original-research/player-prop publication remains separate work.

## First controlled capture receipt — 2026-10-20

PR32 merged as 0a29cce3460e3a9969cf861d84f8bae2159000ef. Manual workflow
[39457313603](https://github.com/Chukwurach2/ev-betting/actions/runs/39457313603)
completed successfully (job118747459750). Ten offline NBA checks passed before
collection. The free-events preflight reported346 remaining credits, then the
single bounded spreads/totals request processed268 paired quote rows across46
provider events. This is a two-credit budget, not a verified remaining balance
after concurrent workers. No recurring polling was enabled.

Independent storage read found268 rows with latest source time16:25:01Z.
The actual public phone response returned18 games and54 distinct research
entries. All54 were negative-EV price watches under the current market estimate;
none was a positive edge. Today's first three cards had2,4,2 entries, retaining
shortfalls rather than inventing prices. Source/reference expiry was approximately
16:39:38Z; this capture does not establish ongoing coverage.

The private offline paper recorder appended54 observations and18 coverage
records from the actual response. Stake and settlement remain unknown. Detailed
quotes and ledger rows stay in ignored runs/. The authenticated curated feed
remains separate and its signed-in delivery verification is pending.

## Pre-tip capture receipt — 2026-10-20

Manual run[39476988595](https://github.com/Chukwurach2/ev-betting/actions/runs/39476988595),
job118815399252, completed successfully. The free preflight reported344 credits
before one bounded request.268 quote rows were processed; the phone returned60
research entries across18 games. Today's three games each had4 entries. The
source/reference deadline for those cards was18:56:10Z; this is another snapshot,
not continuous monitoring. Ten offline checks again passed before collection.

The actual response produced60 private paper observations and18 coverage records
with the source-provenance-aware recorder. These are new quote observations,
not60 independent wagers. No fills or outcomes are inferred. Exactly one entry
across the full slate had slightly positive estimated EV (about0.37% before fees).
Its two reference estimates straddled its break-even probability, so no robust
positive edge or ready bet was established. All entries for today's games were
price watches. Current injury clearance and accepted limits remain unverified.

Source cross-checks remain private. The external shortlist contained unavailable
venues and an event-time discrepancy; no favorable external EV was transferred
to a different offer or asserted as independently validated. Signed-in curated
phone verification remains separate from the public market cards.
