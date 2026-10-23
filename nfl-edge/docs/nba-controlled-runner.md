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

## Evening pregame capture — 2026-10-20

Run[39501340810](https://github.com/Chukwurach2/ev-betting/actions/runs/39501340810)
(job118899324388) completed on the current main code. Preflight reported342
credits before one bounded request, with the existing50-credit reserve enforced.
250 quote rows were processed; independent storage read found786 total rows and
latest source21:33:43Z. The actual phone response returned17 upcoming games and46
entries. The two remaining games tonight each had4 entries, all negative-EV price
watches. No positive estimate was found in the full returned slate. Earliest
supporting expiry for tonight's cards was21:48:12Z. There is no continuous service.

The private source-aware recorder appended46 observations and17 coverage records.
Repeated game/price captures remain dependent, with no accepted wagers, stakes,
fees, outcome or return inferred. The official result page did not provide a
verified current final through the available read, so prior observations remain
unresolved. A refreshed external shortlist offered no supported NBA main-line
candidate at review. No stale source observation was relabeled as a current pick.

Validation:21 focused collector/adapter/ledger tests passed locally; the runner's
10 offline NBA tests passed before its request. Documentation receipts contain
aggregate counts only; quote payloads and ledger rows remain private/ignored.

## Final PHI/NY pregame snapshot — 2026-10-20

Run[39511851302](https://github.com/Chukwurach2/ev-betting/actions/runs/39511851302),
job118934555370, completed successfully. Free preflight reported340 remaining
credits before one bounded request;250 rows were processed across45 provider
events. Actual phone GET returned17 games/48 research entries, with4 entries for
each remaining game tonight. All48 were negative-EV price watches; no positive
edge was created to satisfy coverage. Source/reference expiry was23:12:21Z, with
game-start removal also applicable. The app visibly confirmed48 entries at22:59Z.

48 source-aware private paper observations and17 coverage records were appended.
There are no accepted wagers or settlement/profit claims. The external supported
NBA main-line view provided no qualifying candidate at review, and the earlier
curated watch remains expired.21 focused collector/adapter/ledger tests passed.
No new code deployment or continuous polling was enabled. Timing depends on
runner and source availability; a requested pregame capture is not a guaranteed
real-time alert or exact closing-line observation.

## October 21 midday capture

[Run 39629525998](https://github.com/Chukwurach2/ev-betting/actions/runs/39629525998) completed successfully. Free-events preflight reported 334 credits before the single bounded odds request; 224 quote rows processed from 43 provider events. The phone GET and rendered NBA view both showed 15 upcoming games and 48 fresh entries at 11:52 AM ET. All 11 games on the current local-date slate had four entries; none had a positive point estimate. Treat them as individual price watches, not an edge or bet-all strategy.

Latest offered source time was 15:51:50Z; earliest expiry 16:06:23Z. These are a temporary snapshot, not continuous coverage. Saved 48 observations and 15 coverage records in the private ignored ledger. No code deployment, changed freshness limits, new subscription or wagers. Current EVSharps Recommendations session displayed its sign-in gate; no subscriber access or data freshness assumed.

## October 21 pregame capture and verification gap

[Run39686651952](https://github.com/Chukwurach2/ev-betting/actions/runs/39686651952) succeeded at22:28UTC. Free-events preflight reported332 credits before the one bounded odds request;224 quote rows processed from43 provider events. Its immediate phone check reported48 current entries across15 games at22:28:16Z.

The independent follow-up GET was generated at23:03:02.967Z and returned zero entries; the snapshot had expired by verification. Thus this run does not establish current rendered-phone delivery. Only15 no-price coverage records were appended locally; no48-observation capture is claimed. Do not refill from stale quotes, backdate fresh observations or widen freshness. No repeat odds call or deployment was made. EVSharps remained behind its sign-in gate. Next capture should retain private observations during the valid window rather than relying on delayed interactive verification.

## Midday snapshot — 2026-10-22

Run [39809804807](https://github.com/Chukwurach2/ev-betting/actions/runs/39809804807),
job119932685665, passed all10 offline checks and completed one bounded NBA
spreads/totals request. Free preflight reported326 remaining before the request;
this is not a verified post-request balance.224 quote rows across41 provider
events were processed. Actual independent phone GET and rendered UI confirmed
14 games /40 fresh entries at11:59AM ET, including four distinct alternatives
for each of tonight's two games. All40 were negative estimated-EV price watches.
The private recorder appended40 observations and14 coverage rows, bringing
observations to296. Earlier snapshots and losses remain untouched.

Detailed versioned cards preserve exact offered and reference book prices/times,
break-even thresholds, zero-fixed-fee assumption, unknown size/limits and
correlation flags privately. Earliest supporting expiry for tonight was16:11:29Z;
no continuous monitoring or accepted execution is asserted. Official11:45AM ET
injury report was checked; questionable players remain conditional. Its scheduled
7PM/9:30PM starts differ from provider7:10PM/9:40PM. Both are retained unchanged;
explicit event/start mapping review is required before outcome or pilot linkage.
Sources: https://official.nba.com/nba-injury-report-2026-27-season/ and
https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-22_11_45AM.pdf

NFL was checked separately:15 upcoming games, zero fresh supported phone entries,
including tonight's game. Official team schedule/injury report confirms tonight;
no NFL odds were invented or NBA probabilities transferred. EVSharps Recommended
remains behind the already documented sign-in gate. Public delivery was verified;
owner-signed-in private visibility remains pending. No code deployment, live mode,
prop sweep or additional paid polling was enabled.

## Pregame snapshot — 2026-10-22

Run[39851920384](https://github.com/Chukwurach2/ev-betting/actions/runs/39851920384),
job120080080865, completed with10 offline checks passing. Free preflight
reported324 remaining before one bounded request;224 quote rows/41 provider
events processed. Independent phone GET and rendered UI confirmed14 games and46
fresh entries at4:48PM ET. Private append-only recorder added46 observations
and14 NBA coverage records (342 observations total);15 NFL coverage records
were recorded separately, with zero current NFL entries. No checkpoint deleted,
app deployment, model promotion or freshness change occurred.

Tonight's two NBA games each had four supported price-watch alternatives, all
negative point EV. One later-game observation had a tiny positive point estimate
(~0.20% before fees), but a lower reference implied negative EV. It fails robust
value and the registered prospective thresholds. Keep it conditional, outside
the pilot, with unknown accepted size/offer clearance and incomplete later-game
injury reporting. Exact prices/times, references and original sensitivity
analysis are private. Earliest supporting expiry was21:02:23Z; this remains a
snapshot, not continuous coverage. Past versions and outcomes remain unchanged.

Reviewed official4PM injury report: tonight's questionable players remain
conditional, with final actives/minutes unverified. Provider/official10-minute
start differences remain explicitly unresolved; do not silently rewrite starts.
Source: https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-22_04_00PM.pdf
EVSharps Recommended still requires sign-in; the prior touchdown observations
lack verified quote ages and were not republished as current. Owner-signed-in
private phone verification remains pending. No subscriber table is published.

## Late-window receipt and availability review — 2026-10-23 UTC

Heartbeat metadata was21:13:16Z October22; the audit's first explicit UTC clock
read was01:36:24Z October23, after the intended pregame windows. This measures
receipt-to-audit-clock delay, not a diagnosed scheduler/host failure mechanism.
Do not claim an on-time T-3 review, retroactively select a quote or substitute
heartbeat time for actual observation time. No paid capture was dispatched.

NFL run[39882605298](https://github.com/Chukwurach2/ev-betting/actions/runs/39882605298),
job120183280947, reported at01:12:37Z: captured0,missed2,duplicate2,failed0,
requests0,materialized quote rows0. The successful workflow again did not supply
fresh phone prices. Independent reads at01:36:48/49Z returned14 NFL/20 NBA games,
zero fresh entries.34 private coverage rows were appended; observation count
remains342. No historical checkpoint was deleted or recaptured as fresh.

Official9:15PM ET injury report changed prior questionable statuses: two became
out and one available. These updates were recorded as a new private review,
not backdated into earlier evidence. All prior cards were already expired; no
ready status or paper stake was inferred. Exact player/contract linkage and
final result/rule review remain prerequisites for later descriptive outcomes.
Source: https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-22_09_15PM.pdf

The last recorded15 resolved descriptive entries and their losses remain
unchanged. There is no new performance result, validated edge or active phone
card. This missed-window evidence is retained alongside successful captures,
so availability gaps are included rather than hidden. EVSharps Recommended
still displayed its sign-in gate. Hourly heartbeat timestamps are not proof of
real-time attendance or continuous odds monitoring.

## Full Friday slate capture — 2026-10-23

Run[39996430037](https://github.com/Chukwurach2/ev-betting/actions/runs/39996430037),
job120561089930, passed10 offline checks and made one bounded request after
free preflight reported318 credits remaining.192 quote rows across17 provider
events processed. Independent phone GET/rendered UI confirmed12 games/44 fresh
entries at1:37PM ET. Ten games had4 alternatives; two had2 with explicit
shortfalls.44 private observations plus12 NBA and14 NFL coverage rows appended;
observation total386. Exact source/reference times and earlier histories remain
private. Earliest supporting expiry17:50:55Z; no continuous freshness claimed.

One exploratory point estimate was positive(~0.54% before fees) with references
in a narrow range; its edge is far below registered thresholds and reverses under
1% fixed cost stress. It is conditional on exact eligible-book offer/rules,
accepted size, availability and event/start mapping; no independent model or
validated strategy is claimed. Other43 entries were nonpositive price watches.
Detailed per-game review and sensitivity are private, not a public source table.

Official1PM ET injury report reviewed. Questionable/doubtful and incomplete
clearance remain conditional; no numeric injury adjustment is invented. Source:
https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-23_01_00PM.pdf
NFL checked separately:14 upcoming games/zero fresh entries; next game is
Sunday's international fixture, not within the next24hours. No NFL probability
or quote was borrowed from NBA. EVSharps Recommended remains sign-in gated.

Previously pending performance-branch validation run39996041987 completed
successfully after the deployed withdrawal fix was aligned. Neither that branch
nor the free-plan NFL guard has been merged/deployed. Public market-card delivery
is verified separately from pending owner-signed-in private visibility. No routine
refresh deployment, prop sweep or live execution occurred.
