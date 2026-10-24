# Lean accuracy and paper return tracking

Keep every original observation: exact event, market/selection/line, book, American odds,
quote and capture time, expiry, method, estimated probability and supporting sources.
The private ledger is separate from the personal betting journal. Expired entries
remain historical observations; they do not become current picks again.

`node ops/report-paper-performance.mjs runs/phone-paper.jsonl runs/result-evidence.json runs/performance-YYYYMMDD.json 0`

The evidence file contains `results` and `contracts` arrays using the exact schemas
in docs/paper-outcomes.md. Omit the evidence argument to audit unresolved coverage.
Output creation is exclusive: use a new filename for every review; never replace an
old report. Detailed output stays in ignored runs/ and includes original quote fields
alongside result evidence. The final argument is a fixed fee per settled one-unit
entry; zero is an explicit baseline assumption. Never commit private report rows.

The report shows wins/losses, resolved/unresolved coverage, accuracy, hypothetical
net units and ROI by book, sport, market, method, reference quality and lead-time
bucket (0–1h, 1–6h, 6–24h, >24h). It separately summarizes positive point estimates;
negative-edge watches are not silently treated as recommendations. No resolved
entries means accuracy and ROI are null, never zero or a fabricated return.

Primary descriptive policy: earliest observed pregame quote per
sport/event/start/market/selection/line/book, with ID as deterministic tie-breaker.
Repeated observations remain in the ledger but are removed from the report cohort.
This policy is retrospective and does not validate a prospective selection strategy.
Different books, opposing sides and same-game contracts remain correlated; report
unique event counts and do not infer statistical independence or significance.
Do not rank tiny book/time subgroups as proven edges or optimize on these results.

Every eligible outcome is recomputed from a sourced final and observation-linked
rule evidence, rather than accepting a supplied win/loss label. Conflicting evidence
stays unresolved. Multiple team/start identities under the same sport/event ID
quarantine that event until identity mapping is reviewed; they never count as extra games. Unsupported props, integer pushes, voids and abandoned games are
unresolved pending supported adapters and rules. Unresolved cases remain visible
and excluded from resolved-return denominators; incomplete coverage makes ROI provisional.

A one-unit win earns odds/100 at positive American odds or 100/abs(odds) at negative
odds; a loss costs one unit. Subtract the configured fixed cost per resolved entry.
This is hypothetical observed-quote performance, not accepted-wager P&L. It excludes
execution, limits, tax and contingent fees. Accuracy alone cannot establish profit:
two wins out of three at -300 still lose money. CLV is unavailable without a verified
closing quote for the same contract; do not substitute a later or different line.

Current capture ledger does not automatically include every manually curated private
prop or prior chat lean. Import only original records with exact preserved source
times/identity; do not invent historical prices. Source-incomplete legacy rows are
separately grouped. Next delivery work is curated-card ledger coverage, supported
prop settlement and an owner-private phone performance view. This CLI does not
publish results to the phone or modify the frozen model, journal or bets.

## Resolution work queue

The private report includes unresolved_reasons with entry counts and identity_conflicts
with every preserved team/start variant and affected-entry count. Sum reason counts
to reconcile unresolved coverage. Use this queue to verify provider schedule changes
against original event identity; never silently overwrite scheduled start with actual
tipoff or fuzzy-match teams. A revised start needs a reviewed mapping/evidence policy
before releasing quarantined entries. Publish aggregate diagnostics only.

## First final-source review (2026-10-20)

The rendered official NBA summary for game 0022600001 exposed a final banner
and four period scores. Both team sums reconciled to the displayed final.
Search snippets and static page text were insufficient to establish that result.
The detailed source-review receipt is private; this review does not change original
provider starts or release quarantined rows. Exact event/start mapping and venue
rules remain required before evaluating the observed contracts. Preserve the
source's own game identifier separately from the provider identifier.

## Private curated-history import

Use `node ops/record-curated-paper.mjs PRIVATE_ORIGINAL_ARRAY runs/curated-DATE.jsonl`
then pass that JSONL to the performance report. This opt-in, offline import preserves
original analysis, quote/reference times, conditions, review status and a payload hash.
It validates the existing private-original-analysis contract, rejects future offers
and conflicting content under one ID, deduplicates identical IDs, and creates output
exclusively. It does not refresh expiry or publish data. Keep all files private.

Conditional, withdrawn, unknown-reference-age, unverified-offer or unchecked-availability
entries remain unresolved even if a final is supplied; by_review_status separates these
cohorts. Props remain unsupported for grading. Four existing original-analysis records
were imported privately without changing their historical quotes; this is not proof
that every earlier chat lean or backend record has been imported. Backend publication
and history capture are still separate steps. Include this import in future curated
publication reviews and reconcile coverage before describing statistics as complete.

## First resolved descriptive entries (2026-10-21)

Two entries from one completed event now have directly reviewed official final
and venue-specific rule evidence. The rendered FanDuel NY house rules (effective
October 15, 2026) apply to its mobile apps; its NBA-specific section uses official
league results and includes overtime unless a market states otherwise. This review
was bound only to eligible stored full-game half-point contracts at that operator,
not another book or an unsupported prop. The official four-quarter final reconciled
and the scheduled start exactly matched the preserved provider observation identity.

Private report snapshot: 256 input observations, 140 earliest contract/book entries,
17 event IDs; two resolved and 138 unresolved. Resolved entries were one win and one
loss: hypothetical fixed-unit net -0.107143 and ROI -5.357143%, assuming zero fixed
fees. Both were price watches/alternatives in one game. This is neither a bet-all
strategy nor an independent sample; positive-estimate entries remain unresolved.
No actual accepted stake, payout or realized profitability is established. Details
and observation-linked evidence remain private; no subscriber rows are published.

Sources reviewed directly:
- https://www.nba.com/game/okc-vs-sas-0022600003
- https://www.fanduel.com/fanduel-sportsbook-house-rules-ny

Current rendered rules, rather than older search snippets or a different state's
rules, were used. Keep future rule revisions and result corrections append-only.

## Second operator rule review (2026-10-21)

Reviewed theScore Bet NY rules effective August 17, 2026, basketball BB.1.1
(default overtime inclusion unless explicitly excluded) and completed-event
conditions. Applied only to stored supported full-game half-point observations
at that operator in the already verified event; no rules transferred across books.
Source: https://thescorebethelp.zendesk.com/hc/en-us/articles/41417125762573-House-Rules-New-York

Two additional entries resolved as losses. Descriptive total: four resolved in
one event, 1 win / 3 losses (25% accuracy), hypothetical one-unit net -2.107143
and ROI -52.678571% with zero fixed fees. Other 136 entries remain unresolved.
All four were price watches, not a prospective positive-edge strategy; positive
point-estimate cohort remains unresolved. Different contracts/books in the same
game remain correlated alternatives. The result does not validate or reject a
strategy. Preserve losses, original observation times, and outstanding coverage.
No accepted-wager P&L or CLV is claimed. Private evidence/report files are separate
versions; the earlier two-entry report is retained for audit.

## Second completed event reviewed (2026-10-22)

Verified official NBA game0022600086 in its rendered final banner, summary game
identity and four-quarter linescores. Scheduled start, exact teams/date and venue
matched the preserved provider identity without a time correction. Applied only
the previously reviewed FanDuel/theScore Bet NY full-game basketball rule versions
to supported stored half-point contracts. Five further entries resolved:3W/2L.
Official source: https://www.nba.com/game/mil-vs-was-0022600086

New private report version retains256 input observations and140 earliest
contract/book entries over17 event IDs. Nine entries resolved over two events:
4W/5L,44.444444% accuracy; hypothetical fixed-unit net−1.291734,ROI−14.352599%
with zero fixed fees.131 remain unresolved. By-book descriptive resolved coverage:
FanDuel5(3W/2L,+0.799175u);theScore Bet4(1W/3L,−2.090909u). Do not infer a book
advantage from these tiny correlated samples. Positive-estimate cohort is still
unresolved; these are retrospective price-watch alternatives, not the registered
prospective pilot. No actual accepted stake, user P&L, CLV or strategy validation
is established. Earlier report versions and losses remain preserved privately.

## Third completed event and positive-estimate cohort (2026-10-22)

Reviewed official NBA game 0022600087 in the rendered final banner, game identity
and four-quarter linescores; totals reconcile and original scheduled start/venue
match. Applied only previously reviewed FanDuel/theScore Bet NY full-game rules
to supported half-point contracts. Six further entries resolved: 3W/3L.
Source: https://www.nba.com/game/cha-vs-bkn-0022600087

Private versioned report: 256 input observations, 140 earliest contract/book
entries across 17 event IDs; 15 resolved over three events, 7W/8L (46.666667%),
hypothetical one-unit net -1.430262u, ROI -9.535080% at zero fixed fees;
125 unresolved. FanDuel: 8 resolved, 5W/3L, +1.708266u; theScore Bet:
7 resolved, 2W/5L, -3.138528u. Correlated alternatives and incomplete rule
coverage prevent interpretation as independent book performance.

The sole positive point-estimate entry now resolves as a win (+0.952381u
hypothetically). Its original estimate was only +0.371745% and sensitive to
reference choice; one winning observation establishes no profitability or
calibration. It predates the registered prospective pilot and remains outside
that cohort. All broader price watches stay separate, with losses preserved.
No accepted stake, actual P&L, CLV or validated strategy is claimed. Exact quotes,
book/time details and evidence remain private; previous versions are retained.

## Result timing mismatch diagnostics (2026-10-23)

Do not discard an available same-event final merely because its start differs
from the observation. Route same-sport/event-ID evidence through the existing
strict evaluator so timing mismatch is reported explicitly. Multiple candidate
finals remain ambiguous even if one has an exact start; do not select a favorable
result by silently filtering competing evidence. No fuzzy matching, timestamp
rewrite or widened identity tolerance was introduced. Regression tests cover
both mismatch and mixed exact/different-start result evidence.

Official NBA game0022600091 final and quarter totals were verified, but its
scheduled start differs from original stored observations. Seven entries remain
unresolved as invalid_event_timing, pending a separately reviewed explicit
cross-source mapping; this is not permission to grade the contracts. Source:
https://www.nba.com/game/dal-vs-hou-0022600091

Private report with342 observations now contains205 earliest contract/book
entries across28 event IDs.15 resolve:7W/8L,46.666667% accuracy, hypothetical
fixed-unit net-1.430262u/ROI-9.535080% at zero fixed fees.190 unresolved include
7 timing mismatches,17 missing exact-contract evidence,16 conflicting observation
identities and150 missing finals. Two positive point-estimate entries exist:
one resolved win and one unresolved; no profitability inference. Older versions
remain preserved. All167 Node tests pass. This is offline reporting in review,
not deployed phone performance or accepted-wager P&L.

## CI base alignment (2026-10-23)

CI run39885459111 failed in the curated-paper unknown-reference withdrawal test.
The PR's older stacked base still rejected invalidated entries without reference
times, despite the already deployed PR34 fix. Local167-test success used that
newer validator and therefore did not establish the prior remote tree passed.

Copied lib/private-leans.mjs and its regression tests byte-for-byte from canonical
main ffe15d04855ae2be83203b759de1642eb5623df8 into this branch. Unknown reference
age still cannot become observed; invalidations remain valid and supersede prior
quotes before book preference filtering. This preserves production withdrawal
behavior instead of weakening tests. Both copied files match canonical main;
all167 local Node tests pass with those exact files. Remote CI must be checked
on the new commit. No performance result or private data changed.

## Explicit retrospective result bridge (2026-10-23)

Optional reviewed mappings now bridge a stored provider start and an official
result start without editing either record. No mapping means exact-start checks
remain mandatory. A bridge binds sport, provider event ID, official external ID,
exact home/away teams, both original starts, matching HTTPS result source, review
reason/ID and review time after result evidence and no later than now. Same UTC
date and at most15minutes difference are guardrails, never automatic tolerance.
Multiple mappings are ambiguous. Existing conflicting observation histories
remain quarantined. Quotes must precede the earlier of both starts; contract,
final score and period checks still apply. Output retains mapping ID and both
starts for audit. The bridge is result analysis only, never pregame eligibility,
backfilled pilot selection or a general correction of provider schedules.

Reviewed the previously preserved official NBA0022600091 evidence against the
exact recorded matchup/event/date and venue, explicitly retaining the10-minute
schedule discrepancy. Applying previously reviewed operator-specific full-game
rules resolves two more observations as losses. Private versioned report now has
386 observations,245 earliest contract/book entries across28 event IDs:
17 resolved over4 events,7W/10L (41.176471%), hypothetical fixed-unit net
-3.430262u /ROI-20.178012% at zero fixed fees;228 unresolved. Of those,22 lack
exact-contract evidence,16 have conflicting observation identities and190 lack
finals. Three positive point-estimate entries:one resolved win/two unresolved.
Do not infer profitability or book superiority. All original records and earlier
unmapped reports remain retained; no accepted stake, CLV or realized P&L exists.

All171 Node tests pass, including binding tampering, future/premature reviews,
quote timestamps between the two starts, missing rules, no mutation and duplicate
mapping rejection. Production phone/model/journal remain unchanged.

## Fifth event outcome review (2026-10-23)

Verified official NBA0022600006 in the rendered final banner and quarter table:
full-game score and quarter sums reconcile. Reviewed exact teams/date/venue and
linked the preserved provider event through an explicit manual mapping, retaining
both7:10PM provider and7PM official starts unchanged. Applied only previously
reviewed FanDuel/theScore Bet NY full-game rules to supported half-point entries.
Four additional entries resolve:3W/1L. Official source:
https://www.nba.com/game/cle-vs-phi-0022600006

Latest private report:386 observations,245 earliest contract/book entries across
28 event IDs;21 resolved over5 events,10W/11L,47.619048% accuracy. Hypothetical
fixed-unit net-1.590417u/ROI-7.573413% at zero fixed fees.224 unresolved:
31 missing exact-contract evidence,16 conflicting observation identities,
177 missing finals. By-book resolved coverage:FanDuel11(7W/4L,+2.595731u),
theScore Bet10(3W/7L,-4.186147u). These small correlated cohorts do not establish
book advantage or a validated policy. The three positive point-estimate entries
remain one resolved win/two pending; broader watches are separate.

No original record, price, reference time or earlier report was overwritten.
No accepted wager, stake, CLV or actual account P&L is inferred. Current feeds
were checked separately:14 NFL/12 NBA games, zero fresh entries after the prior
snapshot expired. No paid request or new pregame decision occurred. Mapping
implementation full CI run40004532303 completed successfully; this increment
only adds private evidence and public aggregate documentation.


## Sixth event outcome review (2026-10-23)

Verified official NBA0022600007 directly in the rendered NBA summary: final banner,
four quarter totals, exact teams/date,9:30PM ET start and Paycom Center agree.
Source: https://www.nba.com/game/den-vs-okc-0022600007
An explicit reviewed association preserves the provider9:40PM and official9:30PM
starts separately. Only existing reviewed FanDuel/theScore Bet full-game rules
were applied. Three additional observations resolve:2W/1L. DraftKings/BetMGM
entries remain unresolved without applicable reviewed operator rules; retail or
other-jurisdiction rules were not substituted.

The private report now covers386 observations and245 earliest contract/book
entries over28 event IDs:24 resolved across6 events,12W/12L,50% accuracy.
Hypothetical fixed-unit net is-0.638036u,ROI-2.658482%,with zero fixed fees;
221 remain unresolved. This is a small correlated alternatives cohort, not an
accepted-wager record or validated profitable strategy. Original observations,
prices, source times and previous report versions remain unchanged. No odds
credits, model changes, live actions or new phone deployment were needed.
The Recommended page still displays its Sharp sign-in gate; no subscriber
shortlist or fresh candidate was inferred from that page.


## Seventh event outcome review (2026-10-24 UTC)

Rendered official NBA0022600099 confirms Chicago132–Milwaukee125; quarter sums,
October23 date,4PM ET and Fiserv Forum verified directly:
https://www.nba.com/game/CHI-vs-MIL-0022600099
Explicit reviewed mapping retains stored4:10PM and official4PM starts. Only
previously reviewed FD/theScore full-game rules applied. Four further entries
resolve,2W/2L. Missing operator rules and conflicting identities stay unresolved.

Including the prior evening append, private ledger426 observations yields264
earliest contract/book entries across28 event IDs:28 resolved over7 events,
14W/14L,50% accuracy,hypothetical fixed-unit net-0.733274u/ROI-2.618835%,zero
fixed fees.236 unresolved. Correlated alternatives, not accepted wagers or a
profitable policy. Original rows,starts,quotes and prior reports preserved.

Heartbeat22:51:46Z resumed with first verified clock01:13:01Z: more than two
hours later. Earlier17:42ET quotes expired17:58ET and were not revived. No
pretend pregame decision or paid capture was made for the missed window.
EVSharps Recommended still shows sign-in gate. No new subscriber-backed card
claimed. Code CI previously passed; this increment is private evidence and
aggregate docs, requiring no deployment or model change.


## Eighth event outcome review (2026-10-24 UTC)

Direct rendered NBA0022600095 final banner and quarters verify Dallas121–Indiana105,
October23,7PM ET at Gainbridge Fieldhouse:
https://www.nba.com/game/dal-vs-ind-0022600095
A cached secondary score118 was not used; official quarter sums reconcile121.
Explicit reviewed mapping preserves stored7:10PM and official7PM ET starts.
Only reviewed FD/theScore full-game contracts resolve: four more entries,1W/3L.

Private report426 observations,264 earliest contract/book entries,28 event IDs:
32 resolved over8 events,15W/17L,46.875% accuracy. Hypothetical fixed-unit
net-2.771735u/ROI-8.661673%,zero fixed fees;232 unresolved. All original
quotes,source times,earlier reports and losing observations retained. This
correlated alternatives cohort is neither accepted wagers nor a validated
strategy. No paid request, deployment, model or private journal change.
Recommended page remains signed out; no new subscriber-backed opportunities.


## Ninth event outcome review (2026-10-24 UTC)

Rendered official NBA0022600097 confirms Toronto112–Washington102. Quarter
sums,date,7PM ET and Capital One Arena verified:
https://www.nba.com/game/tor-vs-was-0022600097
Explicit mapping retains provider7:10PM and official7PM ET. Four further
rule-supported FD/theScore entries resolve2W/2L. Earliest-per-contract/book
selection remains frozen; later tiny-positive quotes cannot replace an earlier
observation based on the final outcome.

Private report426 observations,264 earliest contract/book entries across28 IDs:
36 resolved across9 events,17W/19L,47.222222% accuracy,hypothetical fixed-unit
net-2.884943u/ROI-8.013730%,zero fixed fees;228 unresolved. All prior reports,
original quote/reference times and losing alternatives retained. No accepted
wagers, validated profitability, paid request, model change or deployment.

Publication initially failed on connector transport; this aggregate update was
recovered only after checking the unchanged branch head. Private evidence was
preserved locally throughout the outage. Raw data remain unpublished.


## Tenth event outcome review (2026-10-24 UTC)

Rendered NBA0022600100 confirms Utah120–New Orleans119; quarters,date,9:30PM ET,
Delta Center verified directly:
https://www.nba.com/game/nop-vs-uta-0022600100?lid=zml3ljvyq5o5
Explicit reviewed mapping preserves provider9:40PM/official9:30PM. Four more
rule-supported entries resolve3W/1L, including the previously positive-estimate
spread watch. No validation promotion follows from an isolated result.

Including today's distinct observations, private ledger454 observations yields
292 earliest contract/book entries over36 IDs:40 resolved across10 events,
20W/20L,50% accuracy,hypothetical fixed-unit net-1.063240u/ROI-2.658099%,zero
fixed fees;252 unresolved. Earlier records,reports,quote/reference times and
losses retained. Correlated alternatives,not accepted wagers or account P&L.
No paid request,deployment,model or journal change. Recommended remains gated.
The official recap also reports a new in-game knee injury to Missi; tonight's
NOP game requires updated official availability review before any promotion.


## Eleventh event outcome review (2026-10-24 UTC)

Rendered NBA0022600102 confirms Lakers125–Clippers117; quarter sums,date,
10PM ET and Crypto.com Arena verified:
https://www.nba.com/game/lac-vs-lal-0022600102
Explicit reviewed mapping identifies official LA Clippers label with the stored
Los Angeles Clippers identity and preserves10:10PM provider/10PM official starts.
Five supported FD/theScore entries resolve2W/3L; all eligible alternatives
retained, not only the previously positive watch.

Private454 observations ->292 earliest contract/book entries over36 event IDs:
45 resolved across11 games,22W/23L,48.888889% accuracy,hypothetical fixed-unit
net-2.192610u/ROI-4.872468%,zero fixed fees;247 unresolved. The positive-estimate
subset is tiny and selected; wins alone do not establish calibration,independence
or policy profitability. Original quotes,earliest-observation rule and losses
preserved. No accepted wager,paid capture,deployment or strategy promotion.
