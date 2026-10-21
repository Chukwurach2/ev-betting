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
