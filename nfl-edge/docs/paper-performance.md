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
