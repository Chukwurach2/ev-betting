# Curated paper review checks — November 20, 2026

The offline performance evaluator previously accepted truthy offer/availability
flags for curated observations. Strings such as `"false"` could therefore pass
those checks. A missing reference timestamp also differed from explicit null
and could slip through. Exact-book result evidence alone must not convert an
incompletely reviewed curated observation into a graded paper return.

Curated records now need literal boolean true for both offer and availability
verification, observed review status, and a parseable reference timestamp string
before kickoff and no later than the evaluation time. Otherwise they remain
`curated_review_incomplete`, with null return and exclusion from the graded ROI
denominator. This does not add a source freshness certificate, independently
verify an offered price, or replace the separate publisher's validation and
exact-book settlement evidence. Conditional and invalidated records remain
ungraded. Ordinary market-observation rows keep their existing descriptive
cohort policy.

Seventeen focused tests passed, including non-boolean flags, missing/invalid
reference times, post-start/future references and correctly reviewed controls.
A full same-clock replay of the current 1,039-record ledger and 71-final evidence
set was identical before and after the change: 416 earliest entries, 126 graded,
290 unresolved. There are no curated records in that cohort, so no historical
grade, individual profit or denominator changed. The ledger bytes and existing
proof files remain unchanged. Private replay evidence is retained locally.

This is an offline accounting fix on the research branch, not a production
release, new statistical experiment, accepted-wager report or model promotion.
Use the new exact evaluator commit when rerunning future cohorts; preserve
previous reports with their original version pins.
