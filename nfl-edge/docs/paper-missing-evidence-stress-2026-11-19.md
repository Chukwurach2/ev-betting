# Missing-evidence arithmetic stress — November 19

A complete-cohort replay of the retained phone ledger and seventy-event
evidence reproduced the prior report exactly except for its generation
time. Use evaluator `bc82760e9c1813a531ec9568078b696798f2221f` (PR47),
including its explicit NFL selection mappings. PR38 alone has an older
evaluator and is not the calculation pin for this receipt.

Of 414 earliest contract/book entries, 126 are graded (30.43%) and 288
remain unresolved. The unchanged graded cohort is 62 wins and 64 losses,
with hypothetical fixed-one-unit, zero-additional-fee net −6.0499936831u.
Unresolved reasons remain 272 missing exact-contract rule associations
and 16 event-identity conflicts. None was resolved or silently removed.
The graded-only result cannot describe the whole recorded cohort.

A descriptive stress calculation retained each unresolved row's original
quoted odds and assigned two separate marginal endpoints:

- Full loss: −1u per unresolved entry, totaling −288u.
- Quoted full win: odds/100 for positive American odds, or 100/abs(odds)
  otherwise, totaling +265.7801801945u across unresolved entries.

Adding the unchanged graded net gives arithmetic stress endpoints of
−294.0499936831u and +259.7301865114u. These are neither return estimates
nor confidence limits. Simultaneous endpoints are not established as
feasible: opposing selections, multiple books and shared events correlate.
They do not supply missing settlement rules or identity evidence. Costs,
accepted prices, liquidity, refunds and rule-dependent payouts still need
their own evidence; the calculation assumes the stated binary endpoints
and zero additional fees. No expected probability is assigned to either
scenario, and no full-cohort ROI is inferred.

The purpose is to expose denominator risk rather than impute favorable
results. Only 30.43% has the current evidence needed to grade. The sign
or magnitude of the graded subset therefore cannot establish the return
of the full recorded shortlist or validate a betting strategy.

The replay required unchanged ledger SHA-256
`ce5eccda3f4cddbf606a02b85fa6b8d1dc7af55b2f769bf72493e64cdfc08ccd`
before and after calculation. Every prior grade, unresolved reason and
profit reproduced. An independent Decimal sum matched the marginal
quoted-win total within 1e−10. Private methods, the complete replay and
summary are retained in
`runs/paper-missing-evidence-stress-2026-11-19/`.
No detailed observation rows are published here.

This is an accounting-scenario audit, not a new statistical experiment,
new outcome review, prospective-trial extension or strategy promotion.
The original ledger and all prior receipts remain unchanged.
