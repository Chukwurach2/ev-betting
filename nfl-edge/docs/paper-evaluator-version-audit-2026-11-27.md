# Complete-cohort evaluator version audit

The complete current ledger and retained 71-final evidence set were replayed on November 27, 2026. Pin the evaluator to [PR47 commit 1e024d2](https://github.com/Chukwurach2/ev-betting/commit/1e024d216af52851c25daaa62ce968efdc28cb66) when reproducing the current cohort. PR38 is an evidence/documentation branch and does not contain all subsequently stacked evaluator behavior.

An initial replay with PR38's evaluator produced four unsupported NFL selections because it lacks PR47's explicit observation-scoped selection identity path. That failed audit is preserved privately. It is a branch-version mismatch, not a new outcome, missing permission to invent aliases, regression at the correct evaluator head or replacement performance baseline. No historical grades were overwritten to make it match.

The corrected replay used all three evaluator modules from the exact PR47 commit. Deep equality against the complete November 20 baseline passed after excluding only generated_at. Every individual result/profit, unresolved reason, group and denominator remains identical. The original ledger fingerprint also matches; all 1,039 unique records and 578 observations remain unchanged.

The reproduced cohort has 416 earliest entries across 73 provider event IDs, 126 graded entries (62 wins/64 losses) and 290 unresolved (274 exact-contract evidence; 16 identity conflicts). Hypothetical fixed-one-unit, zero-additional-fee graded-only net remains −6.049993683149824 units, ROI −4.8015822882141464%. These descriptive, correlated quote observations are not strategy validation or wager P&L. Recent conditional curated cards and statistics-only receipts were not added to this cohort.

Both initial and corrected replay artifacts are retained privately, with code pins and immutable prior evidence. The corrected full replay completed at 17:48:00 UTC after host delays; the public feed observations at approximately 15:59 UTC (13 NFL/11 NBA upcoming, zero entries) are earlier coverage observations, not fresh completion-time prices. Existing browser and operational-status permissions were not retried or bypassed. No provider request, new statistical experiment, model promotion, deployment, ledger mutation or wager occurred.

Next: retain the exact evaluator pin on every future performance receipt; do not infer implementation version from the evidence PR number. Prioritize supported fresh cards and permitted exact settlement evidence over repeating unchanged performance checks.
