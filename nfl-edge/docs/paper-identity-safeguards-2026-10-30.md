# Paper identity safeguards — 2026-10-30

Review of PR38 found that event identity variants were collected after repeated
contract/book observations were collapsed. A later repeat with a changed team
could disappear before validation, leaving an apparently settled entry.

Collect variants from every valid pregame observation before deduplication.
Any disagreement quarantines all retained entries for that event. Invalid
postgame rows remain excluded. Equivalent parsed quote timestamps now use the
documented ID tie break even when their UTC spelling differs.

Three regression cases cover hidden conflicts in both input orders, equal-time
price selection in both orders, and invalid-row isolation. All fourteen
performance assertion cases pass offline with a synchronous assertion harness;
the native local test runner stalled, so hosted CI remains the standard runner
check. No test behavior or financial formula was relaxed.

Replay of the preserved 890-record private ledger with the twenty-final evidence
receipt left every retained observation, grade, hypothetical profit and overall
summary identical: 368 earliest entries, 57 graded, 30W/27L, 311 unresolved.
The fix prevents an unobserved failure mode; it did not revise prior performance.
Original receipts remain intact and the new audit stays private.

No new statistical experiment, provider request, model promotion, phone release,
live execution or journal change. Historical research watches remain separate
from validated strategy results and actual wagers.
