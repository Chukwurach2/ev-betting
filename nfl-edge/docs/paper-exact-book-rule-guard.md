# Bind paper settlement rules to the exact book

The offline outcome evaluator checked the observation ID and rule family but did not compare the evidence's book key with the observation's book key. A rule record copied from another operator and relinked to the same observation ID could therefore produce a paper grade. A synthetic example reproduced this behavior; no such wrong-book grade was found in the current preserved cohort.

The evaluator now requires a nonblank observation book key and an exactly matching contract book key. Missing or mismatched evidence remains unresolved with null returns under the existing exact-contract-evidence reason. Canonical operator identity must be established before review; the evaluator does not silently translate aliases or treat two operators' rules as equivalent. Same-book evidence still requires all existing event, timing, full-game, overtime and result checks. Historical evidence lacking a book key must be reviewed rather than automatically backfilled.

Validation before publication: isolated synthetic reproduction demonstrated the old wrong-book grade, then the new rejection. Four missing/blank evidence-key cases reject, matching evidence still grades, and complete-cohort replay exactly reproduces every402current row, all108grades/profits and294unresolved reasons. Original953-record ledger and private evidence files are unchanged. Three new repository regressions cover relinked other-book evidence, missing book identity and null performance returns. Existing fixtures now identify their synthetic operator. CI explicitly runs the outcome, event-mapping and performance suites.

## DraftKings readiness audit

The current private cohort has69DraftKings NBA missing-contract entries across34games with retained finals. This is a readiness queue, not69new grades. Current mobile rule verification remains incomplete: the official general/sport-rule pages rendered navigation without the needed rules in the inspected browser, whose footer selected another jurisdiction. Search results also returned a different jurisdiction's general rules. The indexed NY retail rules were not established as the current mobile contract, and the regulator's approved-wager menu does not replace sportsbook settlement rules.

Inspected official navigation:
- https://sportsbook.draftkings.com/help/general-betting-rules
- https://sportsbook.draftkings.com/help/general-betting-rules/general-rules
- https://sportsbook.draftkings.com/help/sport-rules/*
- https://gaming.ny.gov/draftkings-sub-binder-2-part-6

No undocumented endpoint, account/location change, guessed jurisdiction parameter or other-book rule transfer was used. No DraftKings rule association was added. Fixed private readiness queue:runs/draftkings-contract-readiness-2026-11-06.json. All current108grades remain unchanged; source scope must be resolved before any of those queued entries can be graded. Existing identity and venue holds remain intact.

Phone at14:37–38UTC rendered15NFL/9NBAupcoming games and0fresh entries. This is a timestamped coverage observation, not continuous monitoring or a subscriber feed. EVSharps authorization remains pending after the earlier automatic review rejection; it was not retried in this increment. No new statistical experiment, provider request, manual collector dispatch, source-row acquisition, deployment, wager or frozen-model/journal change.
