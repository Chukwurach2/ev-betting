# Private card event identity validation

A supplied batch/feed could previously attach one sport/game ID to inconsistent home/away participants or kickoff instants. Each payload could individually pass validation, and selection deduplication or a selected-book filter could hide the conflicting version. That could present a contract without a consistent event identity.

The research validator now compares the exact participant pair and parsed kickoff instant for each sport/game ID across all otherwise-valid supplied payloads. Publication rejects a conflicting batch. Display suppresses every selection belonging to a conflicting event before selection deduplication or book preferences. Unrelated events remain visible. Equivalent explicit timezone spellings of the same kickoff instant remain compatible.

This is deliberately conservative: a legitimate reschedule or participant correction needs a reviewed consistent mapping, rather than silently selecting one conflicting version. No fuzzy name matching or schedule-tolerance expansion was added. The guard covers the rows supplied to each call; it is not a database-wide historical identity audit or a cross-batch transaction constraint. Invalid payloads retain their existing unavailable behavior.

Validation: the new conflict regression failed on the exact parent source, then all 28 focused tests passed after the change. Cases include changed home/away participants, changed kickoff, both row orders, conflicting same-selection revisions, a hidden unselected book, unrelated games and equivalent kickoff offsets. Existing freshness, explicit verification and immutable publication checks still pass.

PR-only implementation; no deployment, private publication, frozen-model change, journal/ledger mutation, provider collection or statistical experiment. At the current independent coverage check (20:36 UTC), both NFL and NBA feeds still had zero current entries. Browser shortlist access could not complete automatic approval after a connection failure, so no fresh source observations or game cards were inferred.
