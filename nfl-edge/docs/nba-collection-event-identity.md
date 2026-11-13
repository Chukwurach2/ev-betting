# NBA event identity at collection

The NBA collector now retains odds events only if the free schedule preflight
contains a unique NBA event with the same provider ID, home and away teams
and kickoff instant. Duplicate IDs in either response are excluded. An event
with a new or changed schedule can be evaluated on a later authorized capture.

This check runs before quote normalization and storage. It adds no request.
The reserve and one-request limit remain in effect. The schedule and odds
responses are from the same provider, so the check establishes consistency
between payloads and does not independently verify the schedule, sportsbook
availability or settlement rules. The change is covered by offline fixtures.

The collector now resolves duplicate schedule IDs before the odds request.
If every upcoming NBA ID is ambiguous, the run stops after the free schedule
read with status `no_unique_upcoming_events` and zero odds requests. A mixed
slate can still collect the uniquely identified games under the existing cap.
