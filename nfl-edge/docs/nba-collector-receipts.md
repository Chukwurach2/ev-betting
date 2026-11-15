# Bounded NBA collection receipts

The existing one-run NBA collector checks the free events quota before
requesting full-game spreads and totals. The odds response must also report
a valid remaining balance at or above the reserve and a charge of at most
two credits. A nonempty response requires a positive charge. An empty
response may report zero. A missing or inconsistent receipt stops before
quote storage and does not retry the request.

The run summary distinguishes provider-reported charge, post-request
remaining balance, rows processed and new committed rows. Existing quote
IDs are idempotent. Successful storage still requires a separate phone
feed check for current supported cards.

The last verified collection is [the October 30 run](https://github.com/Chukwurach2/ev-betting/actions/runs/41140953492).
Its pre-request receipt is historical. Neither this change nor its
synthetic tests dispatches collection or adds a schedule.
