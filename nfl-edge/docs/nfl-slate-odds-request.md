# Filtered NFL slate odds request

The [provider's sport odds endpoint](https://the-odds-api.com/liveapi/guides/v4/#get-odds)
supports an explicit eventIds filter. The existing sport odds client can now
send several provider event IDs in one request. Empty, duplicate and malformed
filters fail before transport. Existing calls can still request an unfiltered
sport response by omitting the argument.

The scheduled collector still uses its event-specific path. Before integrating
batching, claim only due checkpoints, resolve unambiguous pregame identities,
and verify the returned event ID, teams, kickoff and market timestamps per
checkpoint. Record omissions and use the actual quota headers to verify the
two-credit budget for the selected markets and bookmaker group. Batching can
improve concurrent coverage when the runner reaches the collection window.
The adapter tests use synthetic responses without provider calls.
