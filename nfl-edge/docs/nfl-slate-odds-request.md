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

The offline response matcher requires a unique event ID in both inputs and
a matching NFL sport key, home team, away team and timezone-aware kickoff
instant. Unexpected IDs, omissions, identity conflicts and duplicate IDs are
excluded individually. Equivalent timezone offsets are accepted. A caller
must still map the expected provider events to due checkpoints, handle omitted
games, and check source freshness and quota before persisting quotes.

The single-event collector now uses one reusable response classifier for
checkpoint deadlines and source-time checks. A future batch can apply that
classifier independently to each due game using the actual completion time of
the shared request. The extraction preserves opener semantics and the fixed
checkpoint freshness gates. The request path still fetches one event at a time.

The T−24 batch worker now accepts checkpoints paired with reviewed provider
IDs and an injected quote adapter. It claims eligible checkpoints before
one budgeted request and applies the shared response rules to each claim.
An omitted event is unavailable, mismatched quotes fail that event, and a
late response cannot create a capture. Unknown quota or fewer than 52 credits
defers due work. Runtime integration still needs to connect the adapter and
verify actual billing headers. The scheduled collector does not call the
batch worker.

The draft provider callback makes one filtered spreads/totals request for
an explicit New York bookmaker group. It checks the response's reported
charge and remaining balance before returning matched, paired quotes to the
worker. Missing or out-of-bounds quota headers fail the call. The retained
source URL identifies the sport odds route and excludes credentials.
The callback and worker are covered offline. The scheduled workflow still
uses its existing collector and actual batch billing has yet to be observed.

The draft target resolver pairs only due NFL T−24 checkpoints with a unique
scheduled game and an exact provider matchup and kickoff. Ambiguous matches,
reused IDs, duplicate checkpoint keys and malformed filter values are
excluded. The resolver returns both the input for the batch worker and the
reviewed identities for the response adapter. It uses the existing free
events response in offline tests. Collector selection and scheduling still
need integration before a controlled batch verification.

On November 15, the 00:46 and 07:34 UTC NFL workers reported four missed
checkpoints in aggregate, zero captures and zero odds requests. At 14:42:55–56 UTC
the public feeds returned no current NFL or NBA entries. The batch draft
addresses same-window capacity after a runner starts, so these receipts
provide a separate example of runner timing limiting coverage.

- [00:46 UTC collection job](https://github.com/Chukwurach2/ev-betting/actions/runs/43659750947/job/132646174575)
- [07:34 UTC collection job](https://github.com/Chukwurach2/ev-betting/actions/runs/43690464830/job/132739834360)
