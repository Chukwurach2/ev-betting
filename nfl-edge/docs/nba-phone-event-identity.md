# NBA phone fixture identity

The NBA schedule and quote storage already carry the provider event ID.
The phone route now preserves that ID during SQL deduplication and compares
it when building the card. NBA offers and references must share the scheduled
provider event ID, exact teams and kickoff. A missing or mismatched event ID
leaves a coverage gap even when the display names and start time agree.

The change uses existing storage and adds no provider requests. NFL keeps
its current schedule matching because its quote storage uses a different
identity path. This pull request depends on the phone reference changes in
PR51. Synthetic tests cover exact offers, wrong-ID offers and references,
missing IDs and sport isolation. Production evidence of an affected card
was unavailable, and the change has not been deployed.
