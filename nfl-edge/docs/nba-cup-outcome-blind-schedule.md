# NBA Cup schedule validation

The fixture gate takes three inputs from a future source review. The publisher
rows provide native game, team and start identifiers. The reviewed crosswalk
covers all regular-season fixtures in the registered window. A separate set
contains the 60 official Cup group-stage fixture IDs. The gate checks one-to-one
IDs, exact sides and start, Eastern date, distinct team starts and full coverage
in both directions. It labels games using the official IDs.

Six synthetic cases verify the coverage, time and identity checks. No real
schedule was linked and no scores or market prices were read. The caller
still needs authoritative rules and schedule evidence. A passing structural
gate alone cannot establish the crosswalk's provenance. The frozen protocol
and its thresholds remain as registered.
