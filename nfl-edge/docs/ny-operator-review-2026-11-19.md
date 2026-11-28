# NY mobile operator source review — November 19, 2026

The [New York State Gaming Commission operator list](https://gaming.ny.gov/sports-wagering)
was read directly during this review. It lists Bally Bet, BetMGM, Caesars
Sportsbook, DraftKings Sportsbook, theScore Bet, Fanatics Sportsbook and
FanDuel Sportsbook, covering the seven canonical keys accepted by the
private research-card validator. An offline set comparison confirmed the
mapping with no additions. The Commission list also includes operators
outside that curated subset; this review does not widen it.

Use `williamhill_us` as the existing provider key for Caesars and
`thescorebet` for the current brand. Historical `espnbet` quote aliases
must normalize to the same operator, never count as an extra independent
reference. The Commission list is authorization evidence. It does not
verify a currently offered contract, access to an account, limits, market
suspension, injury status or exact settlement rules. Recheck the actual
offer and its applicable conditions before making an entry ready.

No exchange is added as an execution venue from reference-feed presence.
Provider labels and marketing availability are insufficient substitutes
for authoritative operator review. A regulator's wagering-menu restrictions
also mean that operator authorization alone cannot validate every prop.

The November 19 18:45 UTC public feed snapshots returned seventeen NFL
and four NBA games with zero fresh entries. This review therefore supplies
no current repriced card or positive-edge finding. The failed 16:08 UTC
[pipeline health run](https://github.com/Chukwurach2/ev-betting/actions/runs/44562554949)
reported stale collector/picks components; schedule sync and settlement
were healthy. Its failure is not evidence of a new credential rejection.
Browser inventory remained empty, so subscriber access was not reverified.
No provider odds request, private publication, account action or deployment
was triggered. The operational evidence and operator-name mapping remain
separate from strategy validation and paper outcomes.
