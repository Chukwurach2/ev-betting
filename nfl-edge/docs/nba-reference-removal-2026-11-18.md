# NBA October 30 retained-reference sensitivity

The replay used all 28 published entries across ten games in the archived
October 30 NBA phone response generated at 22:52:59.012 UTC. The original
[capture receipt](https://github.com/Chukwurach2/ev-betting/pull/35)
records the acquisition and expiry. All 28 archived estimates reproduced
from the median of the stored de-vigged reference probabilities and
the original offered prices. Each offer and its references was fresh
at snapshot generation, with the offered operator excluded and
reference aliases counted once.

Each replay held the displayed offer fixed and dropped one retained
reference operator. The original rule required two distinct operators
to remain. Twelve entries across six games had only two references and
lost support under either removal. Of 72 removal cases, 24 lacked enough
references and 48 retained enough. All 48 remaining estimates were
negative, ranging from −4.55% to −0.87% before further costs.

The archived response contains only entries the card algorithm selected.
The replay therefore measures sensitivity of those displayed entries
without inferring what alternate prices, contracts or unpublished
candidates could have replaced them. Removals share observations and
are not independent trials. Distinct references may share inputs.
No outcomes were inspected. The replay did not change the original
observations or their prospective classification. These prices had
already expired before the November review.

The November 18 public feed at 17:22 UTC listed 12 NBA and 14 NFL games,
with zero fresh card entries for either sport.
