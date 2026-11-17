# New York contract evidence still needed

The reviewed archived cohort contains 272 entries with an official final and
missing exact contract evidence. Counts are earliest contract-and-book
entries. Several entries can share a game.

| Operator | NBA spread | NBA total | NFL spread | NFL total | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| BetMGM | 91 | 67 | 5 | 4 | 167 |
| DraftKings | 39 | 47 | 7 | 9 | 102 |
| FanDuel | 1 | 0 | 0 | 2 | 3 |

The public BetMGM New York sportsbook links to its New York help center.
The linked route rendered its management rules, with no dated basketball
or football settlement clauses in the returned text. An indexed
Massachusetts document describes basketball settlement, but provides
insufficient evidence for a New York quote. BetMGM NBA is the largest
remaining group at 158 entries.

DraftKings New York terms link to the general and sport rules. The general
rules link returned a help shell, and the sport link redirected to a
different sport. A search-indexed general rules copy names West Virginia.
The New York commission’s application binder describes approved market
types and identifies scoring sources, but its generic spread and total
definitions do not establish which 2026 overtime and void terms governed
the archived New York quotes.

The FanDuel New York rules landing page returned HTTP 403. Indexed dated
earlier copies lack a verified match to the October and November quotes.
Two NFL totals and the existing neutral-venue NBA spread remain held.
No rule was transferred across jurisdictions, operators or historical
versions. The replay baseline remains 126 graded entries and 16 separate
identity conflicts. No prices, final scores or contract records changed.

- [BetMGM New York management rules](https://help.ny.betmgm.com/en/sports-help/sports-house-rules)
- [DraftKings New York terms](https://sportsbook.draftkings.com/legal/ny-terms-of-use)
- [New York commission application binder](https://gaming.ny.gov/draftkings-sub-binder-2-part-6)
- [FanDuel New York rules landing page](https://www.fanduel.com/fanduel-sportsbook-house-rules-ny)

## November 17 UTC replay and event identity review

Pin the 70-final replay to [the selection-mapping evaluator at
bc82760](https://github.com/Chukwurach2/ev-betting/commit/bc82760e9c1813a531ec9568078b696798f2221f)
from PR #47. Its 30 focused tests passed and the replay matched the retained
report field for field, apart from the generation timestamp. The same inputs
on the PR #38 review branch at a548b11 return 122 grades because its evaluator
does not apply the four verified NFL spread selection mappings. Use the pinned
evaluator with those mappings to reproduce the 126-grade baseline.

Archived phone snapshots show two NBA event IDs each carried an earlier and
later start for the same oriented matchup on October 20. The recorded start
moved by ten minutes for one ID and nine minutes for the other. Both variants
were observed before the earlier start. The NBA's [Boston–Detroit
final](https://www.nba.com/game/bos-vs-det-0022600001) and
[Philadelphia–New York final](https://www.nba.com/game/phi-vs-nyk-0022600002)
confirm the games. An official game page alone cannot establish which
archived quote and operator contract remained in effect through a change in
the provider's start field. Both groups remain quarantined, with 16 entries
unresolved. Preserve the two original starts and receipt times and require an
explicit reviewed association for each affected observation before grading.

The 70-final cohort still has 414 earliest entries, with 126 grades and 288
unresolved entries. Of the unresolved entries, 272 lack exact-contract
evidence and 16 are held for conflicting event identities. The review
added no settlements and used the unchanged ledger and evidence.
