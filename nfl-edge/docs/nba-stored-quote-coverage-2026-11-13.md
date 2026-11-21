# NBA stored quote coverage on November 13

At 03:00:19 UTC, the production phone API listed nine upcoming NBA games, seven within 24 hours, with zero fresh quotes or card entries. The corresponding NFL check listed 14 upcoming games, none within 24 hours, and zero fresh quotes. The NBA schedule and quote storage are separate.

At 03:00:59 UTC, a read-only aggregate of `nba_edge_odds_quotes` found 3,096 rows across 72 distinct provider-event and kickoff pairs. The latest stored source time was October 30 at 22:52:32 UTC and the latest collection time was October 30 at 22:52:48 UTC. Storage had zero event starts in the next 24 hours and zero upcoming events with source times within 15 minutes. This locates the current NBA coverage gap upstream of phone ranking. No provider request was dispatched.

## Can the archive supply T−60 spread prices?

This is a coverage check. It does not inspect outcomes, assess the rest hypothesis, or change its registered protocol. For each stored event and kickoff, count full-game half-point spread rows with source time from T−75 through T−60, collection by T−60, and source time no later than collection. Then group by exact selection and line, collapse sportsbook aliases, and require one phone-allowlisted operator and two other operators for a market comparison.

The timestamp screen retained 28 rows across four event starts. The operator screen found six exact contracts across three event starts. This archive alone provides too little coverage for a useful T−60 rest-spread evaluation. The counts do not establish sportsbook availability, rule equivalence, benchmark independence, feature coverage or any positive edge. No raw quotes or outcomes were exported.
