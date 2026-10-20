# Paper outcome review

`evaluatePaperOutcome` in lib/paper-outcomes.mjs is an offline evaluator for
recorded NFL/NBA full-game half-point spreads and totals. It never fetches data,
places a wager, changes a journal, or calculates profit, ROI or CLV.

Review a final score against an authoritative source, preserving its HTTPS URL
and observation time. Supply exact sport, game ID, teams and kickoff; no fuzzy
matching. Require the observation's pregame source time. The result must be a
full-game final including overtime. NBA tied finals are rejected. Periods, props,
integer lines, voids and abandoned games remain unresolved.

A separate contract evidence record must link the exact observation ID to the
reviewed venue rule URL and full_game_including_overtime rule key. Missing rule
or score evidence leaves the observation unresolved. The function validates the
record structure, not the contents of the linked webpage: a human/authorized
research run must actually verify both sources before using it.

Append the returned outcome record privately; do not rewrite original observations.
Repeated price observations in a game remain correlated. Win/loss arithmetic is
paper evidence only; stakes and profit_loss remain null. Real settlement disputes,
fees, capacity and accepted prices need separate evidence. Do not count missing
outcomes as losses or omit them from coverage reporting.

Seven regression tests cover sides/totals, nonfinal states, event/sport/time/rule
mismatches, invalid scores and NBA ties, and NFL ties. Synthetic fixtures are not
current game outcomes. Today's observations must remain unresolved until finals
and exact contract evidence are available.
