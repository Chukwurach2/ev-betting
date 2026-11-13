# November 12 NFL collector and phone coverage

The [November 12 game preview](https://www.commanders.com/news/commanders-giants-preview-week-10-2026) gives Washington at New York a 01:15 UTC November 13 kickoff. The public phone API at 21:46:58 UTC showed zero stored or fresh eligible quotes for that game. The NBA feed showed four upcoming games and zero fresh quotes or entries. No collector was dispatched for this check.

## Scheduled execution evidence

The [scheduled workflow history](https://github.com/Chukwurach2/ev-betting/actions/workflows/collect-odds.yml) listed three runs on November 12 before the 21:46 UTC read. All ran at production commit `3cf60a2`.

| UTC start | GitHub run | Captured | Failed | Odds requests | Budgeted credits |
| --- | --- | ---: | ---: | ---: | ---: |
| 01:18:43 | [43133628057](https://github.com/Chukwurach2/ev-betting/actions/runs/43133628057/job/130903553064) | 1 | 0 | 1 | 2 |
| 09:17:52 | [43190211011](https://github.com/Chukwurach2/ev-betting/actions/runs/43190211011/job/131087304032) | 1 | 0 | 1 | 2 |
| 17:55:12 | [43274437887](https://github.com/Chukwurach2/ev-betting/actions/runs/43274437887/job/131378965427) | 1 | 0 | 1 | 2 |

The job summaries establish three aggregate captures and do not identify the events. The 01:19 collection step ran inside Washington–New York’s T−24 window, but its captured checkpoint cannot be attributed to that game from the summary. Each run also showed zero previously captured checkpoints waiting for materialization. That check does not establish that the phone found qualifying quotes for a specific game. The budgeted figures do not establish the current provider balance.

The observed starts were approximately eight hours apart. The T−3 window opens at 22:15 UTC. The next paper check should compare actual collection-step time and event-specific source time with that window and verify the phone while any quotes remain fresh. An hourly workflow definition alone provides no evidence that an hourly run executed.

## Game-specific attribution at 23:54 UTC

The [23:13 scheduled run](https://github.com/Chukwurach2/ev-betting/actions/runs/43327052761/job/131562049188) reported one captured checkpoint, one odds request, two budgeted credits and zero failures. A read-only aggregate check of the stored checkpoints and corresponding quote rows linked that capture to Washington at New York. The same check attributed the earlier 01:19 capture to this game as well.

| Window | Collection start (UTC) | Stored rows | Latest source time (UTC) |
| --- | --- | ---: | --- |
| T−24 | 01:19:18.097 | 16 | 01:18:57 |
| T−3 | 23:14:48.865 | 16 | 23:14:34 |

The T−3 collection ran within the 22:15–23:45 UTC window. With the phone’s 15-minute source-age limit, even its newest stored source was 39 minutes 55 seconds old at the 23:54:29 UTC phone read. No fresh card was verified during the usable interval. The read returned zero fresh NFL entries and zero NBA entries across 13 upcoming NBA games, four within 24 hours. The evidence verifies storage and later expiry. A card during the fresh interval would still have required matching references and eligible offers.

## Retrospective contract coverage

A November 13 read-only replay used each checkpoint's recorded completion time and only the materialized rows from that checkpoint. It matched the stored game, teams and kickoff, kept full-game half-point spreads and totals, and applied the 15-minute source-age cutoff. It counted each canonical operator once per exact selection and line. The calculation repriced each allowed offer against the median stored fair probability from at least two other operators. The EV range below uses the best eligible archived offer for each distinct contract and assumes zero additional fees.

| Window | Fresh rows at completion | Distinct priced contracts | Positive estimates | Range of best estimated EV |
| --- | ---: | ---: | ---: | --- |
| T−24 | 16 | 2 | 0 | −4.336% to −3.704% |
| T−3 | 16 | 4 | 0 | −4.496% to −0.980% |

These are historical market estimates from a checkpoint-only replay. It does not establish what the phone displayed at that time, independently verified fair value, accepted size or settlement equivalence. The 01:01 UTC public read returned zero fresh entries for either sport. The archived contracts were already expired and were excluded from prospective results.

### Sensitivity to de-vig method

For each best baseline offer in the replay, a separate read-only audit matched each reference to the exact opposite selection at the same operator, source time and provider event ID. The spread lines had opposite signs. Totals required the same line. Each pair had to reproduce the stored proportional probability within 0.0000000001 before the power method was applied. The alternate estimate kept the same offer and used the median power-adjusted probability across its references.

Neither T−24 entry had a complete set of verified pairs. Two of the four T−3 entries did. Both remained negative with power de-vig, at −4.65% and −2.33% estimated EV before additional fees. The other two T−3 entries have unknown method sensitivity. These are retrospective market estimates for expired offers. No final result, independent fair value, or historical phone display was established in this audit.
