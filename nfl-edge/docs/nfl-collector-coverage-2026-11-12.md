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
