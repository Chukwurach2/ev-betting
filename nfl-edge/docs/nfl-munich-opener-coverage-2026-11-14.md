# New England–Detroit opener coverage

The [NFL's Munich game notice](https://support.nfl.com/hc/en-us/articles/41438702917268-How-to-Watch-NFL-International-Series-Games) lists kickoff at 14:30 UTC November 15. The 09:59:16 UTC November 14 public phone read listed the game at that time and showed zero priced entries. The T−24 window had yet to open.

A read-only aggregate check attributed 16 stored quotes to the November 9 opener for the exact teams and kickoff. Capture completed at 18:19:57.757 UTC and the newest quote source was 18:19:51 UTC. This audit counted full-game half-point spreads and totals available in the archive's 15-minute window at opener completion. It grouped exact selections and lines and collapsed known aliases to their canonical operators.

| Market | Fresh half-point rows | Distinct selections and lines | Most operators on one exact selection and line | Selections meeting the operator floor |
| --- | ---: | ---: | ---: | ---: |
| Spread | 2 | 2 | 1 | 0 |
| Total | 6 | 4 | 2 | 0 |

The phone consensus requires two other operators after excluding the offered operator. The opener archive fell short even before checking rules, injuries, or price. No eligible estimate or historical phone rendering can be inferred from these counts. The opener also exceeded the freshness limit by the time of the phone read.

The latest scheduled collector run visible at review was [43576596302](https://github.com/Chukwurach2/ev-betting/actions/runs/43576596302), which started at 08:36 UTC November 14. A green workflow offers no evidence of a T−24 capture for Munich. The next review should check the stored exact-game checkpoint and any usable quote window after 14:30 UTC November 14. The public NBA feed at 09:59:17 UTC listed all five November 14 games and returned zero priced entries. This audit used stored data and public phone reads.
