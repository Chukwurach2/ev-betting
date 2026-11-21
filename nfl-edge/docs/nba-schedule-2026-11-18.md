# November 18 NBA schedule verification

At 14:17 UTC the public game-card feed listed all 12 matchups on the
November 18 Eastern NBA slate, with zero fresh entries. Ten provider
tip fields were ten minutes after the official listed time. Two
listings exceeded the audit's 15-minute comparison tolerance. They
appear as missing time-compatible rows in the audit while present
by matchup on the phone.

| Matchup | Official scheduled tip ET | Phone listing ET | Difference |
| --- | --- | --- | ---: |
| Golden State at Dallas | 8:00 p.m. | 9:40 p.m. | +100 min |
| Boston at Denver | 9:30 p.m. | 9:10 p.m. | −20 min |

Dallas announced a change from 8:30 p.m. Central to 7:00 p.m. Central
on November 14. The phone time is ten minutes after the prior schedule.
The dated ESPN schedule update, current NBA slate and NBA injury report
list the Boston–Denver game at 9:30 p.m. Eastern.

These are reviewed schedule differences only. A later displayed start
can keep a game in the upcoming list after its official scheduled tip.
An earlier displayed start can remove it prematurely. Preserve the
official and provider times separately. A matchup comparison alone
cannot verify quote identity or establish actual tip. No prices were
joined, and the evidence does not support a paper card from either
listing.

- [Official NBA slate](https://www.nba.com/games?date=2026-11-18)
- [Dallas tipoff change](https://www.nba.com/mavs/news/tipoff-time-change-announced-for-dallas-mavericks-november-18-game-against-golden-state-warriors)
- [Boston–Denver broadcast update](https://espnpressroom.com/press-release/espn-nba-schedule-update-denver-nuggets-host-boston-celtics-on-november-18/)
- [NBA injury report with scheduled game times](https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-11-18_08_30AM.pdf)

## Later snapshot: Dallas listing recovered

In the public response generated at 23:36:23.724 UTC on November 18,
Golden State at Dallas had changed to 01:10 UTC on November 19
(8:10 p.m. Eastern), ten minutes after the official scheduled tip.
The audit now found eleven time-compatible listings, all shifted by
ten minutes, and one matchup without a time-compatible row. Boston
at Denver remained listed twenty minutes before its official tip.
There were no ambiguous matches and neither sport returned fresh entries.

This establishes a correction in the saved public feed, not its cause
or the exact time it occurred. The original 14:17 UTC receipt remains
unchanged above. The response and audit were retained privately; the
replay asserted the counts and both affected provider times. Review
completion was delayed until November 19 UTC, so this receipt is
historical and does not establish current schedule or price coverage.
