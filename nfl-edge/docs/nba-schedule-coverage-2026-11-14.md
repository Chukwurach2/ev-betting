# November 14 NBA phone schedule coverage

The [official NBA schedule](https://www.nba.com/schedule?cal=November) lists five games for Saturday, November 14, all starting within 24 hours of this check. The public NBA phone API at 04:57:39.929 UTC and 04:58:34.122 UTC returned the same two matchups and zero fresh quotes or research entries.

| Away at home | Official tip (Nov 15 UTC) | Phone provider time (UTC) |
| --- | --- | --- |
| Denver at New Orleans | 00:00 | 00:10 |
| Dallas at Cleveland | 00:30 | Missing |
| Detroit at Miami | 01:00 | 01:10 |
| Brooklyn at Memphis | 01:00 | Missing |
| Chicago at Minnesota | 01:00 | Missing |

The five official matchups and tip times were cross-checked against the [NBA's upcoming-game injury report](https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-11-13_12_30AM.pdf). This early report showed submissions pending and provides no clearance for a pick. The phone times were preserved as provider times. A ten-minute difference does not establish that any quote can safely be joined or settled using a substituted start.

The [deployed NBA schedule adapter](https://github.com/Chukwurach2/ev-betting/blob/3cf60a23081b2bd07a8de6fb52877f8e8e89103b/nfl-edge/lib/nba-feed.mjs) consumes the provider schedule; its output alone cannot establish complete league coverage or why the three games were absent. The next bounded diagnostic should compare the free provider event response and the app feed after the five-minute cache window. When official games lack a verified provider mapping, expose them as schedule-only coverage gaps. Keep exact provider identity for all priced entries and preserve separate official and provider times.

The NFL read at 04:57:40.297 UTC returned 14 upcoming games and zero fresh quotes or entries. [The NFL schedule](https://www.nfl.com/news/nfl-how-to-watch-week-10-nfl-games-schedule-munich-game-details-and-more) confirms the next game, New England at Detroit in Munich at 14:30 UTC November 15, was outside the 24-hour window. No new odds collection or paper observation was recorded for either sport.
