# November 12 NFL pregame coverage check

The [Washington team preview](https://www.commanders.com/news/commanders-giants-preview-week-10-2026)
confirms Washington at the New York Giants at MetLife Stadium on November 12
at 8:15 p.m. Eastern. The phone API matched that kickoff and returned zero
fresh quotes at 13:25 UTC. Four NBA games within 24 hours also had zero fresh
quotes. There were no priced alternatives to review for publication.

The [official Giants injury table](https://www.giants.com/team/injury-report/)
showed full Wednesday participation for both starting quarterbacks and Cam
Skattebo, with Lucas Patrick and Colson Yankoff listed out. This is a practice
and status check. Gameday inactives and an exact eligible quote remain needed.

The current [checkpoint planner](https://github.com/Chukwurach2/ev-betting/blob/3cf60a23081b2bd07a8de6fb52877f8e8e89103b/nfl-edge/ops/checkpoints.py)
was replayed offline against this kickoff. Deadlines are exclusive.

| Window | Start UTC | Deadline UTC | Nominal top-of-hour run within window |
| --- | --- | --- | --- |
| T-24 | Nov 12 01:15 | Nov 12 02:45 | Nov 12 02:00 |
| T-3 | Nov 12 22:15 | Nov 12 23:45 | Nov 12 23:00 |
| T-90 | Nov 12 23:45 | Nov 13 01:14 | Nov 13 00:00 and 01:00 |
| Close | Nov 13 01:10 | Nov 13 01:14 | None |

The public health route showed stale collector and picks heartbeats at 247
minutes. This is an observation about the health timestamps. It does not
establish a missed T-24 collection, a credential failure or a remaining
quota balance. Actual workflow start, capture, provider quote timestamps and
phone coverage are separate checks. A top-of-hour run may start late and
a successful capture can expire before the next review. No manual dispatch,
odds request or publication was performed.
