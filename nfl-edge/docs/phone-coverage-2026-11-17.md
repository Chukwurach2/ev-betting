# November 17 UTC phone coverage audit

The public phone feed returned zero fresh entries for both sports at
01:21:30 UTC. The NFL listing included 13 future games, starting with
[Indianapolis at Houston](https://www.nfl.com/schedules/2026/by-team/houston-texans)
at 01:15 UTC November 20. This check used existing stored data and the
public schedule.

## NFL pregame capture

The [Chargers–Ravens](https://www.nfl.com/schedules) kickoff passed at
01:15 UTC November 17. A read-only audit of its checkpoint and normalized
quote tables found 16 opener rows from November 13 at 20:45:14–20:46:51
UTC, and a T−24 miss recorded after its deadline. Neither T−3, T−90
nor Close had a checkpoint row at the time of the audit. Those windows
lack receipts and must remain distinct from a recorded miss or an
observed market with no edge. The latest visible [scheduled collector
run](https://github.com/Chukwurach2/ev-betting/actions/runs/43923553772)
started at 17:56 UTC November 16 and made zero odds requests. No current
pregame card was verified for this game.

## NBA schedule and quote coverage

The [November 17 Eastern slate](https://www.nba.com/games?date=2026-11-17)
had five games. The phone feed listed four of those, each ten minutes
after the official listed tip. A fifth phone listing was from the
previous Eastern slate. All five phone listings had zero fresh quotes.
[Portland at Minnesota on November 17](https://www.nba.com/game/por-vs-min-0022600254)
was absent. The two teams also played at Minnesota on
[November 16](https://www.nba.com/game/0022600250), and the phone feed
listed that earlier event before its tip. These are separate official
events a day apart. The older row cannot cover the later game. This
snapshot establishes a schedule omission at the time of review. It does
not establish the underlying reason or the eventual provider arrival time.

The existing [offline NBA schedule audit](https://github.com/Chukwurach2/ev-betting/pull/60)
confirmed four time shifts and one missing matchup on the saved snapshot.
All four focused tests passed. Retain official event IDs and scheduled
tips separately from provider IDs and source times. Both the coverage
comparison and the NFL checkpoint audit are retrospective. No new
prices or paper entries were recorded.

## Scheduled run at 01:40 UTC

The next [scheduled run](https://github.com/Chukwurach2/ev-betting/actions/runs/43997392036)
recorded the three previously absent Chargers–Ravens windows as missed
at 01:41 UTC, after the 01:15 kickoff. It reported one request, one
capture and a two-credit budget. The capture was the Arizona–Kansas
City opener for November 22, with 16 committed rows at 01:41:35 UTC
and source times from 01:40:58 through 01:41:18 UTC. The run summary
does not establish a current remaining quota.

A retrospective replay of the normalized checkpoint through the exact
production card code produced two opposing full-game total watches,
two entries short of the four-entry target. The estimated returns at
the archived offers were −3.61% and −3.91% with proportional de-vig,
and −3.34% and −4.18% with power de-vig on the exact reference pairs.
Both watches expired at 01:55:58 UTC. The estimates assume no
additional fees and use overlapping market references. Accepted price,
available size and live phone display were unverified. The two watches
are correlated alternatives. The replay did not enter the prospective
paper ledger.

The 03:11 UTC check returned 13 upcoming NFL listings and four NBA
listings with zero fresh entries in either sport. Portland–Minnesota
on November 17 was still absent at that check. No manual collector
request was made.
