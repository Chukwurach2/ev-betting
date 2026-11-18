# November 16 NBA schedule visibility

The NBA lists eight games for Monday, November 16 Eastern. At 23:46:24 UTC
on November 15, the public NBA phone snapshot contained three remaining
Sunday matchups and no Monday matchups. It returned zero fresh entries.
The first Monday tips were still about 24 hours and 14 minutes away. The
NFL phone snapshot at the same time contained no fresh entries.

The read-only audit accepts a manually reviewed list of official games and
a saved public NBA phone response. It compares exact home and away names
within 15 minutes and reports exact, shifted, missing and ambiguous
matches. Official and provider times remain distinct. That difference
window is for coverage diagnostics only. The script neither creates
provider identities nor joins quotes or results. It makes no requests.

Run it from the app directory with
`node ops/audit-nba-schedule.mjs OFFICIAL_JSON NBA_PHONE_JSON`.
The official file is an array of entries with `home_team`, `away_team` and
a timezone-aware `kickoff`. The source for this audit was the dated league
schedule. Three offline cases passed, covering time shifts, reversed
teams, duplicate provider entries and invalid timestamps or sports.

The phone snapshot establishes what the public app returned. The upstream
event response and cause of the schedule gap remain unverified. Recheck
the same Monday set after the games enter the 24-hour window and retain
the actual snapshot time. No quote collection was requested.

- [Official November 16 NBA schedule](https://www.nba.com/games?date=2026-11-16)
- [NBA public phone feed](https://nfl-edge-v2-1.vercel.app/api/game-cards?sport=nba)

## Follow-up after the 24-hour boundary

At 01:04:53 UTC on November 16, seven Monday games were within 24 hours.
The phone returned Portland at Minnesota with a provider time ten minutes
later than its official tip. Six of the seven were absent from the feed.

At 02:10:29 UTC, all eight official Monday games were within 24 hours.
The phone returned three of them, each with a provider time ten minutes
later than the official tip. Five were absent. The phone returned zero
fresh NBA entries and the contemporaneous NFL feed returned zero. The
exact cause of the partial schedule response remains unverified.

Both receipts were retained privately. The audit CLI now resolves a
symlinked entry path before checking whether it was run directly. A
fourth offline regression covers that invocation, which had returned
without a report when called through the macOS temporary directory alias.

## All eight matchups visible at 03:43 UTC

The public NBA feed at 03:43:13 UTC on November 16 returned all eight
official Monday matchups and zero research entries. The offline comparator
found eight unique matches, each with a provider start exactly ten minutes
after the official tip, and zero missing or ambiguous matches. The public
NFL feed at the same time also returned zero entries.

The five games missing at 02:10 UTC appeared by this read. This brackets
the observed recovery to the interval between the two snapshots. It
does not establish why the schedules differed, nor whether future slates
will be complete at the same lead time. The actual phone snapshot and
verification output were retained privately. The audit made no odds request.

## Listings after the official scheduled tip

The offline report also flags a scheduled-time discrepancy when the
official tip is at or before the phone snapshot and the corresponding
provider tip is still in the future. Each flag includes the number of
listed entries when the snapshot supplied its picks array. Those rows
remain outside the future-slate coverage denominator. The comparison
uses the scheduled tip. It cannot establish the actual tip, market
status, or accepted prices. Keep the phone source and reviewed official
times separate, and never reuse this schedule match to join prices.
