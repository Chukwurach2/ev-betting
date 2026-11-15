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
