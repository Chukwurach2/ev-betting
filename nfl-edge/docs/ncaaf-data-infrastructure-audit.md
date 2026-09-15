# Data Infrastructure Audit (2026-09-15)

## Status Summary

| Data | Status | Notes |
|------|--------|-------|
| Historical quotes (2022-24) | ✅ Available | Frozen, fingerprinted |
| Live quotes | ✅ Available | 15-min collector |
| Game outcomes (CFBD) | ✅ Available | Credential works, 3,747 games in 2024 |
| Weather (historical) | ❓ Unknown | Need to identify source |
| Play-by-play | ❓ Unknown | Need to audit CFBD endpoints |
| Canonical game identity | ⚠️ Partial | H5 taught us this is hard |

## CFBD Outcomes

**Tested:** 2026-09-15. `cfbd_games.py` successfully fetched 2024 regular season (3,747 games).

**Available fields include:**
- `homeTeam`, `awayTeam`, `homePoints`, `awayPoints`
- `startDate`, `completed`
- `homeLineScores`, `awayLineScores` (quarterly)
- Venue info (need to check for stadium/location)

**Next:** Test join feasibility between CFBD games and historical quotes.
Key challenge: provider_event_id ↔ CFBD game matching.

## Weather Data

**Not yet investigated.** Requirements:
- Historical hourly weather by stadium location
- Must be knowable before kickoff (forecast, not realized)
- No leakage

**Candidate sources to evaluate:**
- Open-Meteo (free, historical)
- NOAA (free, US stations)
- Visual Crossing (paid)

**Critical question:** Can we reconstruct what was *forecast* (not observed)?
Using realized weather is leakage — the market saw the forecast, not the outcome.

## Play-by-Play / Pace

**Not yet audited.** CFBD has:
- `/plays` endpoint (need to test)
- `/drive` data

**Questions:**
- Coverage for 2022-24?
- Timestamp reliability?
- Can we compute pace (plays/minute, seconds/play)?

## Canonical Game Identity

**Lesson from H5:** Cross-source game matching is non-trivial.
- Odds API uses provider_event_id (opaque)
- CFBD uses its own game IDs
- Team name variations, neutral sites, date mismatches

**Approach:** Build a mapping table using:
- Date + home team + away team (fuzzy match)
- Validate on sample before bulk join

## Priority Order

1. ✅ Outcome data — RESOLVED (CFBD works)
2. ⏳ Canonical identity — Build team/date matcher, validate
3. ⏳ Weather source — Evaluate Open-Meteo historical
4. ⏳ Play-by-play — Audit CFBD coverage

**Do not model until 2-4 are resolved.**
