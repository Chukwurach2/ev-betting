# H-D pull incident (2026-09-16): 0/60 events returned quotes

**Status: BLOCKED. Zero additional paid calls made after diagnosis.**
**Credits spent: 1,150 total quota delta; ~800 attributable to the pull
(80 calls x 10); remainder concurrent background usage (NFL collector,
NCAAF timing). Zero usable rows. No outcomes inspected.**

## What happened

- Workflow `nfl-hd-pull.yml`, run 35129443585 (2026-09-16T17:38Z):
  `events_attempted=60, events_ok=0, credits_consumed=1150, aborted=false`.
- Every event: all candidate provider IDs returned HTTP 200 with empty
  `bookmakers` at T_dec. Immutable artifact: `nfl-hd-pull-1`
  (run 35129443585).

## Root cause (free evidence, no additional paid calls)

The pull script used the frozen manifest's provider event IDs directly.
**78 of the 80 manifest IDs were first observed in provider snapshots
dated AFTER their game's T_dec** (77 one day after, 1 two days after;
the remaining 2 on the same calendar date as T_dec). The provider's
historical `/events/{id}/odds?date=X` endpoint returns empty bookmakers
for an event ID not yet issued at X. The manifest IDs come from the
frozen spread/total dataset's snapshots (clustered on/after game day);
the provider re-issues event IDs between T_dec (kickoff-24h) and game
day, so the manifest carries the game-day-era ID, not the T_dec-era ID.

Supporting evidence:
- The C/D probe (2026-09-16) resolved IDs via the historical **events
  list at a lookup date** and got multi-book prop depth at T_dec for
  both 2023 games. Its one failure (2024 game) was an ID "resolved from
  a later snapshot" returning zero books - the same era-mismatch
  signature.
- The probe's explicit 2023_01_GB_CHI ID (from the identity artifact)
  worked, proving identity-artifact IDs are not categorically broken -
  only wrong-era ones are.

## The fix (mechanical, question unchanged)

`nfl-edge/ops/nfl_hd_pull_v2.py` (prepared, NOT run): for each distinct
T_dec, resolve the T_dec-active event ID via
`/v4/historical/sports/americanfootball_nfl/events?date=<T_dec>`
(1 credit/lookup), match by team fragments, then query that ID's odds
at T_dec (10 credits/game). Same 60-event universe, same T_dec, same
markets, same frozen statistical spec. This is a correction of the ID
resolution implementation, not a change to the preregistered question.

## Cost and the blocker

- Sunk: ~800 credits, zero data.
- Re-pull estimate: 47 distinct-T_dec lookups x 1 + 60 odds calls x 10
  = **~647 credits**.
- Projected total: ~1,447 vs the **hard 1,200-credit ceiling** the user
  authorized. The ceiling is exceeded by ~250 even before background
  usage.
- Per the user's explicit stop rule ("stop before spending if ... cost
  may exceed 1,200, or a preregistered assumption materially breaks"),
  **no re-pull without fresh user authorization.**

## Options for the user

1. **Authorize ~650 more credits** (total ~1,450) for the v2 re-pull.
   Updates needed: workflow to call `nfl_hd_pull_v2.py`, then dispatch.
2. **Retire H-D.** The pull economics are now worse than the feasibility
   projection (~1,450 vs ~1,050 expected), and the family has consumed
   800 credits for zero data.
3. Neither the statistical spec nor the freeze is invalidated - the
   failure is entirely in paid-pull ID resolution. No outcomes were
   inspected; no statistical capital was spent.

## Files

- `nfl-edge/ops/nfl_hd_pull_v2.py` - corrected pull (events-list ID
  resolution, hard cap parameterized, NOT executed)
- Original v1 scripts retained for the audit trail.
