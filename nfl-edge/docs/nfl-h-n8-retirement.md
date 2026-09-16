# H-N8 retirement — red-zone/situational conversion → NFL totals

Date: 2026-09-16. Gate run: 35117722982 (success). Artifact: nfl-hn8-eligibility.

## Disposition: RETIRE-ON-POWER (terminal)

Mechanical rule from the DRAFT preregistration
(`docs/preregistrations/nfl-h-n8-redzone-conversion-totals-draft.md`, never frozen, never tested):

- N < 250 → INFEASIBLE. Eligible N = **359** (2022: 123, 2023: 120, 2024: 116) — passes the floor.
- MDE at exact N with observed feature SD: `2.4865 × 13.5 / (0.1439 × √359)` = **12.31 pts/unit** > 10.0 cap → **RETIRE-ON-POWER**.

No discordance is possible: the frozen rule is defined directly on the
observed SD of the primary feature (home-minus-away lagged offensive
RZ-TD% differential), so there is no planning-assumption vs empirical
branch to disagree.

## Outcome-blindness certification

- `outcome_rows_read = 0`, `outcomes_inspected = false`, `api_credits_used = 0`, `db_writes = 0`.
- Data sources: nflverse schedule metadata (gameday/gametime only),
  frozen totals quotes at decision slots, free nflverse play-by-play
  2021–2024 REG loaded through a strict column allowlist that excludes
  every score column (verified by unit test).
- Per-game CSV contains no score/actual/settlement/residual columns —
  only the pre-registered feature `x_primary_rz_td_diff`, trip counts,
  and mechanical funnel fields.
- Manifest v1 fingerprint verified: `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`
  (291,586 quotes, 162/162 snapshots).

## Attrition (815 bundle REG games → 359 eligible)

- no_decision_consensus (latest pre-cutoff slot, ≥3 books incl. Pinnacle): 437 — the binding constraint, same structural bottleneck as H-N2/H-N6
- no_identity: 16; no_pre_cutoff_slot: 2
- min_rz_trips (either team <12 RZ trips in trailing-8 window): 1 — the conversion-history requirement cost essentially nothing

Feature summaries (eligible games, features only): primary differential
sd 0.1439 (consistent with √2 × team-level sd 0.1011); RZ-TD% mean 0.567,
3rd-down% mean 0.394, goal-to-go TD% mean 0.726, 4th-down% mean 0.523.

## Interpretation (pre-registered, no outcome knowledge)

At N=359 the test could only detect slopes ≥12.3 pts per 1.0 of
RZ-TD% differential (≥1.23 pts per 10pp) — above the 10.0 bar the DRAFT
set as the largest effect worth chasing. The family is retired on power,
not on evidence: nothing about red-zone conversion and totals on this
dataset is known, and the DRAFT's terminal rule forbids rescue via
nearby metrics (3D%, GTG-TD%, 4D%), composites, nearby trip minimums or
lags, defensive-rate reframing, or relaxed consensus rules.

## Consequences

- H-N8 is retired on this dataset. No claim-bearing slot consumed
  (feasibility retirement consumes zero statistical capital).
- No outcomes were inspected at any point; the DRAFT was never frozen and no test was run.
- H-N8's distinction from retired H-N4 (conversion efficiency vs pace/tempo) stands as documented; both families are now closed.
