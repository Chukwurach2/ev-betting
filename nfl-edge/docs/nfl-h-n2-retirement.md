# H-N2 retirement — forecast wind → NFL totals

Date: 2026-09-16. Gate run: 35057472959 (success). Artifact: nfl-hn2-eligibility.

## Disposition: INFEASIBLE (terminal)

Mechanical rule from the DRAFT preregistration
(`docs/preregistrations/nfl-h-n2-forecast-wind-totals-DRAFT.md`, never frozen, never tested):

- N < 250 → INFEASIBLE. Eligible N = **202** (2022: 69, 2023: 69, 2024: 64).
- The tree short-circuits before the MDE branch; no discordance is possible
  (empirical wind SD on eligible games was 4.79 mph — both formulations agree
  the disposition is INFEASIBLE).

## Outcome-blindness certification

- `outcomes_inspected = false`, `api_credits_used = 0`, `db_writes = 0`.
- Data sources: nflverse schedule metadata (gameday/gametime/stadium only),
  frozen spread/total quotes at decision slots, free IEM MOS archive.
- Per-game CSV contains no score/actual/settlement/residual columns.
- Manifest v1 fingerprint verified: `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`.
- 162/162 snapshots matched to decision slots; 139,804 totals quote rows read, 0 outside frozen slots.

## Attrition (815 bundle REG games → 202 eligible)

- no_decision_consensus (Wed 12:00Z slot, ≥3 books incl. Pinnacle): 262
- dome/retractable-roof exclusions: 173
- stadium→MOS mapping gaps (`alias_points_nowhere` + unmatched international): ~158
- no_identity: 16; no_pre_cutoff_slot: 1

Note for the record (not a rescue): the stadium mapping used 2026-era keys and
covered historical stadium names poorly; fixing it would raise N but the frozen
rule forbids relaxed variants or re-runs to chase feasibility. The verdict stands.

## Consequences

- H-N2 is retired on this dataset. No claim-bearing slot consumed
  (feasibility retirement consumes zero statistical capital).
- No outcomes were inspected at any point; the DRAFT was never frozen and no
  test was run. Nothing about wind and totals on this dataset is known.
- With H-N2, H-N3, and H-N6 all retired, NFL has no live research candidate.
  Next: the costed family-specific backfill plan for the remaining hypothesis sketches.
