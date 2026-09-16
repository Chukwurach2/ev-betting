# H-D retirement (2026-09-16): RETIRED-ON-POWER, terminal

**Disposition: RETIRED. Permanent under this specification. No rescue variants.**
**No outcomes inspected. No confirmatory test run. outcome_rows_read = 0.**

## Binding reason

Frozen stop rule (`nfl-h-d-precipitation-attempts-frozen.md`):
"the confirmatory tests run only if usable N ≥ 25 per endpoint;
below that the family is parked/retired on power without outcome exposure."

Integrity gate (`nfl_hd_integrity.py`) on the corrected pull:
- **usable N pass attempts = 25** (meets floor)
- **usable N rush attempts = 23 < 25** → stop rule triggers retirement
- Gate verdict: **FAIL** (also: pull aborted by ceiling guard)

Per the frozen pass/fail rule: "FAIL (retire): ... usable N < 25 per
endpoint at the stop rule. Family retired permanently under this
specification." Per the user's 14:02 EDT authorization §4: "If corrected
coverage materially misses the preregistered power requirement, retire
H-D without another paid rescue attempt."

## What happened

1. **Freeze** 2026-09-16T17:45Z: prereg frozen with user-authorized
   defaults (both-endpoint PASS, best-available T_dec price,
   treated-only, retractable-roof wholesale exclusion, pushes residual
   0 / stake returned). 60-event manifest, 1,200-credit ceiling.
2. **v1 pull** (run 35129443585): 60/60 attempted, **0/60 quotes**.
   Root cause: manifest carried game-day-era event IDs; provider
   re-issues IDs between T_dec and game day. ~800 credits, zero data.
   Classified as plumbing/data-identity failure, not evidence.
   (nfl-edge/docs/nfl-h-d-pull-incident-2026-09-16.md)
3. **User re-authorized** 2026-09-16 14:02 EDT: corrected re-pull, hard
   **700-additional-credit** ceiling, no broadening, retire without
   another paid rescue if power materially missed.
4. **Phase 1 free validation** (zero credits, zero outcomes): all
   checks passed after fixing one genuine v2 defect found by the
   validation — the manifest uses nflverse abbr "LA" (Rams) vs the
   provider's "LAR", which would have silently failed ID resolution
   for 2 games (`2023_14_LA_BAL`, `2024_15_LA_SF`). Fixed with a
   manifest-side alias; both games resolved successfully in the pull.
   Also added per-call spend guard with abort, `request_date` audit
   field, and parameterized the integrity gate's ceiling/band.
   (commit f17ba4e38a3d)
5. **v2 corrected pull** (run 35132159919, 2026-09-16T18:04:58Z):
   - **60/60 event IDs resolved** via T_dec-era events lists —
     the era fix works.
   - 33 events returned quotes, 2 resolved but empty bookmakers,
     **25 skipped by the ceiling guard** (all 2024 games; manifest is
     chronological, guard hit in manifest order).
   - **697/700 credits consumed** — ceiling respected, abort worked.
   - Cost-model failure: observed ~18.6 credits per odds call vs the
     assumed 10 (47 lookups cost exactly 1 each, as assumed). True
     60-event cost ≈ 1,163 — nearly double the 647 projection.
6. **Integrity gate**: FAIL (aborted pull; rush N below floor).
   Timestamp safety PASS (all request dates == frozen T_dec).
   Book depth healthy where data exists (5–8 books/event).

## Credit accounting

| Pull | Credits | Data |
|---|---|---|
| v1 (plumbing failure) | ~800 pull-attributable (1,150 quota delta incl. background) | 0 rows |
| v2 corrected (partial) | 697 (header delta; ~133s run, negligible background) | 33/60 events, 1,686 quotes |
| **Cumulative H-D** | **~1,497 pull-attributable** (1,847 quota delta) | — |

## Why not another rescue

Completing the 25 skipped events needs ~25 × 18.6 ≈ 465 more credits —
a third paid attempt the user explicitly ruled out. The achieved sample
(32×2023 + 1×2024 usable-bound) cannot support the frozen pooled
2023+2024 design regardless.

## Statistical capital

One family-wise slot (α=0.05, Holm) was spent at freeze; consumed with
no test run. No endpoint-level alpha spent (test never ran).

## Platform lessons (standing)

1. **Event IDs are snapshot-scoped** (user's standing fix, 2026-09-16):
   resolve at the decision snapshot; never inherit from later manifests.
   Validate identifier persistence before any paid pull.
2. **Validate per-call costs, not just call counts**: the historical
   per-event odds endpoint cost ~18.6/call with 2 prop markets vs the
   assumed 10. Future pull budgets must probe actual per-call cost on
   2–3 calls before projecting.
3. **Free validation gates catch real defects**: the LA/LAR alias bug
   would have cost 2 games' resolution silently.

## Files

- `nfl-edge/docs/preregistrations/nfl-h-d-precipitation-attempts-frozen.md`
  (frozen; unchanged)
- `nfl-edge/docs/nfl-h-d-pull-incident-2026-09-16.md` (v1 incident)
- `nfl-edge/ops/nfl_hd_pull_v2.py` (corrected pull, as-run)
- Pull artifact: GitHub run 35132159919, `nfl-hd-pull-v2-2`
- Integrity report: run-local (verdict FAIL, outcome_rows_read 0)

*Retired 2026-09-16. No new NFL family opened as a result (standing rule).*
