# NFL historical featured-market research — retrospective (2026-09-16)

**Status:** CLOSED. Both historical featured-market research families (H-C pressure →
`player_pass_yds`, H-D precipitation → attempts) are dead on the current historical
dataset. No new NFL historical feature families; no new NFL alpha lane opened.
**Source docs:** `nfl-edge/docs/nfl-h-c-feasibility.md` (C feasibility, NO-GO),
`nfl-edge/docs/nfl-h-d-feasibility.md` (D feasibility, CONDITIONAL GO),
`nfl-edge/docs/nfl-h-d-pull-incident-2026-09-16.md` (v1 incident), `nfl-edge/docs/nfl-h-d-retirement.md`
(RETIRED-ON-POWER, terminal). This doc summarizes; it amends nothing.

---

## 1. H-C (OL/DL pressure → `player_pass_yds`): dead on coverage

- The frozen design required test on 2024 (2023→2024 walk-forward).
- nflverse NextGen `avg_time_to_throw` — a required input in the frozen §1.3
  information set — has zero 2024 coverage: `ngs_2024_passing.csv.gz` = **4 stub rows**
  (week 0/1, Mahomes/Jackson only) vs 603 (2022) / 620 (2023) full rows. No 2025
  NextGen assets exist. Corroborated upstream (nflverse-data#77, nflreadpy#27).
- Free feasibility funnel: N_test (2024) = **32 units** (all 2024 Week 1, whose lag
  week is 2023 W18; under a strict same-season reading N_test = 0). 2023 train side
  was healthy (490 units) — the failure is specific to the 2024 test set.
- Power (frozen constants, σ_res = 40 yards): MDE ≈ **17.6–18.5 yards/SD** vs the
  plausible-effect bar of ~6–7 yards/SD. 275 test units needed for MDE ≤ 6.0;
  ~29 expected usable. Underpowered by roughly 10× in N. Terminal constraint:
  the dead feed, not sample size (counterfactual: feed alive → 512 units → MDE 4.40).
- Credits spent: **0**. Outcomes inspected: **0**.
- **Status: retired. Do not reopen on the current historical dataset.** Do not amend
  to a charting-only pressure index (dropping NextGen) unless the user explicitly
  authorizes that methodology change — that is a spec amendment, not a feasibility
  decision. One family-wise slot (α=0.05, Holm) was spent on H-C.

## 2. H-D (precipitation → `player_pass_attempts` + `player_rush_attempts`): dead in execution

- Free feasibility was a genuine GO: 60 treated games (2023: 34, 2024: 27; 1 lacks Odds
  event id), frozen 1,200-credit ceiling (~1,020–1,080 expected), 51–54 usable
  units/endpoint, MDE_rush 1.12 / MDE_pass 1.45 (frozen constants σ_res = 3.5/4.5),
  all above the 25-per-endpoint floor. Pull authorization followed.
- Execution failure, in order:
  1. **Snapshot-scoped event IDs.** v1 pull (run 35129443585) used game-day-era IDs
     directly from the frozen manifest: **78/80 manifest IDs were first observed in
     provider snapshots dated AFTER their game's T_dec** (77 one day after, 1 two
     days after). The historical endpoint returns HTTP 200 + empty bookmakers for an
     ID not yet issued at the queried date. **0/60 events returned quotes; ~800
     credits for zero data — a plumbing/data-identity failure, NOT evidence** (audit
     trail preserved in `nfl-h-d-pull-incident-2026-09-16.md`).
  2. **Per-call cost mis-estimate.** The corrected v2 pull (run 35132159919, free
     validation first) resolved **60/60 IDs** via T_dec-era events lists — the era
     fix works — but observed **≈18.6 credits per odds call** vs the assumed 10
     (47 lookups cost exactly 1 each, as assumed). True 60-event cost ≈ 1,163, not
     ~647. The 700-additional-credit guard (user-authorized 14:02 EDT) aborted the
     pull after 33 quoted + 2 empty events; **all 25 skipped events were 2024 games**
     (manifest is chronological, guard hit in order). **697 additional credits**;
     book depth healthy where data exists (5–8 books/event).
  3. **Frozen stop rule fired.** Usable N pass attempts = 25, rush attempts = 23 <
     25-per-endpoint floor → **RETIRED-ON-POWER, terminal** (2026-09-16 14:10 EDT).
     Frozen prereg and statistical spec unchanged throughout.
- **No confirmatory test ran. No outcomes inspected. No alpha/statistical capital
  spent.** Integrity gate certified `outcome_rows_read = 0`; timestamp safety PASS
  (all request dates == frozen T_dec). One family-wise slot (α=0.05, Holm) was spent
  on H-D at freeze, consumed with no test run.
- **Status: RETIRED-ON-POWER, terminal.** No paid rescue pull after a frozen stop-rule
  failure — the corrected pull was explicitly authorized as the final attempt
  ("retire H-D without another paid rescue attempt if corrected coverage materially
  misses power").

## 3. Credit accounting (audit trail, not evidence)

~1,500 total H-D credits recorded as data/research infrastructure cost:

| Pull | Credits | Data |
|---|---|---|
| v1 (plumbing failure) | ~800 pull-attributable (1,150 quota delta incl. background) | 0 rows |
| v2 corrected (partial, aborted by guard) | 697 (header delta) | 33/60 events, 1,686 quotes |
| **Cumulative H-D** | **≈1,497 pull-attributable** (1,847 quota delta) | — |

H-C: 0 credits. v2 partial data (1,686 quotes, 33 events) is preserved in the run
artifact but is not a dataset — no statistical claim rests on it.

## 4. Permanent platform constraints (standing, user-set 2026-09-16)

1. Before any paid historical pull: resolve and validate event identity at the exact
   target snapshot.
2. Never estimate paid-pull economics from nominal/request assumptions alone —
   calibrate actual credits-per-call with a small bounded probe first.
3. Feasibility must incorporate observed provider billing behavior, empty-response
   treatment, snapshot availability, and usable-N attrition.
4. Historical featured-market research cannot proceed unless a free coverage/power
   screen passes before feature engineering.
5. No paid rescue pull after a frozen stop rule fails unless explicitly authorized
   as a methodology/data-source change.

Derived lessons folded into the above: provider event IDs are snapshot-scoped
(unless provider documentation or direct validation proves otherwise); free
validation gates catch real defects (the LA/LAR alias bug was found and fixed
pre-pull in v2); historical per-event odds cost ~18.6/call with 2 prop markets vs
the assumed 10 — validate per-call costs on 2–3 calls before projecting, not just
call counts.

## 5. Strategic outcome

- **Both C and D are dead on this dataset.** Per the standing fork rule, NFL research
  transitions away from historical featured-market backtests toward **prospectively
  collected prop/alternative-market data** (decision snapshots controlled, not
  reconstructed).
- **No new NFL historical feature families. No new NFL alpha lane** until
  (a) enough prospectively collected data supports a preregistered test, or
  (b) a genuinely new historical data source passes free feasibility.
- **NCAAF Saturday 2026-09-19 experiment untouched** — nothing in this doc touches
  NCAAF.
- Brief-first rule stands: do not automatically open new lanes — brief on what was
  learned first.
