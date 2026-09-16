# H-C (OL/DL pressure → `player_pass_yds`) — free feasibility gate

**Date:** 2026-09-16. **Status:** FEASIBILITY ONLY — no pull authorized, no credits spent, no outcomes inspected, no prereg frozen.
**Scope:** `player_pass_yds` ONLY (the `player_sacks` leg is dead per the 2026-09-16 probe — absent at T_dec in both probe games; not counted, not priced here).
**Credits spent producing this document: 0. Outcome columns read: 0** (every source loaded with an explicit column whitelist; score/yardage columns asserted absent).

**Method:** `nfl-edge/ops/nfl_hc_feasibility.py` (free; runbook in §8). All counts below are its output.

---

## 1. Verdict: NO-GO

**Binding constraint: NextGen `avg_time_to_throw` — a required input in the frozen §1.3 information set — has zero 2024-season coverage in nflverse.** The nflverse NextGen feed ends after 2023:

- `ngs_2024_passing.csv.gz` = **4 stub rows** (week 0/1, Mahomes/Jackson only) vs 603 (2022) / 620 (2023) full rows.
- No 2025 NextGen assets exist at all in the `nextgen_stats` release.
- Corroborated upstream: nflverse-data#77 ("2025 NFL NextGen Stats data missing") and nflreadpy#27 ("load_nextgen_stats() returning empty dataframes for 2023, 2024 and 2025").

The frozen design requires **test on 2024** (2023→2024 walk-forward). With no 2024 NextGen inputs, the 2024 test set cannot be built: **N_test = 32 units** (all 2024 Week 1, see §3) → **MDE ≈ 17.6–18.5 yards/SD** vs the plausible-effect bar of ~6–7 yards/SD. **275 test units are needed for MDE ≤ 6.0; ~29 are expected usable.** Underpowered by roughly 10× in N. No rescue is available to this gate (see §6).

---

## 2. Eligible-N funnel (every step counted free)

Unit = (game, designated starting QB). Designation rule applied: unique week t−1 offensive snap-share leader among QBs (position `QB`, `offense_snaps > 0`); ties/missing → censored. Lag window (mechanical): same-season weeks < t; week 1 → prior-season week 18.

| Step | 2023 (n=272) | 2024 (n=272) | Notes |
|---|---|---|---|
| F0: REG games | 272 | 272 | nflverse schedules, REG only |
| F1: identity match (≥1 Odds event id, frozen layer) | **272** (100%) | **256** (94.1%) | 16 unmatched = all **2024 Week 18** (absent from the frozen backfill; gamedays 2025-01-04/05). The 3 provider date-quirk events each match under a second, correctly-dated id — no game lost. Multiplicity (unique ids): 333 games × 1 id, 195 × 2 ids = 723 pairs; no id maps to >1 game |
| F2: feature-available (charting ∧ NextGen ∧ snaps, both teams) | **245** | **16** | §3 |
| F3: units with unique week t−1 QB leader | **490** (245×2, 0 ties) | **32** (16×2, 0 ties) | Designation is clean wherever inputs exist |

**N_test (2024, free funnel) = 32 units. Expected usable at the probe-based ~90% prop-coverage assumption = 28.8 ≈ 29.**

## 3. Where the funnel fails (censored, never imputed)

Full per-game censor list: `nfl-h-c-censored-games.csv` (283 rows).

- **2023 (27 games censored, 32 team-games):** all on **snap_counts** — the lag week (t−1) was the team's **bye week**, so no week t−1 snaps exist to designate a leader (e.g. `2023_06_SEA_CIN`: SEA on bye in W5). The proposed designation rule has no bye fallback (its t−2 tie-break covers ties, not byes); censored conservatively. Charting and NextGen pass for every 2023 game after the `LAR`→`LA` abbreviation fix (NextGen uses `LAR`; nflverse uses `LA`).
- **2024 (240 games censored):** **NextGen missing on 450 of 512 team-games** — the dead feed (see §1). The 16 surviving games are all **2024 Week 1**, whose lag week is 2023 W18 (feed alive). Their pressure lags are ~8 months stale; under a strict same-season reading N_test = **0**. Either reading gives NO-GO. (Snaps: 32 team-games bye-censored, same mechanism as 2023.)
- **FTN charting** (`n_pass_rushers`, `n_blitzers`, `is_qb_out_of_pocket` — verified present per-play in the separate `ftn_charting` release, **not** in `play_by_play`) and **snap_counts** are otherwise complete for 2022–2024.

**Isolation check:** the 2023 *train* side is healthy (490 units). The failure is specific to the 2024 *test* set. Counterfactual: if 2024 NextGen existed and all 256 identity-matched 2024 games were feature-available → 512 test units → MDE = 4.40 ≤ 6.0 → the frozen design **would** be viable on power. The binding constraint is the dead feed, not sample size.

## 4. Power / MDE (frozen constants)

MDE_β = 2.4865 × σ_res / √N_test, σ_res = **40 yards** (frozen planning assumption, never estimated from outcomes).

| N_test | MDE (yards per 1-SD of mismatch) |
|---|---|
| 32 (free funnel) | **17.58** |
| 28.8 ≈ 29 (expected usable, ×0.90 coverage) | **18.53** |
| 275 (**required for MDE ≤ 6.0**) | 6.00 |
| 512 (counterfactual: 2024 feed alive) | 4.40 |

Verdict vs the plausible-effect bar (~6–7 yards/SD): **not viable** — the MDE is ~3× the bar; ~10× more test units than available would be needed.

## 5. Timestamp safety

**(a) Pressure inputs — lagged to weeks < t, published post-game, strictly pre-T_dec.**

| Input | Publication cadence (observed) | Known by T_dec = kickoff−24h? |
|---|---|---|
| FTN charting | nflverse pulls each week's charting the following **Wednesday ~12:30 UTC** (in-season stamps: 2024 W10→11-13, W11→11-20, W12→11-27, W13→12-04, W14→12-11, W15→12-18, W16→12-25/26, W17→01-01, W18→01-08). `date_pulled` column is the evidence. | Yes for Sun/Mon/Sat games (days of margin). **Thursday edge — see below.** |
| NextGen time_to_throw | Weekly while the feed lived (through 2023) | Yes (moot — feed dead for 2024) |
| snap_counts | Weekly (Tuesday AM ET rebuild) | Yes |

**Thursday-game edge** (T_dec = Wednesday ~8:15pm ET; lag week t−1 charting is pulled Wednesday ~7:30am ET → ~12.5h margin *when the pipeline runs on schedule*). 36 Thursday games with week > 1 in 2023–2024:

- **9 verifiably published by T_dec** (all 2024 in-season stamps; e.g. 2024 W10 charting pulled 11-13 12:29 UTC vs W11 Thursday T_dec 11-14 01:15 UTC).
- **2 verifiably NOT published by T_dec** — genuine pipeline delays: 2024 W6 charting arrived **2024-11-26** (40 days late; W7 Thursday game 2024-10-17), 2024 W8 charting arrived **2024-11-30** (30 days late; W9 Thursday game 2024-10-31).
- **25 unverifiable** — archive re-pulls destroyed first-publication evidence (all of 2023 re-stamped 2024-09-06; some 2024 weeks re-stamped 2025-09-01).
- W1 Thursday kickoff games (2023 KC-DET, 2024 BAL-KC): lag = prior-season W18, published months prior — no edge.

Consequence: the feature-availability counts in §2 are an **upper bound** — an implementation gate must enforce as-of-T_dec publication awareness and censor Thursday (or any) games whose lag-week inputs were not yet published. This does not change the verdict (the binding constraint is NextGen, not charting latency), but it is a required prereg implementation rule.

**(b) Prop quotes** are snapshots ≤ T_dec via the provider `date` parameter (closest snapshot at or before the supplied timestamp) — safe by construction. No lookahead possible.

**(c) Final injury designations** (~kickoff−48h: Friday for Sunday games, Wednesday for Thursday games, standard NFL procedure) strictly precede T_dec = kickoff−24h.

## 6. Why no rescue

- **Drop NextGen from the index** (charting-only pressure): changes the frozen §1.3 information set. That is a **spec amendment requiring user approval**, not a feasibility-gate decision. Not done here.
- **One-season (2023-only) pull**: cannot answer the preregistered question — the frozen design is **2023→2024 walk-forward (fit on 2023 residuals, test on 2024)**. A 2023-only test has no held-out season. Stated explicitly: not proposed.
- **Relaxed lag definitions** (e.g. prior-season-only NextGen for all 2024 games): mechanically "weeks < t" but ~8–20 months stale — no prereg could defend these as measuring current OL/DL pressure mismatch. Rejected.
- **Composites / nearby endpoints**: forbidden by the terminal retirement rule; not considered.

## 7. Minimum-cost pull proposal (NOT authorized — priced only)

If the binding constraint were resolved (spec amendment + user approval), the smallest pull answering the frozen question:

- **Events: 261** (245 from 2023 + 16 from 2024) — free-funnel survivors only (identity ✓, features ✓, ≥1 designatable QB), not all 544.
- **Exactly 1 canonical event id per game** (not all 723): prefer the `alias_date` (exact-date) match; tie-break lexicographically smallest id. Deterministic, outcome-blind. Full list: `nfl-h-c-pull-events.csv` (261 unique ids, 261 unique games).
- **1 market** (`player_pass_yds`), `regions=us`, `date` = per-game T_dec.
- **Credit ceiling: 261 × 1 × 10 = 2,610.** Expected actual at ~90% coverage (empty responses are free): **≈ 2,349**. (The ~90% is a planning assumption from the 2-game probe, which showed 8–10-book US depth at T_dec; no free source counts historical prop coverage for all events — the pull itself is the exact coverage screen.)
- **Expected usable N_test: ≈ 29** (32 × 0.90) — the number that fails §4.
- **What minimality cut:** sacks leg dropped (dead); one canonical id per game instead of all matched ids (723 → 261); only free-funnel survivors instead of all 544 games (544 → 261); single market; single region. Both seasons are required — a one-season pull cannot answer the frozen walk-forward (§6).

**Do not execute this pull.** The NO-GO verdict stands independently of cost.

## 8. Reproducibility

- Script: `nfl-edge/ops/nfl_hc_feasibility.py` — inputs: `/tmp/nflverse_data` (nflverse releases: `pbp` not needed; `ftn_charting`, `snap_counts`, `nextgen_stats` 2022–2024 parquets/CSVs; `schedules/games.parquet` with 6 whitelisted columns), the frozen identity artifact (`canonical-identity-nfl` from run 35041420559). Output: this doc's numbers + `nfl-h-c-feasibility-summary.json`.
- Companions: `nfl-h-c-pull-events.csv` (261 canonical event ids), `nfl-h-c-censored-games.csv` (283 censored game rows with reasons).
- Credits spent: **0**. Outcomes inspected: **0** (column whitelists asserted in code).

## 9. What could unblock H-C (not a gate decision)

1. User approves amending the frozen §1.3 information set to a charting-only pressure index (NextGen removed) — then re-run this gate: 2023 train 490 units; 2024 test would need recounting without the NextGen leg.
2. A free, point-in-time-safe weekly time-to-throw source for 2024 appears (none identified; not searched — out of scope for this gate).
3. The prereg is rewritten around a different testable question — that is a new family, not H-C.

Until one of these happens **by explicit user approval, H-C stays NO-GO. Do not re-open via relaxed thresholds, composites, or one-season redesigns.**
