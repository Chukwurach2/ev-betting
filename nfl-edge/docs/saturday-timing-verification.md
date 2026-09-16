# Saturday Timing Experiment — Repair Verification (2026-09-16)

One-time experiment: Saturday 2026-09-19, NCAAF totals, 48 observation
slots (10:00–21:45 ET, every 15 min). Measurement only.

## Defect dispositions (2026-09-15 audit)

1. **Every-Saturday schedule → FIXED.** The collector hard-gates on
   `EXPERIMENT_DATE_ET = "2026-09-19"` (America/New_York) plus the
   10:00–22:00 ET window. The gate fires before ANY provider API call;
   runs on other dates exit 0 with `status=date_gate_skip` as zero-cost
   no-ops. The cron stays weekly so no manual dispatch is needed, but
   only 2026-09-19 can spend credits. Authorizing another Saturday
   requires a commit.
2. **52 vs 48 observations → FIXED.** Cron was `*/15 14-23 * * 6` +
   `*/15 0-2 * * 0` = 52 runs (through 22:45 ET). Now `*/15 14-23 * * 6`
   (40 runs, 10:00–19:45 ET) + `*/15 0-1 * * 0` (8 runs, 20:00–21:45 ET)
   = exactly 48 runs. The 22:00 ET boundary is exclusive by design.
   Verified by a unit test that expands both cron lines.
3. **Per-event cost → MEASURED + FIXED.** Live probe 2026-09-16:
   one event-odds call (markets=totals, regions=us) costs exactly
   1 credit (`x-requests-last=1`, used delta +1). The old code's
   "1 credit/snapshot" comment was wrong and the silent `games[:30]`
   cap is gone, replaced by deterministic truncation to the credit cap.
4. **Quota logging → FIXED.** `log_quota` now receives the balance from
   the free events call (before the first paid call) and the headers
   from the last paid call (after), so `credits_consumed` spans the
   complete run. Verified in a mocked 3-event run: 100 → 103 = 3.
5. **Hard credit cap → ADDED + TESTED.** Three layers, all enforced
   BEFORE any paid call: per-run cap `TIMING_MAX_CREDITS_PER_RUN=80`,
   per-day cap `TIMING_MAX_CREDITS_DAY=4000` (measured from
   `ncaaf_timing_runs`, fail-closed), and the standing NFL-protection
   reserve `NCAAF_EDGE_MIN_QUOTA_REMAINING=2000` (also re-checked
   mid-loop). Unit tests prove zero paid calls fire under each cap.
6. **Explicit snapshot states → ADDED.** Mechanical contract universe =
   Saturday's provider events × configured book universe (9 books
   observed on the live NCAAF us feed: draftkings, fanduel, betmgm,
   betrivers, betonlineag, betus, lowvig, mybookieag, bovada).
   Every (run, event, book) gets one of `available / absent /
   request_failed / unmapped` in `ncaaf_timing_book_states`; non-universe
   books are recorded, never silently dropped; unmapped-identity events
   keep their quotes but are flagged, never treated as clean.
7. **Analyzer table → FIXED.** `opportunity_lifetime.py` now reads only
   `ncaaf_timing_quotes` (+ runs/book_states), never the live
   odds-quotes table. A unit test asserts the live-table reader is gone.
8. **Reconstructable snapshots → FIXED.** New migration
   `016_saturday_timing.sql`: `ncaaf_timing_runs` (mechanical slot,
   before/after credit balances, status), `ncaaf_timing_quotes`
   (run+slot, provider `observed_at`, collector `collected_at`,
   de-vig fair probabilities), `ncaaf_timing_book_states`. The analyzer
   reports interval-censored `[lower, upper)` minute bounds from actual
   slot timestamps (e.g. `[15, 30)`), right-censoring, per-lag survival,
   repricing-while-quote-intact, and measured credit totals.

Also fixed (found during repair): the deployed collector could never
have run — `from provider_oddsapi import ...` with only `ops/` on
`sys.path` (module lives in `model/`), and `q.get(...)` on the
`Quote` dataclass (attributes, not a dict). Both repaired; the import
path is covered by a unit test.

## Dry-validation evidence (2026-09-16)

- 27 new unit tests in `nfl-edge/tests/test_saturday_timing.py`: all pass.
- Full suite: 446/448 pass; the 2 errors are pre-existing
  `test_model_integrity` / `test_quarter_model` loader errors (missing
  local `scikit-learn`, installed in CI via `model/requirements.txt`).
- Collector dry-run (`--dry-run --date-override 2026-09-19`,
  zero API/DB): gate passes on the simulated date, 70 Saturday events
  enumerated from the free events feed, 0 capped, status ok.
- Mocked paid run (2 synthetic events): devig pairing, book states
  (available/absent/unmapped), and complete-run quota delta verified.
- Analyzer on synthetic 4-slot data: signal detection, `[15, 30)`
  bounds, right-censoring, survival fractions, repricing direction
  match — all correct.
- Migration + all 8 SQL statements parse (pglast); both workflow YAMLs
  parse; cron arithmetic = 48 runs.
- CI note: the repo's CI bootstrap extracts `nfl-edge/` only, so
  `.github/workflows/` is absent there. The two cron-schedule tests
  skip in CI and run locally (same posture as `test_workflows.py`,
  whose YAML assertions only execute in dev). GitHub CI is green on
  the final commit.

## Measured cost and Saturday budget

- **1 credit per event-odds call** (totals-only, us) — measured live.
- **70 Saturday games** (ET date 2026-09-19) on the current provider feed.
- **≈70 credits per 15-min run; ≈3,360 credits for all 48 runs.**
- Caps: 80/run, 4,000/day, stand down below 2,000 remaining (NFL priority).
- Against the 20k/month production key: ~17% of monthly quota for the day.

## What remains before Saturday

- Merge this commit; confirm CI green on the push.
- The workflow's migrate step creates the timing tables on first run.
- Dispatch `saturday-timing` with `dry_run=true` once more after merge
  as a final live smoke test (zero paid calls, zero DB writes).
- After 2026-09-19 22:00 ET, dispatch `opportunity-lifetime` and read
  `/tmp/saturday_timing_analysis.json` from its artifacts.
