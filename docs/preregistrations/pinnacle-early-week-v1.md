# Preregistration: pinnacle-early-week-v1 (early Pinnacle vs close)

**Status: PREREGISTERED — no results viewed. Analysis code does not yet exist.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Dataset (frozen): fingerprint `43f853a44bf93937d85149ca5fd7241b`,
  162/162 snapshots, 291,586 quotes. Freeze verification before running:
  quote count = 291,586 and distinct snapshots = 162, else the run stops.
- Scope: FULL_GAME_SPREAD and FULL_GAME_TOTAL, 2022-2024.
- Question: does Pinnacle's EARLY-WEEK price predict the CLOSING consensus
  direction better than the early-week cross-book consensus does? This is a
  different mechanism from market-alpha-v1 Track A (which tested whether
  followers copy Pinnacle's moves in SUBSEQUENT snapshots and found nothing
  tradeable): here the benchmark is the CLOSE, not the next snapshot.

## Definitions (pre-declared)

- **Game identity:** canonical keys from the event-identity audit
  (`(home,away)` + kickoff-proximity clustering).
- **Early snapshot** per game: Wednesday 12:00 UTC snapshots with
  kickoff - 9 days <= observed_at <= kickoff - 48h; take the EARLIEST such
  snapshot having a valid Pinnacle pair at its selected line AND >= 3 other
  books with valid pairs at EXACTLY that line (exact-line comparability).
  If none -> game infeasible. (Weekday/hour checked defensively; the
  cadence guarantees Wed 12:00 UTC.)
- **Pair selection / reference selection / fair probs:** same rules as
  execution-alpha-v1 (modal line, deterministic tie-break; spreads -> home,
  totals -> Over; multiplicative de-vig from raw american odds).
- f_pin = Pinnacle fair prob at line L; C_wed = median fair prob over books
  != Pinnacle at EXACTLY line L at the early snapshot (>= 3 required).
- **Signal** s = sign(f_pin - C_wed); require |f_pin - C_wed| >= 0.005
  (half a probability point minimum signal; weaker deviations are
  "no signal", excluded from numerator AND denominator, count reported).
- **Closing snapshot** per game: last snapshot with observed_at < kickoff.
  C_close = median fair prob over books != Pinnacle at EXACTLY line L at the
  close (>= 2 required; complete-case).
- **Outcome** o = sign(C_close - C_wed). **Agreement** = (s == o).

## Tests (pre-declared, confirmatory)

1. **Direction:** one-sided exact binomial test of agreement rate > 0.5,
   alpha = 0.05. Minimum 30 signal games, else `infeasible`.
2. **Tradeability:** the Wednesday Pinnacle-lean strategy PRICED AT
   CONSENSUS (no execution shopping): bet the reference selection if s=+1
   else the other selection, entered at the Wednesday LOBO consensus fair
   prob C_wed (Pinnacle excluded), scored against C_close. One-sided
   one-sample t-test of mean per-game CLV > 0. Under the noise null the
   direction is symmetric noise and the price is consensus, so mean CLV = 0
   and this test is calibrated (verified by null simulation pre-results,
   A1 template).

**Multiple testing: Holm-Bonferroni across the 2 tests**, family-wise
alpha = 0.05.

**Practical bar:** mean per-game CLV >= 0.01 (pre-declared, same as the
other candidates).

## Decision rule (pre-declared)

- `pinnacle_early_signal`: BOTH tests significant after Holm AND mean CLV
  >= 0.01.
- `significant_but_negligible`: tests significant but mean CLV < 0.01.
- `no_signal`: otherwise.
- `infeasible`: < 30 signal games.

## Falsification (exploratory, pre-declared)

Reverse direction: does PINNACLE converge toward the early consensus by
the close? Agreement of sign(C_wed - f_pin_early) with
sign(f_pin_close - f_pin_early) at line L (Pinnacle's own close price,
where available). If Pinnacle moves toward consensus more often than away,
it weakens the "Pinnacle predicts" interpretation — reported descriptively,
does not change the verdict.

## Transparency

Point-in-time safe: the early snapshot uses only information available at
its observed_at; the close is an ex-post benchmark. Target book (Pinnacle)
never enters its own consensus. Zero API credits; read-only Neon. A
positive finding selects the hypothesis for FORWARD-SHADOW validation only
— the mandatory promotion gate remains positive CLV against actual closing
lines on forward data. Historical results never promote.
