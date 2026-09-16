# NFL remaining-family investment comparison (zero-credit)

**Date:** 2026-09-16. **Status:** ANALYSIS ONLY — zero API credits spent,
zero outcomes inspected, no historical returns used.

**Scope:** H-N3 (rest/travel → 1H) retired on power grounds 2026-09-15;
H-N6 (officiating crews → totals) retired INFEASIBLE 2026-09-16
(eligible N=365 < 700; `docs/nfl-h-n6-retirement.md`). Both are excluded
and permanently closed under their specifications. The six original
sketches were scored in `docs/nfl-backfill-investment-comparison.md`
(2026-09-15); this note reassesses only the survivors on current
evidence, with H-N2 re-examined against the now-validated IEM MOS
infrastructure.

**Statistical-capital framework:** each preregistered family test spends
one family-wise slot (α=0.05, Holm across active families). Families
retired on feasibility or analysis (H-N1, H-N3, H-N4, H-N5, H-N6) consumed
**zero** alpha — none was tested. The next tested family is the first
spender.

**Economic bar (precedent):** H-N6 required ≥ +0.005u/game incremental.
A family must show a mechanically plausible path to clearing that bar
net of vig, or it is not worth a slot.

---

## H-N2. Forecast wind → totals — REASSESSED: VIABLE, preregister next

**Hypothesis / mechanism.** Pregame *forecast* sustained wind at the
stadium degrades passing efficiency and kicking reliability, so realized
totals fall short of the market total on high-wind games. Information
set: IEM MOS GFS forecast, mechanically the latest cycle with
`runtime + 4h ≤ kickoff − 24h` (frozen archive rule v1 — timestamps only,
never chosen by fit). Directional, pre-specified: higher forecast wind →
negative residual (realized − market). Honest prior: this is the
most-cited weather edge in betting literature, so the base rate of it
being fully priced is high; the testable question is strictly
*incremental* information in the forecast-wind series beyond the market
total, not whether wind matters physically.

**Required market / data.** `totals` (featured market) + IEM MOS wind
(`wsp`, knots) forecasts. No new market pull: the design must use frozen
snapshots.

**Existing-data coverage.** Totals: full frozen scope (291,586 rows,
162 snapshots, Wed/Sat/Sun cadence, 2022–2024). MOS: IEM GFS MOS archive
covers 2003→realtime (free, no key); point-in-time semantics proven with
a real 2023-10-01 KBUF retrieval (`docs/nfl-weather-archive-spike.md`,
verdict AVAILABLE). Stadium→MOS station mapping under construction by
the archive agent (verified against the IEM station list); the
prospective 2026 archive is accumulating under frozen v1 rules.

**Eligible-event estimate.** 816 REG games × ~65% outdoor ≈ 530 ×
totals consensus at the latest pre-kickoff frozen snapshot (~80%
any-slot rate from the H-N6 gate) ≈ 424 × MOS-station mapping (~95%
expected) ≈ **400–450**. Exact count is a read-only computation at
prereg freeze (mapping pending).

**Minimum detectable, economically plausible effect.** Residual SD of
(realized total − market total) ≈ 13.5 pts. Continuous-wind slope
design: MDE ≈ 2.8·σ/(sd_wind·√N) ≈ 2.8·13.5/(5·√425) ≈ **0.37 pts/mph**.
A 15-mph forecast wind then implies ~5.5 pts — at the upper edge of
published wind effects (1–4 pts for high-wind games). High-wind-subset
design (top quintile, N≈85): MDE ≈ 4.1 pts mean shift — detectable only
if the true effect ≥ ~4 pts. Economic translation: the wind adjustment
must move the fair total by ≥ ~1.5 pts on bettable high-wind games, net
of vig, to clear the +0.005u/game bar. The test is powered for large
effects only; the slope design dominates the subset design on power.

**Required paid credits.** **$0.** Totals from the frozen dataset; MOS
forecasts free via IEM. (Any design needing non-frozen timestamps would
cost — the prereg must not require them.)

**Clean confirmation path.** (1) Historical one-shot on 2022–2024 under
the frozen MOS rule (mechanically latest eligible cycle — no
retrospective cycle shopping, ever); (2) prospective confirmation on the
2026 MOS archive now accumulating (pristine, no reconstruction). 2025
stays out of scope (not in the frozen dataset).

**Statistical capital consumed.** 0 to date. If preregistered and run,
H-N2 becomes the first family to spend one slot.

**Explicit retirement rule.** Preregister an N floor (eligible < 250 →
infeasible, no test). Primary endpoint: incremental out-of-sample
log-loss (or residual-slope t) of wind-augmented total vs the
market-total baseline, correct sign required, clustered by season.
Null or wrong-sign → family retired permanently under this
specification. No retests, no cycle re-selection, no window widening.

---

## H-N1. Injury-report shock in correlated props — reaffirm FAIL

**Mechanism.** Books reprice quickly on QB news; a residual edge would
require the book to be simultaneously fast (headline) and slow
(prop/total cross-adjustment) on the same news. Weak — no clean
information asymmetry.

**Required market / data.** `team_totals`, `player_pass_yds`,
`player_reception_yds` × 2 timestamps per event (pre-news baseline vs
post-news; the endpoint is prop CLV). Props have no provider history
before 2023-05-03 → 2023–2024 only.

**Existing-data coverage.** None — prop markets are outside the frozen
dataset; thinnest book consensus (3–5 books); wide prop vig.

**Eligible-event estimate.** ~50 genuine QB Out/Doubtful shock events
over 2 seasons (injury-filtered from free nflverse data; the prereg would
first have to define "shock" mechanically, narrowing further).

**MDE / economics.** Underpowered at any attainable N: N≈50 with high
prop residual variance cannot detect realistic cross-market lags, and
wide vig erodes whatever residual exists.

**Required paid credits.** 3,000 filtered (50 × 3 × 2 × 10); 32,640 full.

**Confirmation path.** None clean at this N.

**Statistical capital.** 0 (never tested).

**Retirement rule.** Failed 2026-09-15 on cost/mechanism; reaffirmed —
do not preregister without a genuinely new information set.

---

## H-N4. Lagged tempo vs 1H totals — reaffirm FAIL

**Mechanism.** Neutral-script pace from strictly lagged play-by-play.
Pace is publicly modeled everywhere (nflverse columns, public models) —
the market 1H total most likely already spans it. Weakest
justification-per-dollar of the data families.

**Required market / data.** `totals` + `totals_h1`. 1H markets are
outside the frozen dataset and have no 2022 history → 2023–2024 only.

**Existing-data coverage.** Tempo features free (nflverse PBP); markets
not covered.

**Eligible-event estimate.** 544 games (continuous treatment, no
filtering possible).

**MDE / economics.** Power is not the binding problem — mechanism is:
no reason to expect lagged pace to be unpriced in 1H totals.

**Required paid credits.** 10,880 (544 × 2 × 1 × 10).

**Confirmation path.** 2023→2024 walk-forward OOS log-loss vs market
1H total — clean but unjustified spend.

**Statistical capital.** 0 (never tested).

**Retirement rule.** Failed 2026-09-15; reaffirmed — do not preregister;
the information set is public and the cost is 2.4× H-N3's for a weaker
mechanism.

---

## H-N5. Alt-line key-number consistency — reaffirm FAIL

**Mechanism.** None — alt lines are algorithmically generated from the
same distribution as the main line; a systematic deviation would be a
pricing bug, not a persistent edge. No information set at all.

**Required market / data.** `spreads`, `alternate_spreads`,
`alternate_totals` × 1–2 timestamps. No pre-2023-05-03 history.

**Existing-data coverage.** None in the frozen dataset.

**Eligible-event estimate.** 544 games.

**MDE / economics.** A bug hunt, not a hypothesis test.

**Required paid credits.** 16,320 (1 ts, no-arb band) – 32,640 (2 ts,
CLV). Highest cost tier.

**Statistical capital.** 0 (never tested).

**Retirement rule.** Failed 2026-09-15; reaffirmed — permanently out;
no information edge exists to test.

---

## Ranking (feasibility / mechanism / cost only)

| Rank | Family | Feasibility | Mechanism | Cost | Verdict |
|---|---|---|---|---|---|
| 1 | H-N2 wind → totals | N≈400–450, data in hand (frozen totals + free MOS) | Real physics; high prior of being priced — test is strictly incremental | **$0** | **Preregister next** |
| 2–4 | H-N1 / H-N4 / H-N5 | — | Weak → none | 3,000–32,640 | Reaffirmed FAIL, no preregistration |

No historical returns were used. No outcomes were inspected. The ranking
uses only: data already held, free point-in-time sources, eligible-N
arithmetic, mechanism plausibility, and credit cost.

## Smallest preregistrable set

**{ H-N2 } alone.** It is the only remaining family with a $0-credit
path, adequate eligible N for large-effect detection, a genuinely
point-in-time information set (now proven, not assumed), and a clean
two-stage confirmation path (frozen historical one-shot + prospective
2026 archive). Its honest weakness — the edge is famous and likely
partially priced — is exactly what the incremental preregistered test is
designed to adjudicate; a null retires it cleanly at the cost of one
alpha slot.

**Not authorized by this note:** any credit spend, any claim-bearing
test run, the H-N2 preregistration text itself (next step), or any
revisit of H-N3/H-N6 (both permanently closed under their specs).

*Analysis-only. Zero API credits spent. No picks, no model changes, no
forward-shadow contact.*
