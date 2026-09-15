# Findings: NCAAF market-structure map F1 (Phase F, part a)

- Preregistration: `docs/preregistrations/ncaaf-market-structure-f1.md` (+A1)
- Run: GitHub Actions `market-structure-f1.yml` #34992616908 — **success**
- Scope: frozen dataset fp `684c58410968c440a6d7500582ac9ecf`,
  seasons **2022–2024 only** (2025 sealed and structurally excluded)
- Freeze gate: **135/135 planned snapshots matched — PASS**
- Zero Odds API credits used (read-only)

## Integrity

- 686,578 quotes read; 195,160 excluded by kickoff-season filter (all of 2025
  plus out-of-scope bowls); 0 unmatched; 208,878 selected pairs
  (one per event×window×book×market, deterministic modal-line rule).
- 333 event-seasons (~6%) dropped: provider event ids with unstable
  identity (team names or kickoff changed within the season). Conservative
  exclusion; likely reschedules/ID reuse.

## Map results

**Coverage.** 2022–23: ~17 books per event-window; 2024: ~10.5 (book
composition evolved — barstool/wynnbet/pointsbetus out, matchbook/onexbet in —
but all US majors present every season). 75–98% of event-windows have ≥5
books; totals slightly better covered than spreads.

**Disagreement (cross-book, per event×window×market).** Small and stable
across all three windows:
- Spreads: std of de-vigged fair prob ≈ 0.006 (0.6pp); typical line range
  ≈ 1 point (p90 ≈ 1.5).
- Totals: std of fair prob ≈ 0.005; typical line range ≈ 1 point.
- No window is systematically more dispersed. The market is tight at
  Wed, Fri, and Sat snapshots alike.

**Line movement (Wed → Sat).** Real and economically meaningful:
- Spreads: mean |Δ| 0.67 pts, p50 0.5, p90 1.5 (n≈30k book-level).
- Totals: mean |Δ| 1.03 pts, p50 1.0, p90 2.0.
- Consensus moves nearly as much as individual books. Entry timing matters.

**Pinnacle vs consensus.** Pinnacle sits *at* consensus, not ahead of it:
paired |pin − median| − mean|other − median| = +0.02 mean, −0.06 median;
Pinnacle is exactly the median line in ~50% of event-windows (n=11,304).
No level-deviation evidence for a Pinnacle-lead effect in this sample.

**Data-quality flag.** 2022 early-window totals contain extreme line ranges
(mean up to 5.8, p90 30.5 points) — a handful of bad book lines. Any
modeling on this sample needs an outlier rule (e.g. drop lines >4 pts from
the cross-book median) before fitting.

## What this means for the research program

Per the preregistration, this map selects nothing. Leads for *separately
preregistered* Phase H challengers:

1. **Execution/timing study.** 0.5–1.5 pt of Wed→Sat movement means the
   decision window is a first-order execution variable. A "beat the close"
   analysis (early price vs late consensus) is the natural next measurement.
2. **Outlier cleaning rule.** Required before any totals modeling; preregister
   the rule, don't tune it.
3. **Pinnacle lead/lag in *changes*, not levels.** Levels show no deviation;
   if the sharp-leader hypothesis is tested, test whether Pinnacle's
   *moves* precede others'.
4. **No window shows structurally higher disagreement** — window discovery
   will have to come from market-vs-outcome analysis (Phase F part b),
   not from dispersion alone.

## Next

Phase F part (b): market vs outcomes — needs final scores (CFBD free API key
pending from user). Then Phase G: preregistered v0 + 2022–24 walk-forward.
