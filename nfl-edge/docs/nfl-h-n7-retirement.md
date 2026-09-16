# H-N7 retirement — special-teams efficiency → NFL totals

Date: 2026-09-16. Gate run: 35124744335 (success; first attempt 35117001625
failed on a `KeyError` in `build_consensus`'s empty-quotes return — fixed
by adding `n_books_at_line: 0`, commit d1d3ebdd04e0, no outcomes touched).
Artifact: nfl-hn7-eligibility.

## Disposition: INFEASIBLE (terminal)

Mechanical rule from the DRAFT preregistration
(`docs/preregistrations/nfl-h-n7-special-teams-totals-draft.md`, never frozen, never tested):

- N < 200 → INFEASIBLE: retire without running. Eligible N = **136** → **INFEASIBLE**.
- (MDE branch never reached: at the frozen planning sd_F=1.60 the cap is
  0.50; empirical feature sd 1.68, no discordance — moot.)

## Outcome-blindness certification

- `outcome_rows_read = 0`, `outcomes_inspected = false`, `api_credits_used = 0`, `db_writes = 0`.
- Data sources: nflverse schedule metadata (gameday/gametime only),
  frozen totals quotes at decision slots, free nflverse play-by-play
  2021–2024 REG loaded through a strict column allowlist that excludes
  every score column (verified by unit test).
- Per-game CSV contains no score/actual/settlement/residual columns —
  only the pre-registered features, trip counts, kicker-status fields,
  and mechanical funnel fields.
- Manifest v1 fingerprint verified: `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`
  (291,586 quotes, 162/162 snapshots).

## Attrition (815 bundle REG games → 136 eligible)

- no_decision_consensus (latest pre-cutoff slot, ≥3 books incl. Pinnacle): **406** — the binding constraint, same structural bottleneck as H-N2/H-N6/H-N8
- no_closing_consensus: 139 (of the decision-consensus survivors)
- kicker treatment: ambiguous_kicker 47, kicker_change_in_window 18,
  kicker_unknown_last_game 5 (70 total) — the DRAFT's conservative kicker
  rule cost roughly as much as the identity gaps
- week_lt_2 (trailing-window requirement): 48; no_identity: 16

Feature summaries (eligible games, features only): primary ST-difficulty
differential mean −0.15, sd 1.68, range −4.70 to 3.49.

## Interpretation (pre-registered, no outcome knowledge)

At N=136 the family cannot clear the DRAFT's 200-game feasibility floor,
so no test was run and no effect size was estimated. The family is retired
on feasibility, not on evidence: nothing about special-teams efficiency
and totals on this dataset is known, and the DRAFT's terminal rule forbids
rescue via relaxed kicker treatment, composite features, nearby lags, or
relaxed consensus rules.

## Consequences

- H-N7 is retired on this dataset. No claim-bearing slot consumed
  (feasibility retirement consumes zero statistical capital).
- No outcomes were inspected at any point; the DRAFT was never frozen and no test was run.
- All six NFL historical research families (H-N1…H-N6) plus the two
  new-lane probes (H-N7, H-N8) are now closed on this dataset. H-N4
  (pace/tempo) remains distinct from H-N7/H-N8 by construction; all three
  are retired.
