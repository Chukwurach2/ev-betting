# Next-season NBA data audit — 2026-10-28

## Scope and decision

Metadata review only after today's completed descriptive experiment. No second
statistical experiment, source-row download, new cutoff search or strategy
promotion was performed. The next useful study is an independently dated
replication of the fixed fade/rest lead, after its own registration.

## Candidate source

Publisher card:
https://huggingface.co/datasets/minmaxxxer/nba-closing-lines-ats-2025-26/blob/main/README.md

The publisher labels the sample CC BY 4.0 and describes 1,315 games:
1,224 regular season, six play-in and 85 playoff games, with Cup knockout
fixtures omitted. Fields include stable source IDs, scheduled starts, scores
and consensus closing handicaps. Actual offered per-book prices and exact
quote times remain unavailable in this sample. These are metadata claims,
not row-level coverage verification or independently certified upstream rights.

The README displays league-wide aggregate outcomes. Those were visible during
metadata review; no B2B subgroup results or individual rows were inspected.
Therefore call this an independently dated replication candidate, not a
completely blind external holdout. Same compiler/method also means it is not
independent source validation. Freeze this disclosure in any next protocol.

## Gates before evaluation

1. Register the fade/rest follow-up in the experiment registry before reading
   rows. Keep the previously inspected 2024–25 season as development evidence.
   Do not run another substantial experiment today.
2. Preserve the fixed population: regular season, exactly one B2B team,
   consecutive Eastern dates and 18–36 elapsed hours, opponent at least 48
   hours rested, half-point spreads, one observation per game. No new
   travel/odds/workload filters selected using the first season's results.
3. Freeze exclusion of all December 2025 candidates before outcomes, given
   the reported Cup omissions. Audit missing games around every retained
   candidate; do not infer complete schedules from row count alone.
4. Audit identity, dates, duplicates, score/ATS reconciliation, timezone,
   exact schema and source hash. The existing helper hard-codes December
   2024: do not reuse it silently on 2025–26. Add explicit season-aware
   exclusion configuration and focused tests before running the study.
5. Predeclare uncertainty that accounts for dependence, retain all outcomes,
   and distinguish calendar-week resampling from team clustering. A Wilson
   interval alone is descriptive. Do not claim execution profit using a
   blanket assumed price.
6. Require actual decision-time eligible-book offers, settlement and complete
   availability inputs before prospective picks. This source cannot unblock
   the original T−60 feature model or validate a calibrated 60% probability.

No raw rows, subscriber data, account details or hypothetical future picks are
published here. No purchase, account creation or odds-provider credits used.

## Guard added before any next-season evaluation

The existing helper now rejects missing or non-2024–25 season labels before
ATS grading, and rejects calendar dates outside July 2024–June 2025 even if a
later season is relabeled. This prevents silently applying December-2024-only
exclusions to a 2025–26 input. The helper remains bound to the original screen;
no configurable follow-up evaluator or second experiment is introduced.
Eight focused tests passed, including mixed-season/missing-label rejection.
Replay of the previously inspected source produced an exactly identical full
report and source hash. Existing results and selection rules were not changed.
A later protocol still requires its own season-aware implementation and tests.
