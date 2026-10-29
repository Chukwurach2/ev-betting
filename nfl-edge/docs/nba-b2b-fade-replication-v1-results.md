# NBA fade/rest replication — 2026-10-29

The fixed next-season rule went **97–99 (49.49%)** across196half-point decisions.
The earlier2024–25 comparator went106–79(57.30%); that lead was not reproduced.
The registered four-week circular-bootstrap95%interval is42.28–59.06% and spans
both50% and the hypothetical-110break-even rate52.38%. No positive edge is
established. Do not turn either historical hit rate into a game-level probability.

## Registration and evaluation trail

- Protocol/registry committed before source rows:24efc4072eb987639ebc46573c58a2b05792d426.
- Frozen uncertainty implemented before rows:fba10b4972ea370e1da1a2a6e0b140d011978027.
- Fixed season eligibility, Cup coverage guard and tests committed before
  download/evaluation:af5db649c7d199c8270a83aebdccafb47f022003.
-18focused tests passed, including original-season guards and synthetic
  rest/side/December/January coverage tests. Original2024–25report replays
  exactly unchanged. Implementation CI40875527869passed.
- One substantial experiment on2026-10-29Eastern. No additional subgroups,
  thresholds or rescue variants searched after results.

## Data and audit

Source:
https://huggingface.co/datasets/minmaxxxer/nba-closing-lines-ats-2025-26
Publisher license labelCC BY4.0; all upstream rights/provenance not independently
certified.326818bytes;SHA256
9ff4950cd96dc97da58dd64632444e5865d39f1e02a0673533a924f895d1ecae.
1315rows,30team IDs,1224regular-season rows,91postseason rows; unique game IDs.
The parser checks dates/timezones, season identity, score/handicap arithmetic
and every publisher ATS label/margin. Contradictions fail rather than repair.

191December2025candidates excluded before outcomes for known omitted Cup games.
The official release places knockout games in December:
https://pr.nba.com/2025-26-nba-regular-season-schedule/
All January source-listed priors passed the guard requiring a date after
December16, isolating that known blind spot. Other exclusions:16first-prior
unavailable,821rest-population failures,91postseason.196eligible+1119excluded
reconcile to1315. These are fixed-order exclusion counts.

The original official release assigns only80games per team and cannot certify
the final complete schedule or all rescheduling. This is a descriptive result
on the available compilation, not independently certified full-season coverage.
League-wide README summaries were visible before registration; subgroup rows
were not inspected before the protocol. Same compiler across seasons is not
independent-source confirmation.

## Fixed rule and uncertainty

Exactly one B2B team: consecutive Eastern dates and18–36elapsed hours since
its prior exact team-ID game. Opponent at least48hours rested. Fade the B2B
team at the exact opposite half-point closing spread, regardless favorite
status; one game per observation. No fitted model or60%selection threshold.

196decisions across21nonempty weeks/25calendar weeks pass the registered
100decision/20nonempty-week floor.20,000valid circular four-week bootstrap
replicates, seed20261029,0empty skips. Game-weighted rates, preserved empty
weeks and nearest-rank quantiles follow the protocol. Wilson95%42.57–56.43%
is secondary/descriptive. Neither method fully handles season-long team
dependence; no confirmatory p-value or production certification.

Blanket-110fixed1unit sensitivity gives-10.8181818units/-5.51948%ROI.
This is counterfactual arithmetic, not actual-price profit: sample lacks
offered-book execution quotes/times, limits, fees and measured CLV.

## Decision

Retain the disappointing result. No promotion or prospective picks from this
simple fade rule, no pooling the two seasons to rescue it and no tuning cutoffs
on the inspected replication.2025–26is now inspected descriptive evidence
for this family. A modified theory needs newly registered rules and genuinely
new evidence. The originalT−60rest/travel model still lacks decision-time
feature/price history and has not been tested here. Raw data and selections
remain private; only original code and aggregate evidence are published.
