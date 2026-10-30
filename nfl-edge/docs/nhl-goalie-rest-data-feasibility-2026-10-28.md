# NHL goalie/rest theory: source feasibility — 2026-10-28 Eastern

Metadata review only, observed2026-10-29T00:29Z. No NHL game rows downloaded,
no historical selections evaluated, and no second statistical experiment today.
This advances the cross-sport queue while the NBA replication waits for its
next registered daily slot.

## Source observation

Publisher README, inspected through the in-app browser after web-reader errors:
https://huggingface.co/datasets/minmaxxxer/nhl-closing-odds-2025-26/blob/main/README.md

The publisher labels the compilation CC BY4.0 and describes1,394games:
1,312regular-season and82postseason. It lists stable source/team IDs,
scheduled starts, final scores, consensus main spreads/totals and no-vig
probabilities. Its stated grading includes overtime and shootout. Actual
per-book prices/quote times and goalie confirmation/workload are absent from
the listed sample columns. The card's generic stage description includes
Play-In wording; verify NHL phase values against official identity rather
than importing that template.

League-wide aggregate outcomes were visible; no individual or goalie/rest
subgroup outcomes were inspected. Same compiler as the NBA sample is not an
independent benchmark. Compilation license label does not certify upstream
rights, full schedule coverage or bookmaker settlement compatibility. No raw
source rows or displayed performance table are reproduced here.

## Original mechanism to make testable

Hypothesis: a confirmed change from the expected starter to a backup interacts
with recent goalie workload, producing a full-game price adjustment that lags
the public confirmation. Fatigue alone may be fully priced; the target is
incremental information after the pre-confirmation market, not a universal
fade of every backup or every B2B team.

Before registering a statistical test, establish:
- Public, permitted, timestamped expected/confirmed goalie announcements and
  their revisions. A box-score starter known after the game cannot substitute
  for pregame confirmation time.
- Prior goalie shots/minutes/rest frozen before the decision; travel and
  opponent controls defined without outcome-driven cutoffs.
- Actual paired same-book offers immediately before/after confirmation,
  plus decision-time availability and exact event/market identity.
- A chronological development/holdout split and one fixed endpoint, with every
  tested variant retained. Do not add this to the NBA replication family.

Moneyline, puck line and regulation-only contracts need separate adapters.
NHL odds are asymmetric: do not apply the NBA screen's blanket-110 accounting
or assume50%puck-line coverage is break-even. Verify OT/shootout inclusion,
official score convention and the exact book rules before any grading.

## Decision

The free sample cannot test the proposed announcement-lag mechanism. Status:
proposed_data_blocked, not rejected and not profitable. A separately registered
closing-consensus calibration study could be feasible after coverage audit,
but would answer a different question and would not establish executable ROI.
Do not replace the mechanism with whichever subgroup looks good in this file.

Next useful NHL work is a permission/provenance assessment of timestamped
confirmation and paired-quote history, or predeclared prospective capture
using an already authorized source. No paid plan, API account, endpoint
integration or purchase was enabled. NBA and NFL phone priorities remain intact.
