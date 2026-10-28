# Core freshness incident — 2026-10-28

Health run40698220191/job122850614060 checked13:38:37UTC and returned status
`down`, exit2. Schedule sync and settlement were OK, while core collector and
picks ages were11295s and11294s. Prop collection was also stale, with zero missed
prop snapshots. This is a genuine core-freshness threshold breach according to
the current monitor; it is not the earlier exit1/degraded shell-errexit defect.

https://github.com/Chukwurach2/ev-betting/actions/runs/40698220191

The previously successful collector run40672549431/job122762731422 must not be
used as proof of fresh prices. Read its capture/duplicate/availability counts and
source timestamps separately; completed GitHub jobs and materializer backlog
checks are not quote coverage. Subsequent rendered phone checks showed14NFL
games/0entries and a DOWN health badge. NBA was checked separately.

PR40's guarded handling of exit1 deliberately continues to fail for exit2. It
must not mask this incident or widen freshness thresholds. Main is unchanged;
PR40 and the free-quota collector policy PR39 remain unmerged. No paid rerun,
credential inspection, threshold relaxation, model promotion or deployment was
performed for this diagnostic review. Current supported quote coverage remains
the delivery limitation; auth recovery is not being reclassified as a401 error.

The10:30:22UTC collector summary explicitly reports0captured,0missed,
0unavailable,0duplicates,0failed,0requests and0materialized; backlog0. Thus the
last successful job did no quote work. This is consistent with no due checkpoint
in that run, but the summary alone does not establish why there were no due
items. It does not prove an API outage, invalid key, or missed execution. Monitor
status is reported literally; schedule-window-aware monitor semantics need
separate review before diagnosing a service outage or changing thresholds.
