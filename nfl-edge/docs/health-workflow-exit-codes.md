# Health workflow exit-status handling

Run40191894839/job121164454556 returned a valid degraded JSON report:core schedule/collector/picks were healthy, research prop collector stale. GitHub runs the shell with errexit, so exit1 stopped the step before its intended warning branch.

Capture the health command status through an OR-list before classifying it. Status0 succeeds;1 emits a warning and succeeds;2 or unexpected nonzero statuses still fail. No thresholds,prop commissioning,quote freshness or model/picks logic change. A collector heartbeat is not evidence of fresh phone odds.

Validation: reproduced old failure for status1 under bash -e -o pipefail. Tested statuses0,1,2,3,127: expected exits0,0,1,1,1; warning only for1. No database,provider or credential access during testing. Existing NFL run40185958785 captured1/duplicate12/failed0/requests1,materialized0; independent phone25NFL/7NBA games with0fresh entries. No new odds request or deployment for this fix.
