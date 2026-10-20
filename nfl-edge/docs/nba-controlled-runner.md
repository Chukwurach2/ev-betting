# Controlled NBA phone runner

GitHub Actions workflow `NBA bounded phone quote collection` connects the existing
quota-checked collector to existing Actions secrets. It has **no schedule, push
trigger or PR trigger**. Run workflow on main and explicitly select
confirm_quota_use to permit one spreads/totals request, budgeting two credits.
Leaving confirmation false or selecting another branch skips the job.

The workflow serializes its own runs, reads the exact workflow commit without
resolving unrelated submodules, installs runtime dependencies with lifecycle scripts disabled, runs offline NBA
tests, and then executes the collector with server-side secrets. The collector's
free-events preflight must authenticate and report enough credits to leave50.
It does not purchase credits, retry requests or activate continuous polling.
This lock is not a quota reservation against NFL/college/prop workers.

Migration020_nba_phone_quotes.sql must already be applied; it was installed in
the initial NBA phone release. No migrations or personal journal writes occur
in this runner. The final step checks the actual public NBA card response and
reports fresh lean counts, including an explicit no-supported-cards result.
Provider quote age and current reference support still govern availability.

Do not call the pipeline live until a real run has completed and fresh quotes
and phone cards are verified. This workflow is only a manual controlled path;
private original-research/player-prop publication remains separate work.
