# NFL free-plan quota guard

The confirmed plan is500 credits/month, shared with NBA. The previous NFL
workflow specified us,eu under a paid20K comment, and NFL bypassed its quota gate.
The old per-run limit was8 requests and loop reserve10. This could consume
multiple-event credits despite the bounded NBA guard.

This proposal uses one US region, at most one NFL two-market request per run,
and a50-credit loop reserve. Missing/malformed/negative quota or fewer than52
credits stops NFL before odds calls. Multi-region overrides are still charged
by the loop's actual2*region count, so the reserve survives boundary cases.
No schedule cadence, checkpoint window or quote-age acceptance is widened.
Existing NCAAF policy remains separate. A per-run cap is not a monthly global
budget or protection against independent concurrent collectors.

Provider contract: https://the-odds-api.com/liveapi/guides/v4/ . Free events
requests expose usage headers without usage cost; odds cost depends on markets
and region/book groups. No keys were retrieved or new paid request dispatched.

Validation:18 collector tests and38 checkpoint tests pass offline. Broader local
Python discovery ran598 tests with12 import/dependency errors (including missing
pandas/PyYAML); it did not pass. CI must verify the proposed branch.

At2026-10-22 around13:05ET, public phone feeds had15 NFL games/zero fresh entries
and14 NBA games/zero fresh entries after the preceding snapshot expired. Tonight's
NFL and two NBA games were checked against official schedules. EVSharps priority
shortlist still required sign-in. Coverage was recorded privately; no stale quote
became an active lean. This PR is not a deployed guard or a fresh game card.
