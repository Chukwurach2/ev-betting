# NFL quota guard release — 2026-11-02

PR39 merged after exact-head hosted validation:95Python regressions passed
at b198cc4fd8c3e533c26fc1932d25d59d7f47bfc3, including the NFL quota-header
module now present in focused CI. The matching Vercel preview was READY.
Merge commit:3cf60a23081b2bd07a8de6fb52877f8e8e89103b.
https://github.com/Chukwurach2/ev-betting/pull/39
https://github.com/Chukwurach2/ev-betting/actions/runs/41390203742

Canonical main now uses one US region, a known-quota gate,50-credit reserve
and one NFL request per run. This reduces the prior paid-plan assumptions;
it is still a per-run policy, not a global monthly allocator or shared NFL/NBA
lock. Existing hourly cadence, quote-age and checkpoint windows are unchanged.
No live wagering or model promotion was enabled.

## Production verification

Project nfl-edge-v2-1, team nfl6, production deployment
dpl_bdVg4KjLZkwVnFw6s5MLeqYm7hfm is READY at the exact merged commit,
with nfl-edge-v2-1.vercel.app assigned. Git-triggered build completed in about
ten seconds. No manual provider collector was dispatched as part of release.

Public phone shell and both documented sport feeds were read. At14:53Z:
NFL returned16upcoming games and NBA15; neither returned a fresh research
entry. Tonight's first stored NFL schedule entry is CHI at SEA,
2026-11-03T01:15Z. Missing quote coverage remains a separate blocker; this
release is not a delivered pick or continuous monitoring guarantee.
Recommended still displayed a sign-in gate. Private owner-scoped delivery
was not verified.

The recent production log scan returned seven stderr records labeled error.
All seven were inspected: they were dependency warnings about future SSL-mode
semantics or legacy URL parsing, not observed thrown request failures.
Both sport feeds returned valid paper JSON. This is a bounded release check,
not proof of all routes, private sessions or long-term health. No credential
values or personal records were retrieved or published.

The50-credit reserve does not establish the current balance; retain the last
captured provider preflight as historical and check current headers before
any future quota-consuming request. Build-time collection, if configured,
must not be mistaken for a verified fresh phone card.
