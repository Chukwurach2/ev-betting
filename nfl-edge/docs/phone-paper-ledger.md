# Private phone paper ledger

`node ops/record-phone-paper.mjs private-feed.json` records an offline export of
the app's GET /api/game-cards?sport=nfl (or nba) response. Default destination
is ignored runs/phone-paper.jsonl, opened with owner-only permissions on new
files. Do not commit inputs or private ledgers. An optional second argument
selects a private local ledger path. No provider request, database write,
sportsbook execution, personal journal mutation or paid quota use occurs.

The recorder validates sport/event identity, supported half-point contracts,
NY offer allowlist, reference count and source-time freshness. It recomputes EV
from stored probability and offered odds. Unknown/malformed candidates are
rejected; each upcoming game retains a coverage record including shortfalls
and rejected count. Invalid envelopes fail before any append. Identical
observations have stable hash IDs and are not appended twice; revised odds or
fair probability preserve a new observation. A local exclusive lock prevents
concurrent writers; an abandoned lock requires inspection before manual recovery.

This is an exploratory observation ledger, not a wager/fill ledger or strategy
trial: stake is null, acceptance unverified, settlement unresolved. It does not
certify reference composition, exact operator void rules or profitability.
Repeated captures are correlated and must not be treated as independent bets.
The selection method is phone-market-consensus-v1. Future analyses require
predeclared selection/decision windows, contract-linked settlement and matching
closing evidence; neither ROI nor CLV is fabricated when those inputs are absent.

2026-10-19 real-feed verification: 15 NFL and 18 NBA upcoming games returned
zero leans. The local recorder appended 33 coverage records and zero paper
observations, correctly retaining unavailable coverage without inventing bets.
124 Node tests passed, including seven new paper-record tests.
