# Bounded NBA capture — 2026-10-28

One manually confirmed existing-quota capture ran on main ffe15d04855ae2be83203b759de1642eb5623df8:
https://github.com/Chukwurach2/ev-betting/actions/runs/40771536276
Job 123109733596 completed successfully; ten offline collector/feed tests passed.

The free events preflight reported 224 credits remaining BEFORE the bounded
US spreads/totals request (up to two credits), retaining the 50-credit reserve.
194 paired quote rows across 18 provider events were processed. This is not a
current quota balance; no further odds request or deployment was made.

Independent public feed read at 2026-10-28T21:24:08.881Z returned 13 upcoming
games and 42 entries, all negative estimated EV. Nine games had four entries;
three had two; one later game had zero. These are correlated individual
price watches, not positive-value recommendations, calibrated model signals
or a bet-all portfolio. Four-entry shortfalls were retained.

Quote times were 21:22:52–21:23:19Z. Entries expired at 21:37:52–21:37:55Z,
well before the later rendered-phone check. At 22:28Z they were stale. The
workflow's immediate GET and independent feed read establish fresh API
delivery at capture time; fresh phone UI rendering was not verified before
expiry. Do not claim current fresh picks, private owner-scoped visibility or
24/7 monitoring. NFL was checked separately and had zero fresh entries.

42 actual observations and 13 coverage receipts were appended to the private
paper ledger under its lock. Detailed source-preserving cards remain ignored
and include exact price, line, fair-price threshold, reference times/ranges,
conditional injury/rule/identity review, unknown size and zero separate-fee
assumption. No wager, fill, paper settlement or profit is inferred.

References:
- Official schedule: https://www.nba.com/games?date=2026-10-28
- Injury review: https://ak-static.cms.nba.com/referee/injury/Injury-Report_2026-10-28_05_15PM.pdf
- Venue review: https://gaming.ny.gov/sports-wagering

Provider scheduled starts differ from official listed starts; exact
provider-to-official event/rule equivalence remains conditional. Full-game
markets are alternatives within each event. Market-derived fair probabilities
exclude the offered operator but share underlying market information and are
not independent model validation. Limits, accepted book offers, tax treatment
and all minutes/late availability remain unverified. No entry was promoted
to ready; the original historical model is unchanged.

EVSharps Recommended still displayed its sign-in gate in this session. No
subscriber rows were extracted or redistributed. Research-only documentation;
no second statistical experiment, account change or live execution.
