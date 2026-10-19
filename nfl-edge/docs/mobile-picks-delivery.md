# NFL Edge mobile delivery

## User-facing output

The default phone screen is NY Picks, grouped by upcoming NFL game. It shows up
to four distinct exploratory market leans per game with offered book/odds,
estimated fair odds/probability, estimated EV, break-even price, source quote
time and explanation. Shortfalls remain explicit. A price watch may have negative
estimated EV; it must not be described as an edge. Correlated game exposures are
not independent bets and the card is not a parlay.

My sportsbooks is a device-local venue filter. The server reprices against the
chosen eligible offers, while reference books can be outside the execution set.
The offer allowlist is NY-specific and needs periodic regulator/operator review.
Legacy provider key espnbet is displayed as theScore Bet. Aliases are one operator
for reference counting. No credentials, deposits, wager execution or new paid
provider polling were introduced.

## Data path and boundaries

Phone -> GET /api/game-cards -> existing games + nfl_edge_odds_quotes -> separate
exploratory presentation layer -> phone card. This read-only route uses the
existing server-side database configuration and makes no paid provider requests.
It is not a new model, a frozen-engine change, or a model-validation promotion.
All reference quotes must be fresh, match the exact game/kickoff/selection/line,
and exclude the offered operator. At least two other operators are required.
Only half-point spread/total families from the existing stored quote table are
supported. No probability is generated for missing data. Quote age limit is
15 minutes; unknown or future prices are rejected. Current scope does not certify
independent settlement-rule equivalence, accepted limits or execution capacity.

The separate novig-hqmm repository remains a research library. Its curated prop
reports and licensed subscriber observations are NOT silently copied to a public
frontend or used as a production model. A permission-aware authenticated publisher
is still needed for those detailed prop cards to sync automatically to the phone.
No old chat quote should be recast as a fresh app pick.

Research diagnostics and the existing NFL/college boards remain under Research.
My Bets preserves account-isolated journal sync. The new cards do not write bets,
change bankroll or automatically fill the journal.

## Arbs / Middles

The phone planner is local arithmetic, not a live opportunity feed. It balances
two stakes, includes a configurable fixed total cost and rounds stakes down to
cents. It displays the worst modeled win/loss profit. Middle mode separately
shows conditional both-win profit and a conservative break-even middle probability.
Pushes, voids, tax and contingent commissions are excluded; corresponding trades
need a fuller outcome model before evaluating profitability.

## Next delivery work

1. Verify the live route returns the current slate and stored quote coverage; fix
   underlying collector gaps rather than increasing the freshness limit.
2. Add an authenticated original-analysis publisher for curated prop cards with
   explicit data rights, injury/active status, source age and exact rule identity.
3. Persist immutable paper decisions and actual accepted prices separately from
   personal bets. Report unique events, CLV, net returns and drawdowns.
4. Add paired-opportunity records with both NY venues, leg capacity, acceptance,
   void/push scenarios, locked capital and realized results.

Release validation: 112 Node tests; static build; phone-width UI checks with
synthetic fixtures for rendering, preference filtering and middle arithmetic.
Production database coverage must be checked separately from synthetic UI QA.
