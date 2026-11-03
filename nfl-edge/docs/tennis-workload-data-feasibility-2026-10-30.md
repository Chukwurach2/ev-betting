# Tennis workload theory: metadata feasibility — 2026-10-30

Status: proposed, data-blocked for priced evaluation. This is a metadata audit,
not an experiment registration or a backtest. No match rows or outcomes were
downloaded or evaluated.

## Mechanism and competing explanation

A long prior match may reduce a player's next-match performance, while the
market may overreact to visible duration. These are opposing hypotheses:
fatigue alone does not tell us which side has value. The economic test must
measure information remaining after the actual offered price.

A narrower future mechanism study could compare prior-round workload within
one event. It would need to separate match competitiveness, opponent strength,
surface and survivor selection: players reaching the next round already won.
A simple count of next-round wins after long matches would confound fatigue
with these factors. Do not infer a fade or a fair probability from it.

## Source checks

The original Jeff Sackmann ATP repository returned HTTP404 during this check:
https://github.com/JeffSackmann/tennis_atp
This observation does not establish why it is unavailable.

A public mirror retains the author's README and dictionary:
https://github.com/azhou5849/tennis-atp-sackmann-data/blob/master/README.md
https://github.com/azhou5849/tennis-atp-sackmann-data/blob/master/matches_data_dictionary.txt

The retained notice attributes Jeff Sackmann / Tennis Abstract and states
CC BY-NC-SA4.0. Mirror availability is not an audit of provenance, completeness,
current-season maintenance or permission for commercial app distribution.
No mirror data was imported or redistributed.

The dictionary describes tournament-week dates, arbitrary match numbers,
rounds, match duration when available, and tournament-date rankings.
Consequently, sorting by date and match number cannot establish exact match
order, a48-hour recovery window, or timestamped feature availability.
The target match's duration and serve statistics are outcomes, never pregame
features. The README also notes missing and filtered statistics; complete-case
analysis would need missingness and selection checks.

## Next usable research increment

Before any rows are inspected, obtain an attributable frozen source with actual
start/end times, player IDs, draw/round progression and retirement/walkover
status. Add permitted bookmaker quotes with observation times and exact
retirement/settlement rules for a priced test. Freeze one direction, thresholds,
chronological split, opponent-strength baseline, all exclusions and player/event
dependence handling in a separate protocol.

If only round ordering can be verified, register a distinctly labeled
within-event descriptive study; do not rename it a short-rest or betting-return
test. No numeric cutoff, statistical result, paid access or strategy promotion
was created here. NFL/NBA delivery remains the priority.
