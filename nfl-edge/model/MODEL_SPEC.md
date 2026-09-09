# Model specification

## Target

Price early-game NFL markets with a reusable possession model and identify sportsbook prices
where model probability exceeds de-vigged market probability.

## Statistical unit

A drive that begins in Q1.

Drive terminal outcomes:
- TD
- FG
- PUNT
- TO
- DOWNS
- OTHER

Drive duration is game-clock seconds consumed.

## Leakage control

For team T in game G, all rolling offense and defense features are calculated using games
strictly before G. The feature state is written first, then updated with G's realized values.

That sequencing is intentional and should never be changed.

## Hierarchical shrinkage

The current version starts unseen/early-season teams at a league prior, then exponentially
updates the state:

state_t = alpha * observation_t + (1-alpha) * state_(t-1)

Default alpha = 0.28.

This is effectively a simple hierarchical/shrinkage model: small samples remain close to the
league prior and established samples increasingly reflect the team.

## Predictive features

Offense and opponent-defense priors:
- first-drive TD rate
- first-drive score rate
- Q1 drive TD rate
- Q1 drive scoring rate
- Q1 EPA/play
- Q1 success rate
- Q1 drive duration
- Q1 plays/drive

Pregame/context:
- home/away
- team points favored by
- total
- roof
- surface
- temperature
- wind
- head coach
- opposing head coach
- drive index
- opening-drive indicator

## Outcome model

Regularized multinomial logistic regression.

Why use this as V1:
- probabilities are naturally produced
- easier to calibrate and audit
- much less likely than a large tree ensemble to exploit noisy team/coach sample artifacts
- coefficient stability is useful when diagnosing drift

A boosted-tree challenger should be added later and must beat the baseline on out-of-time
log loss, Brier score, calibration, and betting ROI after vig.

## Duration model

Ridge regression on log drive duration. During simulation, duration is sampled from a lognormal
distribution centered on the predicted median.

## Simulation

For each simulation:
1. Randomize first possession 50/50 unless known.
2. Sample offensive drive result.
3. Sample clock duration.
4. Update Q1 score and event flags.
5. Alternate possession.
6. Stop when the Q1 clock reaches zero.

Run 50k-200k simulations per game depending on desired Monte Carlo error.

## Bet decision

A selection qualifies only if:
- American odds >= -150
- model probability is available
- model probability minus de-vigged market probability >= configured edge
- expected profit per $1 risked >= configured EV

Default:
- minimum odds: -150
- minimum edge: 3 percentage points
- minimum EV: 4%

## Production hardening roadmap

1. Build an odds snapshot database with provider, book, timestamp, and market id.
2. Track exact decision-time lines.
3. Add QB starter / QB change.
4. Add OC/play-caller opening script.
5. Add offensive-line starter continuity.
6. Add injury participation and skill-player availability.
7. Add forecast weather.
8. Train a boosted challenger.
9. Isotonic / beta calibration on an untouched validation window.
10. Walk-forward backtest by season/week.
11. Closing-line-value tracking.
12. Fractional Kelly sizing with strict exposure caps.
13. Drift alerts by market, team, and probability bucket.
14. Weekly automated retraining after PBP updates.
