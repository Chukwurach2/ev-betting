# November 18 scheduled opener audit

Read-only normalized quote lookup recovered evidence for both runs after
an earlier database transport interruption. Each scheduled run logged
one capture and budgeted two credits. Remaining account quota was
unverified.

| Game | Run | Capture time UTC | Source-time range UTC | Committed rows |
| --- | --- | --- | --- | ---: |
| New Orleans at Chicago | [02:07 UTC](https://github.com/Chukwurach2/ev-betting/actions/runs/44220934820) | 02:08:06.772 | 02:07:09–02:07:55 | 16 |
| Tampa Bay at Detroit | [11:07 UTC](https://github.com/Chukwurach2/ev-betting/actions/runs/44291852397) | 11:08:56.088 | 11:07:54–11:08:55 | 16 |

The production card algorithm reconstructed two opposing full-game
spread watches for New Orleans–Chicago, two entries below the requested
four. The proportional de-vig estimates were −3.70% and −4.13%.
Power de-vig on the same exact reference pairs yielded −3.70% and
−4.10%. These were overlapping market references and the estimates
exclude additional fees. Both watches expired at 02:22:09 UTC.
Available size and execution price were unverified.

Tampa Bay–Detroit had all 16 rows within the freshness window at
checkpoint completion, yet produced zero entries. Each half-point
spread selection had at most one quoted operator at its exact line,
and each half-point total selection had at most two. A candidate
needs an offer plus two distinct reference operators at its exact
selection and line. The replay preserved the coverage shortfall.

At 13:15 UTC the public game-card feed returned zero fresh entries
across 14 NFL and 12 NBA listings. The captures above were replayed
retrospectively, and their pre-expiry phone rendering remains unverified.
The prospective ledger contains neither replay.
