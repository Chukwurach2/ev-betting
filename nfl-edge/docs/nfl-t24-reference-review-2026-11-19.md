# Colts–Texans T−24 reconstruction — November 19

The scheduled checkpoint completed at 01:49:16.011 UTC retained sixteen
quotes, independently verified in normalized storage. This review replayed
those rows at their actual checkpoint completion time using the deployed
card evaluator at `3cf60a23081b2bd07a8de6fb52877f8e8e89103b`. It did not
relabel later work as contemporaneous phone delivery.

The evaluator produced four distinct spread/total alternatives, all
negative-value price watches. Stored proportional de-vig estimates implied
returns from −3.89% to −3.13% before any additional fees or taxes. Exact
paired-price power de-vig returned −3.70% to −3.28%; all four remained
negative. Each offered operator was excluded from its reference set;
two alternatives retained two other operators and two retained three.
These are overlapping market estimates, not independent model validation.
Opposing selections are correlated alternatives, not a bet-all portfolio.

For every retained reference, the replay required a unique opposite
outcome at the same operator, exact corresponding line and identical
source timestamp. Recomputed proportional probabilities and the original
four EV values matched within 1e−12. Power de-vig used bisection on the
sum of the two implied probabilities raised to a common exponent, with
100 iterations over [0, 10]. The fixture probabilities bracketed the root.
The median other-operator fair probability was evaluated at the unchanged
offered price under each method. No cutoff or outcome was searched.

An integer total line at one operator remained unsupported for this
half-point-only presentation layer. It was not substituted for a different
line or used to manufacture extra entries. Exact prices, paired source
rows and detailed historical alternatives remain in ignored private
research storage. No subscriber table is reproduced here.

The retained quotes had source times 01:47:10–01:49:12 UTC. All expired
before the earlier 03:02 UTC phone review. A replay at actual review time
also returned zero active entries. Injury status, accepted sizes and exact
book-rule eligibility were not newly verified; nothing was promoted as
ready. The four reconstructed observations were not inserted into the
prospective ledger or published as active private phone cards. No result,
ROI, strategy validation or fresh execution opportunity follows.

The November 19 09:31:21.528 UTC NBA phone response contained all four
Thursday matchups, each ten minutes after the reviewed official scheduled
tip, correcting the earlier three-missing coverage receipt. Both sports
still returned zero fresh entries in their 09:31 responses. Review completion
was delayed until after 12:51 UTC; these are historical snapshots rather
than a claim about current quotes. No provider odds request was dispatched.

This is a descriptive operational replay, not a new statistical experiment.
Assertions checked the sixteen-row identity/freshness cohort, four distinct
negative watches, paired source identity, proportional reproduction,
power sensitivity and zero active entries at actual review time. Detailed
receipts and replay methods are private in
`runs/nfl-t24-review-2026-11-19/`.

- [Scheduled capture](https://github.com/Chukwurach2/ev-betting/actions/runs/44439689201)
- [Official NFL schedule](https://www.nfl.com/schedules/2026/by-team/houston-texans)
- [Official Thursday NBA slate](https://www.nba.com/games?date=2026-11-19)
