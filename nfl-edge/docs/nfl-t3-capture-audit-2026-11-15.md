# November 15 NFL checkpoint and phone coverage

The scheduled NFL worker finished at 15:21 UTC with one captured checkpoint,
three missed checkpoints, six deferrals and one odds request budgeted at two
credits. The three missed windows were T−3, T−90 and Close for the 14:30 UTC
Munich kickoff. The captured window was Buffalo at the Jets, T−3 for the
18:00 UTC kickoff. Actual post-request credit balance is unverified.

A read-only storage check found 16 rows in both the checkpoint and normalized
tables. There were eight spread and eight total rows from four stored
operators. Provider update times ran from 15:20:38 to 15:21:13 UTC and the
checkpoint completed at 15:21:20.193 UTC. Materialization reported zero new
attempts in this run. The normalized rows were already present when queried.

Applying the existing public card builder to just those rows at checkpoint
completion reconstructed four half-point price watches. Each had two
other-operator references. All matching opposite quotes reconciled with
stored proportional probabilities. Estimated proportional EV ranged from
−4.50% to −3.70% at zero additional fees. The power method also kept all
four negative, with estimates from −4.65% to −3.70%. Both methods reuse the
same operators. Actual market depth, account limits and pre-expiry phone
display were unverified. All four expired by 15:35:39 UTC. Exact rows and the
reconstruction were preserved privately with their retrospective review time.

At 15:52:58 UTC, both public feeds returned zero fresh quotes and zero
entries. The public schedule feed contained 11 NFL and nine NBA games in
the next 24 hours. The reconstruction was excluded from current cards and
prospective performance. This review made no new quota-consuming request.

- [Scheduled collector job](https://github.com/Chukwurach2/ev-betting/actions/runs/43729343128/job/132858119495)
- [Official NFL schedule](https://www.nfl.com/schedules/2026/by-week/week-10)
- [Official NBA schedule](https://www.nba.com/schedule)
