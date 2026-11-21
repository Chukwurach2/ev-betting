# November 15 late-window NFL capture

The scheduled collector started at 20:48:34 UTC and completed one odds
request budgeted at two credits. The 20:49:36 UTC result logged one capture,
23 newly recorded misses, two deferrals and no failures. The misses were
nine T−3, seven T−90, and seven Close checkpoints. Twenty belonged to the
18:00 UTC slate. Two belonged to the 21:05 UTC group and one to the
21:25 UTC group. Post-request quota was not independently verified.

The capture was the Los Angeles Rams at Arizona T−90 checkpoint, completed
at 20:49:36.488 UTC for a 21:05 UTC kickoff. Storage independently showed
16 checkpoint rows and the same count in the normalized table. Provider
timestamps were 20:49:12–29 UTC. The audit used those actual timestamps and
the captured cutoff; the nominal checkpoint label is not a 90-minute lead
time for these quotes.

Replaying the current phone card builder on this checkpoint reconstructed
two opposing half-point total watches. Each had three other-operator pairs
that reconciled to the stored proportional estimates. The zero-additional-
fee proportional EV range was −4.13% to −3.70%; power de-vig was −4.10%
to −3.70%. Both methods reuse the same pairs. The spread lacked sufficient
same-selection and same-line references for a card entry. Both watches
expired at 21:04:12 UTC, before kickoff. No timely phone rendering, offer
acceptance, liquidity, or account limits were verified.

At 21:11:24 UTC, both public phone feeds showed no fresh entries, with two
NFL and seven NBA games in the next 24 hours. The privately preserved
checkpoint reconstruction is retrospective and was excluded from current
cards and prospective trials. This audit made no odds request.

- [Scheduled collector job](https://github.com/Chukwurach2/ev-betting/actions/runs/43760130887/job/132952855884)
- [Official NFL Week 10 schedule](https://www.nfl.com/schedules/2026/by-week/week-10)
- [Official NBA schedule](https://www.nba.com/schedule)
