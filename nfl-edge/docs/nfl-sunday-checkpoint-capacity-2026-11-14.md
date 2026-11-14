# Sunday NFL checkpoint coverage

At 18:29 UTC November 14, the public phone feed showed eight NFL starts and five NBA starts within 24 hours. Both feeds had zero fresh quotes and zero research entries. A read-only checkpoint check found one completed T−24 capture for Munich and none yet for the seven Sunday 18:00 UTC games. All eight had older opener captures. The latest visible scheduled run had started at 15:33 UTC and captured Munich. [Scheduled execution](https://github.com/Chukwurach2/ev-betting/actions/runs/43610803986)

For the seven-game group, the 90-minute T−24 collection window was 18:00–19:30 UTC November 14. The hourly workflow is scheduled at minute zero, so two nominal starts fall inside that window. The deployed collector allows one event-specific odds request per NFL run, budgeting two credits per request and preserving a 50-credit reserve. Even if both nominal runs execute inside the window and use their request on that group, they can cover at most two of its seven games. Any contention, delay, or quota stop reduces that coverage. [Workflow](https://github.com/Chukwurach2/ev-betting/blob/3cf60a23081b2bd07a8de6fb52877f8e8e89103b/.github/workflows/collect-odds.yml) · [Collector](https://github.com/Chukwurach2/ev-betting/blob/3cf60a23081b2bd07a8de6fb52877f8e8e89103b/nfl-edge/ops/collect_checkpoints.py)

A future coverage change can evaluate one sport-level spreads/totals snapshot for several due games within the existing per-run cap. Before it is used, verify actual billing, eligible book coverage, event identity, market timestamps and the disposition of games missing from the response. The counts above describe capacity at the existing cadence. Checkpoint capture and the phone’s 15-minute freshness window remain separate measurements. This audit used existing stored data and public reads.

## Observed result after the window

The next visible [scheduled collector run](https://github.com/Chukwurach2/ev-betting/actions/runs/43637362998/job/132577000772) started at 20:24:52 UTC. Its log reported seven misses, zero captures and zero odds requests. A read-only check separately matched all seven 18:00 UTC games to their own missed T−24 records. The configured window had closed at 19:30 UTC. This slate shows that scheduling delay can eliminate coverage even if the one-request cap is sufficient. The late run did not consume odds credits.

The [provider's sport-level odds documentation](https://the-odds-api.com/liveapi/guides/v4/#get-odds) supports filtering by a list of event IDs and documents charging by requested markets and regions or bookmaker groups. That makes a bounded pregame batch a candidate for the remaining coverage bottleneck when a run starts on time. A future implementation must verify the actual response cost and handle each event independently. No batch was dispatched.

The 20:30 UTC phone read returned zero fresh entries or quotes across eight upcoming NFL games and five NBA games within 24 hours.
