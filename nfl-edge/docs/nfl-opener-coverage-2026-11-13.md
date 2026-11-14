# November 13 NFL opener coverage

The [scheduled collector](https://github.com/Chukwurach2/ev-betting/actions/runs/43539666834/job/132277002041) recorded one capture, zero failures, one odds request and two budgeted credits. A read-only aggregate check identified it as the Indianapolis at Houston opener, scheduled for November 20 at 01:15 UTC. The checkpoint ran November 14 from 01:17:51.877 to 01:17:52.074 UTC. It had eight captured rows and eight corresponding stored rows. The job's zero pending materializations counted the remaining backlog after collection. The run does not establish the remaining provider balance.

| Full-game market | Stored rows | Distinct raw book keys | Source time range (UTC) |
| --- | ---: | ---: | --- |
| Spread | 4 | 2 | 01:16:36–01:17:32 |
| Total | 4 | 2 | 01:16:36–01:17:32 |

For the exact teams and kickoff, the same two-book ceiling applied across all stored quotes in the freshness window at checkpoint completion. [The deployed card calculation](https://github.com/Chukwurach2/ev-betting/blob/3cf60a23081b2bd07a8de6fb52877f8e8e89103b/nfl-edge/lib/game-cards.mjs) requires two other operators on the same selection and line after excluding the offered operator. With at most two operators per market, this capture could not meet that requirement even when fresh. The next useful opener capture needs at least three distinct canonical operators on the same contract before evaluating any price. No observation entered the paper ledger.

At 02:50:31 UTC the phone's public API returned zero entries for 14 upcoming NFL games and two upcoming NBA games. All opener sources had aged beyond 15 minutes. The next NFL game was [New England at Detroit in Munich](https://support.nfl.com/hc/en-us/articles/41438702917268-How-to-Watch-NFL-International-Series-Games), November 15 at 14:30 UTC. The NBA feed listed Portland at Sacramento and Oklahoma City at the Clippers. No new collection was triggered for this review.
