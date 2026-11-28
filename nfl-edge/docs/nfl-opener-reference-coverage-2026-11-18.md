# NFL opener coverage under loss of one operator

This descriptive replay uses all five NFL opener receipts from the three
documented capture audits on November 17–18 UTC. The receipts cover five
different games and contain 16 normalized rows each. The production card
function at 3cf60a2 reproduced every archived output using each
checkpoint's actual completion time.

For each event the replay removed the quote rows of one canonical
operator at a time, then reran identical contract, price, freshness and
reference checks. This measures losing an operator across both offered
and reference prices. It does not simulate a change to the offered-book
filter alone.

| Opener | Baseline entries | Range after one operator is removed | Removals leaving zero |
| --- | ---: | ---: | ---: |
| Arizona at Kansas City | 2 | 0–2 | 3 of 4 |
| Baltimore at Carolina | 2 | 0–2 | 3 of 4 |
| Miami at Buffalo | 4 | 4–4 | 0 of 4 |
| New Orleans at Chicago | 2 | 2–2 | 0 of 4 |
| Tampa Bay at Detroit | 0 | 0–0 | Baseline already zero |

The baseline had ten entries and all ten had negative estimated value.
Six of the 16 operator-removal replays with a nonempty baseline lost the
entire card. The others retained the same exact selections and lines,
with no estimated-value sign changes. Offered operators could change
on retained entries. Tampa Bay–Detroit remained empty in all four
replays because the baseline lacked sufficient exact-line references.

The five events represent one bounded operational sample. Removals are
correlated within each event. Differences in exact market-line overlap
matter even with the same nominal number of operators and quote rows.
No outcomes or imputed quotes were used. This does not establish
benchmark independence, acceptance, execution capacity or expected
strategy performance. All results remain retrospective.

- [Prior November 17 capture receipts](phone-coverage-2026-11-17.md)
- [Prior November 18 capture receipts](nfl-opener-replay-2026-11-18.md)
