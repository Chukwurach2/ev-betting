# De-vig method sensitivity on archived phone quotes

The audit compared proportional and power de-vigging at the same offered price,
with the same reference operators and the same median aggregation. For the
power method the two implied probabilities are raised to a common exponent
chosen so their sum equals one. [Clarke, Kovalchik and Ingram (2017)](https://sciencepublishinggroup.com/article/10.11648/10026106).

A reference pair required the exact same event, period, line, operator and
source timestamp on both sides. For spreads the lines had opposite signs.
The audit reconstructed each stored proportional probability and the card
median to within 0.000000000001 before making the comparison. Repeated
observations with the same price and reference identity were counted once,
using the first retained file in filename order. Each row used the stored
offer and a zero-additional-fee sign check.

| Sport | Unique archived entries | Complete paired reconstruction | Missing or ambiguous opposite contract | Unpaired operator or timestamp | Positive under either method |
| --- | ---: | ---: | ---: | ---: | ---: |
| NFL | 28 | 6 | 14 | 8 | 0 |
| NBA | 394 | 68 | 0 | 326 | 0 |

All 74 comparable entries remained nonpositive. The other 348 lack complete
pair evidence under this audit. No positive estimate was evaluable, so this
finding cannot establish robustness for positive leans. The archived cards
display a shortlist, which limits reconstruction and can select a different
cohort than an all-quotes capture. No outcomes were used, no ledger rows were
changed and no current market was evaluated. The method was checked on
symmetric and asymmetric synthetic prices and the full result reproduced.
