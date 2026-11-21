# Fixed-cost sensitivity of retained paper grades — 2026-11-03

Replayed the complete unchanged private ledger against the twenty-five-final evidence receipt using the exact PR38 evaluator at 5021504cbe2e94848ee80cef9372ef3332d0c7ab. All retained outcomes and zero-cost profits reproduce exactly: 948 unique records, 560 observations, 398 earliest contract/book entries across 67 provider IDs; 64 graded across 24 games, 34 wins and 30 losses. The other 334 entries remain unresolved (130 exact-contract evidence, 16 identity conflicts, 188 missing finals).

This is a deterministic sensitivity calculation using the existing feePerUnit option, not a new statistical experiment or a verified operator fee schedule. Each scenario subtracts the same additional cost from every graded one-unit entry, including losses. Unresolved entries retain null profit and are excluded from every return denominator.

| Additional fixed cost per graded 1u entry | Hypothetical net units | Graded-only ROI |
| --- | ---: | ---: |
| 0.00u | +1.854802 | +2.898128% |
| 0.01u | +1.214802 | +1.898128% |
| 0.02u | +0.574802 | +0.898128% |
| 0.03u | -0.065198 | -0.101872% |
| 0.05u | -1.345198 | -2.101872% |

The zero-net fixed cost is approximately 0.0289813u per graded 1u entry. It is only the arithmetic margin of this incomplete, correlated retrospective subset. It is not an estimate of executable edge, a book's actual charge, an odds-movement allowance or a permissible price threshold. American-odds payouts already embed the offered price; these scenarios add a separate hypothetical cost. Taxes, accepted fills, liquidity and market-specific rules are not inferred.

Validation: verified local evaluator bytes equal the exact remote source; all five full-cohort scenario replays preserve 64 grades, 334 unresolved entries and every unresolved reason; each net equals zero-cost net minus 64 times scenario cost within 1e-10; unresolved profits remain null. The zero-cost replay reproduces every prior observation ID, outcome and profit. Private receipt: runs/paper-cost-sensitivity-2026-11-03T1900.json, generated 18:59:09Z. Prior receipts and original ledger remain unchanged.

These watches include correlated alternatives and negative-edge observations. No validated strategy, independent profitability result, prospective inclusion, wager P&L or CLV is established. No new final, source quote, provider request, deployment, model change or journal update.
