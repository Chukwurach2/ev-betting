# NFL longshot calibration: not supported

The fixed diagnostic did not support a longshot calibration lead. Its same-week treated-count-weighted win-minus-closing-probability contrast was **−1.827453 percentage points**, with a **99% interval of −13.950552 to +12.612762 points**. It failed both the registered −3-point threshold and the requirement that the interval's upper bound be below zero. Retain this result without changing probability bands, reversing the rule or promoting picks.

| Fixed evaluation population | Games | Wins |
| --- | ---: | ---: |
| Lower-probability team with p≤1/4 | 68 | 14 |
| Moderate underdog with 1/3≤p<1/2 | 142 | 61 |

All18weeks had both groups and positive weight; no weeks were substituted and no global-control contrast was used. The30treated/80control/8week minimums passed. Circular3week blocks over the complete18week grid, six blocks per draw,20,000attempts and seed20261107 produced20,000valid draws and0zero-weight skips. The99%interval uses the registered linear percentile interpolation. The wide interval includes substantial effects of either sign; this is not proof of absence or a basis for a reversed betting rule.

## Registration, implementation and verification

Registration commit5ac4784d61e7ee5fff5dd53b42772af5ff8c0efb preceded new-group computation. Tested implementation68ef75ee790b484cfd9a6595cc69c308385448ca preceded evaluation; CI42397648774/job128478374709 passed, explicitly running14new synthetic tests alongside50retained research tests. The protocol bytes remained unchanged. Evaluation preflight was retained at 2026-11-07T18:37:55.525028+00:00; independent exact-Fraction count/flag/week-weight/primary reconstruction and all20,000fixed bootstrap draws agreed within1e−14 at 2026-11-07T18:38:37.249517+00:00. Exact reconstructed primary: −344467601/18849600000.

The source hash and complete-season identity gates passed:272regular games,32teams,17games/team,13postseason rows excluded. Score-blind band audit before outcome exclusions:69treated,142controls,58gap,3exactly-even. Nonexclusive outcome exclusion flags:58gap,3even,2previously search-exposed rows and1tied final. Overlap means these counts must not be summed as disjoint exclusions. Required probabilities and scores validated even for excluded rows. One lower-probability team per game; exact decimal/rational boundaries avoid rounding a source decimal into1/3.

## Source and limits

Existing private source from [minmaxxxer's NFL2025closing-line compilation](https://huggingface.co/datasets/minmaxxxer/nfl-closing-lines-ats-2025/tree/1d90317a9258ed8ad7984ed13ee516834de763ba), revision1d90317a9258ed8ad7984ed13ee516834de763ba, file nfl_2025_closing_lines_ats.csv:71,573bytes, SHA256 fdd15d1c67c59ff5440f5ea5de79067b5aaa1ebf5ab872014f9e1175204a89e3. Publisher labels CC BY4.0; upstream rights remain uncertified. No new download or raw-row redistribution. Only original aggregate calculations are public.

This entire season was already inspected for rematch and long-rest studies. It remains development evidence from the same compiler, never an untouched holdout or independent bookmaker validation. Probability-band strength, calendar, home/away and source confounding remain; week blocks do not eliminate team dependence. A99%interval does not establish project-wide multiplicity control. All20previous registry entries and failures remain unchanged. No executable-price ROI, CLV, causal effect, earlier quote, fee, limit, settlement, availability or accepted-offer inference follows from closing consensus.

Machine-readable original aggregate report: model/reports/nfl-longshot-calibration-v1.json. Raw source, per-game audit, pre-evaluation and independent-verification receipts remain ignored/private. The original paper ledger, frozen model and isolated journal are unchanged.

This consumes the November7Eastern substantial-experiment slot. No second new substantial experiment today; settlement, offline maintenance and permission-aware current-card review remain available. Future confirmation requires separately registered independent/prospective evidence, with exact decision-time prices before any profitability claim.
