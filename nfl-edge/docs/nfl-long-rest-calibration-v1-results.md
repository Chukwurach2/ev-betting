# NFL long-rest calibration — not supported

The fixed preparation/recovery hypothesis is **not supported**. The treated-count-weighted, within-week/home-away contrast in selected-team win-minus-closing-probability residuals is **−0.246939 percentage points**, opposite the predicted positive direction. The registered three-week circular bootstrap99% interval is **−39.51250 to +33.59542 percentage points**. This very wide interval includes zero and cannot support a useful current-game adjustment. It is neither profitability evidence nor proof that long rest has no effect.

## Registration and chronology

- Frozen protocol: research/protocols/nfl-long-rest-calibration-v1.json, registration commit bc450414e126afd140b719d0ec4c57f914aff223 at2026-11-05T19:18:15Z.
- Tested implementation: b0f93e452377a4772a4f8e19b58a27850082f4c5.12new synthetic tests and38retained research tests passed locally before subgroup evaluation. Hosted run42106467013/job127522478639 passed,including explicit long-rest test step11,before the evaluation started.
- Private pre-evaluation receipt:2026-11-05T21:52:06.957429Z. Implementation/dependency bytes matched the remote commit before use.
- Fixed contrast reconstruction completed2026-11-06T02:13:35.080836Z after host/tool delays. This remains November5Eastern's one substantial experiment. No second new statistical experiment is permitted that Eastern day.
- Protocol bytes and prior19registry entries remain unchanged. No alternate cutoffs,selection,reverse rule or secondary hypothesis was evaluated to rescue this result.

## Fixed design and coverage

The schedule proxy is not verified official bye status. Treatment requires12–18elapsed24hour days since the selected team's immediately previous regular start and5–9days for its opponent; controls require both teams5–9days. Home-treated strata use control home teams,and away-treated strata use control away teams. Same control games can appear in both roles;136appearances are not136independent games.

Source gates passed:272regular games,32stable teams,17games each,unique game IDs/team starts,stable identity mapping,timezone/local-date validation.13postseason rows were excluded. Outcome-blind labels were16missing-prior games,65other-rest games,169control games,13home-treated and9away-treated games.

After the registered exclusions,22treated games(13wins) and167unique control games remain across the evaluated season. Primary comparison uses all22treated games and136control appearances across9weeks:6,7,8,9,10,11,12,13,15. Registered minimums20treated/80control appearances/6weeks passed,but the treated sample is still small. All36week×role strata,including zero-weight strata,are preserved in the aggregate report. No substitute controls were introduced.

Nonexclusive exclusion flags:16missing-prior,65other-rest,2prior search-exposed rows,and1tied final. A row can have multiple flags;do not add these as disjoint counts. First regular starts can supply future rest history,and excluded outcome rows can still supply schedule history. Exactly-even probabilities are eligible here,as registered;the separate rematch even-probability exclusion was not transferred.

## Fixed result and verification

| Quantity | Result |
|---|---:|
| Comparable treated games |22|
| Treated wins,descriptive only |13|
| Comparable control appearances |136|
| Comparable weeks |9|
| Weighted residual contrast |−0.246939pp|
|99% percentile interval |−39.51250 to +33.59542pp|
| Bootstrap attempts / seed |20000 /20261105|
| Valid / zero-weight skipped draws |19972 /28|

The bootstrap samples six circular three-week blocks on the full18week grid,retains all shared game/role contributions in each sampled week,and recomputes treated weighting. The valid-draw floor19000passed. Linear interpolation at(n−1)q gives the registered0.5%/99.5%quantiles. A separate reconstruction from retained rows and frozen schedule labels reproduced the fixed contrast,counts and comparable weeks. Synthetic tests also independently reconstruct the seeded blocks and directional/minimum gates. This is implementation verification of the same fixed analysis,not another statistical experiment.

Original aggregate output:model/reports/nfl-long-rest-calibration-v1.json. Detailed source rows,schedule labels,pre-evaluation/source receipts and verification remain private under ignored runs/nfl-long-rest-2026-11-05/. No raw match table is published.

## Source and limits

Existing authorized private source only;no redownload. Publisher:minmaxxxer,dataset nfl-closing-lines-ats-2025,revision1d90317a9258ed8ad7984ed13ee516834de763ba,file nfl_2025_closing_lines_ats.csv.71573bytes;SHA256 fdd15d1c67c59ff5440f5ea5de79067b5aaa1ebf5ab872014f9e1175204a89e3. Publisher labels itCCBY4.0;upstream rights are not certified. Closing probabilities are publisher consensus/proportional de-vig benchmarks,not independent compiler validation or earlier executable sportsbook quotes.

The entire2025NFLseason was already inspected in the separate rematch study. This is development evidence,never untouched confirmation. The99%interval does not establish project-wide multiplicity control. Sparse treated games,team dependence,schedule/strength/travel/injury confounding and consensus dependence remain. No executable ROI,CLV,causality,fee/limit certification or T−60 information is established.

Retain the unsupported result. No cutoff rescue,reversed betting rule,model promotion or phone picks follow. Frozen model,journal and paper ledger were unchanged;no provider request,purchase,account action,wager or deployment was performed. Current-game cards require fresh eligible exact-book prices and availability checks separately. The phone review in this delayed run found no supported NFL prices;NBA refresh failed to load. Those observations are not current coverage claims after the host delay. EVSharps access was not completed because automatic approval review's transport failed;no bypass or subscriber-data acquisition occurred.
