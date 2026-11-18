# Prior-only NHL rematch assignment implementation

Current status was reconciled November 18. The fixed evaluation completed on
November 10 Eastern with insufficient coverage. See the [retained result](nhl-immediate-rematch-results.md)
for 10 treated games and 218 controls in nine comparable weeks. The coverage
gate stopped the bootstrap. The implementation account below records its
earlier stage. The rule remains unpromoted.

The frozen registration remains unchanged. Implement only schedule validation and prior-game assignment before any new private subgroup evaluation. There is no estimator,bootstrap,private-data runner,downloader,model or pick publisher in this increment.

The classifier audits1312regular fixtures/32stable teams/82games each,UTC/Eastern identity/probability consistency and valid final scores. It chooses each team's latest strictly earlier game in sorted time/ID order. Both rest gaps and the home team's prior loss determine eligibility;the exact shared previous fixture determines treatment. Current scores are not read during assignment. History updates only afterward,so a result can influence later games but cannot determine its own treatment.

Eleven synthetic regression cases cover outcome-blind schedule audit,true shared fixture and home/away reversal,current-outcome exclusion,one-sided/older head-to-head controls,both18/96hour boundaries,missing history/prior wins,shared-ID conflicts,coverage/identity/date/probability failures,duplicate starts/nonUTC timestamps,invalid/tied finals/winner contradictions,and input-order/future invariance. Run in an isolated ignored directory and explicitly in hosted CI.

No new group count,outcome contrast or statistical experiment. All fixtures are synthetic. The prior source/target-semantics gate and the remaining fixed weekly estimator/uncertainty implementation still need completion before any private evaluation. Local11/11passed. Do not call these tests evidence for the mechanism or change the registered rest window,minimums,estimator,seed or interval.
