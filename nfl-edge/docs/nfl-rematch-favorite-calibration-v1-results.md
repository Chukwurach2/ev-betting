# NFL rematch favorite calibration — result

Status: **not supported**. The fixed within-week favorite calibration contrast is−4.39533percentage points, but the predeclared three-week bootstrap95% interval is−12.99861to+7.97446pp and crosses zero. No rule reversal,cutoff rescue or pick promotion follows.

The hypothesis,exclusions,sample floors and uncertainty method were [registered](../research/protocols/nfl-rematch-favorite-calibration-v1.json) at c7aeac86f54cf42e3f39af28ed338adfb2fd6d4e before bulk acquisition. Implementation ea3b1347a532feca53c8a7d358ae427e51b172c5 passed ten local synthetic tests and hosted CI41840504309/job126610510811 before download. The hosted step explicitly ran the NFL suite; the existing NBA/NHL and application regressions were retained.

## Frozen population and gates

The [publisher README](https://huggingface.co/datasets/minmaxxxer/nfl-closing-lines-ats-2025/blob/1d90317a9258ed8ad7984ed13ee516834de763ba/README.md) labels the compilation CC BY4.0 and describes consensus de-vigged closing probabilities. Revision1d90317a9258ed8ad7984ed13ee516834de763ba,source71573bytes,SHA256fdd15d1c67c59ff5440f5ea5de79067b5aaa1ebf5ab872014f9e1175204a89e3. Attribution: minmaxxxer,NFL2025ClosingLinesandATSResults; upstream rights and independent source accuracy remain uncertified.

All frozen schedule gates passed:272regular games,32stable teams,17games each;13postseason games excluded. The outcome-blind audit precedes evaluation. Schedule alone identifies48repeat-pair games. Evaluation excludes the two previously search-exposed IDs,three exactly-even probabilities and one tied final,leaving48treated plus218controls. No excluded outcome enters either evaluated group. This is development evidence,never an untouched holdout.

## Descriptive result

| Cohort | Games | Favorite wins | Mean favorite closing probability | Mean win-minus-probability |
| --- | ---: | ---: | ---: | ---: |
| All evaluated rematches |48|30|70.02333%|−7.52333pp|
| All evaluated controls |218|144|66.48050%|−0.42546pp|

Those pooled means are not the primary statistic. Only weeks containing both groups contribute to the frozen within-week comparison:32treated/100controls across9weeks,passing30/80/8sample floors. Week18has16treated and zero controls,so its primary weight remains zero as registered; no substitute control was introduced. Other unmatched weeks also retain zero weight.

Primary contrast uses treated-count weighting within comparable weeks:−4.39533pp. The fixed20000attempt bootstrap samples six circular three-week blocks over the full18week grid,seed20261104;19978valid draws and22zero-weight skips pass the19000draw floor.95% interval−12.99861to+7.97446pp. The point estimate passes the−3pp magnitude threshold,but the upper bound does not pass zero; outcome **not_supported**.

Late-season rematches,team/division/pair dependence,only9comparable weeks,one season and compiler uncertainty limit inference. The18week grid is not18independent teams or trials. Closing consensus is not an executable sportsbook offer or earlier T−60 probability. No ROI,CLV,fees/limits/injury adjustment or wager accounting is inferred. Do not turn the unadjusted−7.52pp residual into a future betting edge.

## Evidence and next action

[Original aggregate report](../model/reports/nfl-rematch-favorite-calibration-v1.json) omits individual game rows and private weekly residuals. Private runs/nfl-rematch-2026-11-04 retains source.csv,source-receipt.json,pre-acquisition.json,schedule-audit.json,report.json,evaluation-provenance.json. Source hash,272-game coverage,266evaluated observations,32/100matched counts and20000attempt totals reconcile; independent Decimal recomputation matches the primary contrast within1e-12.

NFL2025is now inspected development for this family. Preserve the failure and all exclusions; do not search new cutoffs or reuse it as an untouched replication. A differently motivated future study needs its own registration and data; any confirmation of this family needs genuinely new independent/prospective evidence. November4Eastern's substantial statistical-experiment slot is consumed.

No production model,journal,phone data or venue eligibility changed. No paid provider collection,purchase,account action or deployment. Missing current phone quotes still mean pending coverage,not no-edge.
