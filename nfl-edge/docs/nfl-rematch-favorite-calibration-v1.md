# NFL rematch favorite calibration — registered, not run

This independently motivated mechanism asks whether repeat-opponent familiarity leaves NFL closing favorites overestimated relative to other favorites in the same week. It does not reverse the failed NBA rest or NHL homecoming rules. The exact fixed rules are in [the protocol](../research/protocols/nfl-rematch-favorite-calibration-v1.json).

The [publisher README](https://huggingface.co/datasets/minmaxxxer/nfl-closing-lines-ats-2025/blob/1d90317a9258ed8ad7984ed13ee516834de763ba/README.md) identifies a CC BY4.0 compilation covering272regular games plus13postseason games. It describes consensus de-vigged opening/closing probabilities,stable IDs,UTC starts,week labels and scores. Metadata establishes a possible descriptive input,not complete rights provenance or actual executable prices. Revision is pinned; raw rows will remain private. No API purchase or account is needed.

Exposure is explicit: README aggregate season statistics were seen; search snippets exposed game IDs IL4EwN7l and fH8rHjXi with outcomes before registration. Both are excluded from every evaluation group; only their schedule identity can define later rematches. No bulk CSV or rematch-stratified result was inspected. This is development research,not an untouched holdout.

Freeze one favorite per eligible game,one schedule-derived repeat-pair indicator,and a treated-count-weighted within-week residual contrast. Full272game/32team/17games-each gates precede evaluation. Exactly-even probabilities,ties,the two exposed rows and postseason are excluded under declared rules. Minimum30treated/80control observations across8comparable weeks; no retrospective relaxation.

A fixed three-week circular bootstrap uses the complete18week grid,20,000draws and seed20261104. Only contrast at most−3pp with upper95% below zero can become a descriptive lead. A failure stays failed; no threshold search or reversed betting rule. Team/division/pair dependence,limited weeks and compiler uncertainty remain. No ROI,CLV,betting-price threshold,injury adjustment or future pick is inferred.

This bounded increment registers rules and verifies source metadata only. Next: implement/test outcome-blind schedule assignment,exclusions,gates and bootstrap before downloading game rows; then hash the pinned CSV and preserve every result. No substantial statistical experiment has run today in this increment. Registry preserves all earlier variants.

Validation: JSON parsed;ID uniqueness and prior18registry entries preserved. Current source README retrieved without downloading the CSV. Current phone feeds at13:33Z contain15NFL/14NBA games and0fresh entries; Recommended remains gated. Host resumed the05:35trigger at13:30Z,so these checks are not backdated. No provider collection,deployment or model/journal change.

## Tested implementation before acquisition

research/nfl_rematch.py separates schedule-only auditing from outcome evaluation,checks the frozen coverage/identity gates,retains the two declared exclusions and computes the fixed same-week contrast/three-week bootstrap. Ten synthetic regressions cover outcome blindness,input ordering,coverage/identity/date failures,exclusions,away favorites,within-week weighting,invalid inputs,deterministic20kdraws and exclusive private receipts. All ten pass locally before bulk CSV acquisition. Hosted CI explicitly runs this suite.

Run only after the tested implementation commit is retained: `python3 -m research.nfl_rematch PRIVATE_PINNED_CSV runs/nfl-rematch-2026-11-04`. It creates a source hash receipt,then an outcome-blind schedule audit,then the result,each exclusively. No threshold overrides are exposed by the CLI. A gate/sample failure stays failed; do not edit rules to accommodate the data. This implementation receipt is not an empirical finding.
