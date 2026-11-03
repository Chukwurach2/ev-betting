# NBA B2B descriptive closing-line screen — 2026-10-28

The first simple B2B-underdog theory was tested under a rule committed before
row download: dff010226714c92b5671162f5a16cff12ed5bd16 (PR41).
No fitted10-variable model or calibrated60% probability was tested.

| Fixed rule | Wins | Losses | Cover rate | Wilson95% interval |
|---|---:|---:|---:|---:|
| Back B2B underdog vs rested opponent (primary) |43|57|43.00%|33.73–52.78%|
| Fade B2B team vs rested opponent, any favorite status (predeclared comparator) |106|79|57.30%|50.09–64.21%|

The simple underdog rule is not supported in this sample. The comparator is
a research lead, not a strategy selected for future betting. Its interval spans
the52.38% break-even rate at-110; neither interval accounts for team/week
dependence. No confirmatory significance or positive expected return is claimed.
Do not convert57.30% observed accuracy into an individual game's probability.

## Accounting and data limits

At an assumed fixed1-unit stake and blanket-110, counterfactual net returns are
-17.9091u(-17.9091%) for the primary and+17.3636u(+9.3857%) for the comparator.
These are price-sensitivity arithmetic, not actual-price historical profits.
Consensus main lines may not have existed at that exact price at any eligible
book. There are no decision-time quote timestamps, fills, fees, limits, injury
publication times or measured CLV.

Source: minmaxxxer, NBA2024-25 Closing Lines and ATS Results:
https://huggingface.co/datasets/minmaxxxer/nba-closing-lines-ats-2024-25
Publisher license label:CC BY4.0. Source bytes326163;
SHA256 e756b4c09c3ac56070f95ad8b6de78fc1b95a1df1049825a5f329d7ce57968cc.
Source unchanged; original code derives rest, recomputes ATS grades and aggregates.
No raw bookmaker data or per-game source rows are republished in this PR.
Publisher compilation/line provenance is not independently certified; the
publisher's license label does not independently establish all upstream rights.

1314source rows,30team IDs;1224regular-season and90postseason. The publisher
omits NBA Cup knockout games. Protocol therefore excluded all185December2024
candidate games before outcomes, avoiding known rest-reconstruction ambiguity
without substituting guessed missing games. First prior-game unavailable16,
non-half-point31,postseason90,other rest-population failures807,eligible185.
The185+1129 exclusions reconcile to1314. These counts are row exclusions in
fixed evaluation order, not disjoint causal explanations. Missing Cup fixtures
and the limited season reduce generalizability; do not call this full-season
coverage.

B2B means consecutive US Eastern calendar dates with18–36h between scheduled
starts. Opponent must have at least48h since its previous source-listed game.
All prior games strictly precede the candidate; team IDs and season boundaries
are explicit. There are100underdogs within185eligible games. Every source ATS
label/margin reconciled with final-score-plus-handicap recomputation; duplicate
IDs, date/time mismatch, invalid scores/lines or label contradictions fail closed.
No filters were changed after results.

## Reproduction and next action

Download the named public source privately, verify the hash, then run offline:
`python3 -m research.nba_closing_screen INPUT.csv OUTPUT.json`
Output creation is exclusive; previous artifacts remain preserved.
Six focused tests cover side/sign handling, chronological order, missing/December
coverage, duplicates, wrong grades, date identities, half-point eligibility and
unit accounting. All6passed locally. No new dependency or provider credit.

This is one exploratory descriptive test, with all fixed variants retained.
2024-25 is now inspected development evidence. It is not an untouched holdout for
future model selection. Keep2025-26/new prospective observations separate and
freeze any fade follow-up before evaluation; do not search additional cutoffs on
this same season until something looks profitable.

The original T-60 NBA B2B model remains data-blocked. Use this screen to prioritize
a separately registered fade/rest study if suitable independent data becomes
available; do not automatically issue future picks from this result. Continue
new cross-sport mechanisms under the same registration/data/holdout discipline.
