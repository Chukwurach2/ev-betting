# Frozen settled CLV: offline measurement adapter (draft)

Issue #16. Branch `codex/frozen-settled-clv`, stacked on PR #19 at
`c99a32c2e7d2835f992572a606d46bb4684af5c7`. No production integration.

## Scope and blocker

On 2026-09-20, repository inspection and a read-only query of
`public.research_dataset_manifests` found only the historical quote manifest:
`v1`, `nfl-2022-2024-spread-total`, 291586 rows, 162 snapshots,
SHA-256 `0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920`.
It is NOT a frozen settled-position export. No real-data CLV was computed.
No historical quotes were repurposed into hypothetical positions; no protected
experiment was rerun. Synthetic fixtures are engineering tests, not evidence.

## Interface

`measureFrozenSettledClv(rawUtf8Json, manifest)` performs no file, database or
network I/O. Manifest fields: `schema: frozen-settled-clv-v1`, `sourceRef`,
`sha256` (raw UTF-8 bytes, including whitespace), `rowCount`, `frozenAt` (explicit
timezone). Store the expected manifest independently; hashing an arbitrary input
alongside itself does not authenticate it. Hash/count failure aborts the batch.

Each row supplies the PR #19 settlement-verifier fields, plus `positionId`,
`contractId`, `canonicalGameId`, `rulesRef`, `settlementSourceRef`, `decisionAt`,
`kickoffAt`, `settledAt`, `entryPair`, `closePair`, and `closeEvidence`.
Each pair has exactly two quotes, selected side first, with `canonicalGameId`,
`market`, `rulesRef`, `bookKey`, `snapshotRef`, `observedAt`, `selection`, `line`,
`americanOdds`. Within each pair the game/book/rules/snapshot/time must match.
The opposite quote must be the actual other team or Over/Under, with negated
spread or identical total line. No fuzzy identity or line matching is used.

`closeEvidence` has `kind: frozen_designated_close`, matching `snapshotRef`, and
`sourceRef`. This is a structural requirement, NOT proof of a true close. The
trusted export owner must authenticate designation, full-game/overtime rules,
settlement source and canonical game resolution separately at EACH provider
snapshot. A provider event ID is never a cross-snapshot canonical game ID.
Entry quotes must precede decision; close must follow decision and precede
kickoff; settlement must follow kickoff and precede the export freeze.
Entry and close books may differ; this measures price movement, not fillability.
No new timing tolerance or close-selection rule is introduced by this module.

## Measurement and exclusions

For each pair: implied American probabilities are normalized proportionally,
`fair = implied(selected)/(implied(selected)+implied(opposite))`.
`clvProbabilityDelta = closeFair - entryFair` (fraction);
`clvPercentagePoints = 100 * clvProbabilityDelta` (percentage points).
Positive means the selected side became more expensive on a de-vigged basis.
This is NOT realized ROI, executable EV, price-return CLV or a promotion gate.
It does not consume or certify an execution price. No batch profitability mean,
statistical inference or model tuning is performed.

Only verified settled half-point full-game spreads/totals are measured. Void,
push, integer-line contracts and moneyline are explicitly excluded: unconditional
valuation needs a separately supported push/tie-mass convention. Props remain
unsupported. Duplicate position IDs exclude every occurrence. Every input row
receives an indexed result or exclusion; missing closes are never zero CLV.
Outputs enforce SHADOW, measurement-only, productionEligible=false.

## Review and verification

Run `node --test tests/frozen-settled-clv.test.mjs tests/settlement-verifier.test.mjs tests/build-environment.test.mjs`
from `nfl-edge`. Synthetic tests cover magnitude/units/sign, same side, mismatched
game/line/book/rules/snapshot/time, duplicate quotes/positions, missing close,
invalid odds, invalid chronology, hash/schema/count, settlement mismatch,
push/void/integer line, late settlement, spread direction, null and empty input.

Owner review must approve the frozen export mapping and actual-close evidence
before any real-data run. Retarget to main only after PR #19 merges, preserving
its preview paid-check guard. No collectors, watchdogs, frozen specs, production
state, promotion gates or research registries were edited. Odds API credits: 0.
