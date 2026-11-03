# NBA replication uncertainty implementation — 2026-10-28

Implements only the already registered uncertainty component of
research/protocols/nba-b2b-fade-replication-v1.json (registration24efc407).
No 2025–26 source rows downloaded or evaluated; no second statistical
experiment, filter search, model promotion or future pick.

research/nba_replication_uncertainty.py builds Monday-start Eastern calendar
week counts, retaining empty intervening weeks. Fixed circular four-week
blocks preserve within-block ordering, wrap at the endpoint and truncate
to the original week count. Each replicate weights games by actual sampled
decision counts; it does not average unequal weekly rates. It uses the frozen
seed20261029,20,000 valid replicates,100,000-attempt cap and nearest-rank95%
interval. Zero-decision draws are counted and skipped.

Missing/duplicate selected game IDs, non-win/loss outcomes and timezone-free
starts fail. The helper has no source downloader or eligibility selector:
a separately audited, protocol-bound caller remains necessary. It exposes
the100-decision/20-nonempty-week floor and labels insufficient samples
descriptive/inconclusive even when a tiny all-win sample gives[1,1].
No interval here controls for season-long team dependence or supplies
executable-price profitability evidence.

Six synthetic tests verify Eastern/UTC week boundaries, empty weeks,
block wrapping/truncation, game weighting, zero-decision draws, rejected
bad inputs, sample floors and deterministic input-order invariance.
All14 tests passed including the8 original-screen tests:
python3 -m unittest tests.test_nba_replication_uncertainty tests.test_nba_closing_screen -v

Next: implement and test the frozen2025–26 eligibility/source audit before
row-level evaluation, no earlier than2026-10-29 Eastern. Preserve any data
audit blocker or disappointing result. The historical price/model gaps persist.
