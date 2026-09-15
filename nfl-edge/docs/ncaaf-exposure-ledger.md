# NCAAF Research Exposure Ledger

Every inspection of 2026 NCAAF data in a claim-bearing way is recorded here.
Anything listed with outcomes exposed is burned for that hypothesis family.

## Firewall

**Frozen:** 2026-09-15
**Boundary:** 2026-11-01T03:59:59Z (2026-10-31T23:59:59 America/New_York)
**Scope:** Global across all NCAAF research.

## Entries

| Date | Dataset / Range | Fields | Outcomes? | Family | Purpose |
|------|----------------|--------|-----------|--------|---------|
| 2026-09-15 | 2026-09-01 → 2026-09-15 live collector | quote rows (line, price, book) | No | microstructure | Timestamp QA only |
| 2026-09-15 | 2026-09-15 Odds API events | commence_time, team names | No | n/a | Firewall feasibility count |

## Rules

- Pre-registration required before any post-firewall exposure.
- Operational QA (missing rows, latency, schema) does not burn.
- Outcome inspection burns the range for that family permanently.

## 2026-09-15 — Price-pressure historical exploration CLOSED

**Status:** Historical exploration permanently closed. No further variants.

**Artifacts (immutable):**
- Feasibility: run 35014865225 (50,650 qualifying contracts)
- Descriptive: run 35015417472 (Q1 r=0.51, Q2 67.1% strong-pressure hit rate)
- Clustered: run 35015825735 (n=1,190, 1,038 clusters, CI [64.4%, 69.8%])

**Frozen candidate:** nfl-edge/docs/ncaaf-totals-pressure-v1-frozen.md (v1.0)

**Firewall:** 2026-11-01T03:59:59Z. Post-firewall outcomes = prospective confirmation only.
No outcome inspection of pre-firewall shadow predictions for rule modification.

**Fields exposed (historical):** provider_event_id, market, selection, line,
fair_probability, observed_at, book_key (2022-2024 only, totals/spreads).

**Exposure date:** 2026-09-15. **Purpose:** Burned descriptive for candidate selection.
