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
