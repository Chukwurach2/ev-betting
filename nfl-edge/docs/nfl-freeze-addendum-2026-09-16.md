# NFL frozen dataset — addendum 2026-09-16

## What happened

On 2026-09-12 between ~14:53 and ~15:32 UTC, a moneyline backfill
(migration 010, commit `83b7692e`) inserted **47,080 `FULL_GAME_MONEYLINE`
rows** into `nfl_edge_historical_quotes` — after the freeze record
(~12:45 UTC). The table now holds 338,666 rows vs the frozen 291,586.

## Finding

The 47,080 rows are an **out-of-scope additive backfill, not a freeze
break**:

- They are exactly the `FULL_GAME_MONEYLINE` market; spread (149,602) +
  total (141,984) = 291,586 = the frozen count.
- Temporal cross-tab (forensic runs 35044649206, 35044941694): all
  47,080 moneyline rows have `collected_at` post-freeze; all 291,586
  spread/total rows have `collected_at` pre-freeze. Zero rows cross the
  boundary in either direction. Same 162 snapshots, zero duplicate keys.
- **Value-level integrity of the frozen subset** (forensic run
  35045570451, read-only): all 291,586 spread/total `quote_id`s recompute
  exactly from the original backfill construction
  (`sha256("rawZuluTs|event|book|market|selection|str(float(abs(line)))|price")`
  with the raw provider envelope timestamp string); all stored
  `fair_probability` values re-derive exactly under the original
  spread/total pairing and de-vig logic; zero unpaired groups.
- No post-freeze UPDATE to frozen-scope rows was found. (Commit
  `0ba8f600`, 2026-09-15, "store signed spread lines", changed backfill
  code only — the frozen table retains absolute spread lines; zero
  spread rows carry a negative line.)

## Consequence

Frozen-scope work (including H-N6) filters to
`market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')`. The moneyline rows
are retained in the table but are outside the frozen dataset and outside
every frozen claim. The recorded spread/total fingerprint
`43f853a44bf93937d85149ca5fd7241b` was produced by an undocumented
2026-09-12 construction and is not independently verifiable; the
value-level recomputation above supersedes it as the integrity evidence.

## Method note (for future verifiers)

`quote_id` hashes the **raw provider timestamp string**
(e.g. `2024-09-04T11:55:38Z`), not the parsed timestamptz — re-serializing
`observed_at` from the database (`2024-09-04 11:55:38+00:00`) yields a
different string and a 100% mismatch. The line component is
`str(float(abs(line)))`: Python float rendering turns integral lines
into `"45.0"`, while the numeric column reads back as `Decimal("45")`.
