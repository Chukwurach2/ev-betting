# NFL prospective prop/alternative-market data layer — design

**Date:** 2026-09-16. **Status:** DESIGN ONLY. **Zero API calls made or authorized.**
A separate worker implements this. No live paid collection in the build phase —
dry-run validation only, zero paid `/odds` calls.

**Purpose:** build the prospective data layer for NFL prop/alternative markets
(standing rule, 2026-09-16): C/D are terminal on the current historical dataset;
no new NFL historical feature families. The research substrate going forward is
prop/alternative-market data collected prospectively with controlled decision
snapshots, canonical identity, raw quote capture, completeness monitoring, and
empirical credit tracking. No new NFL alpha lane opens until (a) enough
prospective data supports a preregistered test, or (b) a genuinely new historical
source passes free feasibility.

**Governing evidence this design is built on:**

- Probe (2026-09-16): at kickoff−24h, `player_pass_yds`, `player_pass_attempts`,
  `player_rush_attempts` had multi-book depth; `player_sacks` was absent.
  Bulk historical returns 422 INVALID_MARKET for prop markets → props are
  per-event pulls only.
- H-D incident (`docs/nfl-h-d-pull-incident-2026-09-16.md`): the provider
  re-issues event IDs across snapshots (median 2, max 5 per game, measured
  2026-09-16). **Provider event IDs are snapshot-scoped; never carried across
  checkpoints.**
- H-D cost lesson: billed ≠ nominal — historical per-event odds billed
  ~18.6/call for 2 markets vs the 10×2 nominal. The economics model below
  carries an explicit planning margin until calibration measures the live
  number; implementers must never project spend from nominal pricing alone
  (standing platform constraint, 2026-09-16).

---

## 1. Canonical identity (extend, don't replace)

Reuse `ops/nfl_canonical_identity.py` as-is for the matching logic.

### 1.1 What already exists and is reused

- `match_events(odds_events, nv_games, aliases, seasons)` — pure, deterministic
  matcher on (date, home_abbr, away_abbr) with explicit alias-table resolution,
  no fuzzy matching, explicit states (matched / ambiguous / unmatched /
  missing_from_source). **Reused unchanged.**
- `nfl_team_aliases.json` — 32 explicit team-name → abbreviation aliases.
- Migration 017 table `nfl_game_identity`, keyed by
  `(nflverse_game_id, odds_event_id)` composite PK — the proven pattern for
  provider-ID multiplicity.

### 1.2 The one new function to add

`normalize_live_event(provider_event) -> oe_dict`: adapts the **live**
`/v4/sports/americanfootball_nfl/events` payload
(`{id, home_team, away_team, commence_time}`) into the oe-dict shape
`match_events()` expects (`event_id`, `home`, `away`, `kickoff` as a datetime).
`commence_time` is ISO-8601 → parsed to an aware UTC datetime. Nothing else
changes: the same explicit alias table, the same (date, home, away)
deterministic attempts, the same ±1-day UTC-date fallback. Ambiguous or
unmatched resolutions are recorded, never guessed.

### 1.3 Prospective canonical IDs

Canonical game identity for 2026+ games is the nflverse `game_id`
(e.g. `2026_01_KC_BUF`) from the synced `public.games` table
(`sync_schedule.py` already populates it; `nflverse_schedules_2026.json`
is bundled). The prop collector enumerates games from `public.games`
(`sport='nfl'`), never from provider event lists.

### 1.4 The snapshot-scoping rule (hard)

A provider `event_id` is valid **only for the tick in which it was resolved**.
At each checkpoint the collector:

1. Fetches the live events list (free — skill-verified).
2. Normalizes + matches each due game via §1.2 against its canonical schedule row.
3. Stores the resolved `provider_event_id` **on that snapshot row only**
   (`provider_event_id_at_snapshot`).

It never reads a previously stored provider ID and reuses it. The join path
for analysis is: `(canonical_game_id, provider_event_id_at_snapshot)` —
exactly the composite-pair pattern proven in migration 017, but with the
provider half re-resolved per snapshot instead of accumulated.

Match provenance (`match_type`, resolved tick id, `resolved_at`) is stored on
the snapshot row so a later forensics pass can distinguish
`alias_date` from `swapped_date_shift` resolutions.

---

## 2. Checkpoint model

### 2.1 Checkpoints (configurable)

Default offsets before kickoff, configurable via env
`NFL_PROP_CHECKPOINTS` (JSON list of minute offsets) — no code change to
retune:

```
T-24h (1440), T-12h (720), T-6h (360), T-3h (180), T-90m (90), Close (5)
```

Rationale: the C/D probe proved quote depth at T-24; the T-3/T-90m/Close
checkpoints capture line movement into kickoff; T-12/T-6 give intraday
trajectory. Props are rarely posted before T-24, so no opener checkpoint
(the featured collector's 6-day opener does not apply here — empty responses
are free, but resolving IDs for games with no posted props wastes nothing
except a free events-list row, and the attempts table records it explicitly).

### 2.2 Collection mechanics (reuse, don't rebuild)

The prop collector **reuses `ops/checkpoints.py`'s `plan()`** with the
prop window config: 90-minute collection windows (deadline = target + 90m,
capped strictly before kickoff), per §2 of the existing module's comment —
the wide-window fix already landed for the featured collector and applies
identically here. Every snapshot row records `checkpoint_target_at`,
`started_at`, `captured_at`; quotes carry their own `captured_at`; research
uses actual capture times, not nominal window names.

Collection tick: the workflow runs every 60 minutes in-season (props move
slower than featured lines; cost per game-checkpoint is the binding
constraint, not latency). The workflow schedule is the cadence; windows do
the due/missed accounting.

### 2.3 Per-checkpoint sequence (the ID rule, operationalized)

For each due (game, checkpoint):

1. Resolve the provider's **current** event ID from the live events list
   fetched **this tick** (§1.2). One free events-list call per tick covers
   all due games (same bulk-discipline as `saturday_timing.py`: one free
   call, client-side per-game resolution).
2. If no event ID resolves (game not yet listed, ambiguous match) →
   attempts row with status `no_event_id`, zero paid calls. Never fall back
   to a previously resolved ID.
3. Credit pre-check (§4.4): project this tick's paid calls; if projection
   exceeds the per-run cap → status `aborted_cap`, zero paid calls.
4. Paid call: `GET /v4/sports/americanfootball_nfl/events/{eventId}/odds`
   with `regions=us`, `markets=<v1 keys>`, `bookmakers=<prop-book filter>`,
   `oddsFormat=american`. One call carries **all** v1 markets for the game.
5. Store snapshot (§5): canonical ID + the tick-fresh provider ID +
   target/capture timestamps + all returned quotes verbatim.
6. Heartbeat (§6); per-call quota logging (§7).

---

## 3. Market set v1 (bounded)

### 3.1 The v1 list

| # | Market key | Meaning | Evidence |
|---|-----------|---------|----------|
| 1 | `player_pass_yds` | QB passing yards O/U | Probe: multi-book depth at T-24 ✓ |
| 2 | `player_pass_attempts` | QB pass attempts O/U | Probe: multi-book depth at T-24 ✓ |
| 3 | `player_rush_attempts` | Rusher rush-attempts O/U | Probe: multi-book depth at T-24 ✓ |

**Excluded from v1:** `player_sacks` — the probe found it absent at T-24 in
both kickoff−24h games. It is parked, not dropped: it may enter only via a
future bounded re-validation gate (§3.3), never silently.

### 3.2 Why these three (prioritization criteria)

- **Reliable multi-book availability:** proven at the T-24 decision window by
  the probe — no assumption.
- **Stable settlement definitions:** all three settle on standard nflverse
  `player_stats` columns (passing yards, pass attempts, rushing attempts) —
  the same free settlement source the C/D spec already validated.
- **Meaningful pregame liquidity:** QB/RB core props are the deepest prop
  menu at US books; thin-tail props (anytime TD, alt lines) are excluded by
  construction.
- **Controllable decision-window coverage:** posted by T-24 (probe-proven),
  so the T-24 checkpoint — the natural prereg decision window — is
  collectible.
- **Reasonable collection cost:** 3 markets = ~3 nominal credits per
  game-checkpoint call (§4). Each additional market is a linear cost
  increase, so the list stays at 3 until calibration says otherwise.

### 3.3 Validation gate (each market, before it joins standing collection)

No market enters the standing schedule on documentation alone. Two gates:

**Gate A — availability evidence (free).** The C/D probe already satisfies
Gate A for all three v1 markets (T-24 multi-book depth observed).

**Gate B — bounded live calibration run.** Before the standing schedule
starts, one small user-authorized calibration run: 3 games × 2 checkpoints
(T-24, T-3) = up to 6 paid calls (~≤30 credits at nominal pricing, bounded
by the per-run cap). It measures:

1. % of games where the market is present in the response at ≥1 checkpoint;
2. % of games with **≥2 US books** quoting the position at ≥1 checkpoint;
3. **Empirical billed credits per call** (from `x-requests-*` headers —
   the H-D lesson: measure, don't assume);
4. ID-resolution success rate (events-list → canonical match).

**Pass rule:** criterion 2 ≥ 2/3 games AND billed credits/call within 2× of
the §4 model. A market failing Gate B is **dropped from v1 with a documented
exclusion** (reason + measured numbers); it does not retry without a
methodology or data-source change. `player_sacks` must pass Gates A+B —
re-run against live data, bounded — before it can ever join v1.

### 3.4 Participant scope

The paid call returns quotes for **all participants** the provider lists for
each market (every QB, every rusher). The collector stores **all of them
verbatim** — it never filters to a "designated" player. Designation rules
(starting QB, RB1, lagged snap share) are analysis design frozen later in a
prereg; collecting the full participant menu means a designation change
never requires a re-pull.

---

## 4. Collection economics model

### 4.1 Assumptions (explicit; the implementer re-verifies in dry-run)

- **A1 — live per-event cost = (# unique markets present in response) ×
  (# regions), 1 credit each.** Grounded: `collect-odds.yml` comment
  ("cost = markets x regions = 2 x 2 = 4 credits per event fetch") and the
  skill ("The /odds endpoint spends credits (weighted by markets/bookmakers)").
- **A2 — regions = `us` only (1 region).** The `bookmakers` filter (≤10
  books) is cost-neutral (priced-pull spec §0.1). Prop-book filter:
  `draftkings,fanduel,betmgm,betrivers,espnbet,williamhill_us,fanatics`
  (7 books — the US books that post props; cost-neutral, payload-light).
- **A3 — empty-data responses cost 0** (provider docs, quoted in the
  priced-pull spec §0.1: "responses with empty data do not count towards
  the usage quota"). Markets not posted at a checkpoint are free.
- **A4 — live events-list ID resolution is free** (skill-verified:
  "/v4/sports and /v4/sports/{sport}/events are free"); one call per tick
  covers all due games.
- **A5 — billed ≠ nominal until calibration lands.** Historical per-event
  odds billed ~18.6/call for 2 markets vs 10×2 nominal. All planning numbers
  below carry a **2× margin**; the calibration run (§3.3) replaces the margin
  with the measured live-prop number. Future preregs budget from the
  empirical cost model (§7), never from A1 alone.

### 4.2 Per-cycle request plan and exact expected credits

Per (game, checkpoint) cycle:

| Step | Endpoint | Paid? | Nominal credits |
|------|----------|-------|-----------------|
| ID resolution (shared per tick) | `GET /v4/sports/americanfootball_nfl/events` | Free (A4) | 0 |
| Odds pull | `GET /v4/sports/americanfootball_nfl/events/{id}/odds` (regions=us, markets=3 v1 keys) | Paid | **3** (A1: 3 markets × 1 region) |

- **Per game-checkpoint:** 3 nominal credits (all 3 markets present) → **6
  with the 2× margin**; fewer when markets are unposted (A3, free).
- **Per game per week** (6 checkpoints): 18 nominal → **36 budgeted**.
- **Full Sunday slate** (13–16 games): 234–288 nominal → **budget ceiling
  ~576/week**.
- **Full 18-week season** (272 REG games): ~4,900 nominal → **~9,800 budgeted
  ceiling** — roughly half a 20K-tier month. Acceptable only if calibration
  and early completeness justify it; the standing schedule starts only on
  explicit user authorization after the calibration run.

These are planning numbers. The dry run (§4.5) projects each run's cost from
due checkpoints × the empirical-per-call number (or A1×2×margin before
calibration); nothing is billed in the build phase.

### 4.3 Hard per-run caps (before any paid call)

- `NFL_PROP_RUN_CREDIT_CAP` (env, default **90**): the run projects its paid
  calls (due checkpoints × expected credits/call); if projection > cap, the
  run **aborts with zero paid calls** and logs status `aborted_cap` — the
  `nfl-hd-pull.yml` ceiling-abort pattern, applied per run instead of per pull.
- `NFL_PROP_LIVE_OK` (env, default unset): the collector **refuses live
  mode** unless this is `1`. Build phase runs are dry-run by construction.
- Per-tick retry discipline (from the Saturday timing lesson): max 1 retry
  per paid call, only on request failure, logged with tick timestamp +
  reason; retries write under the same tick timestamp (no cadence shift).

### 4.4 What the implementer measures in dry-run

Dry-run executes the entire cycle — schedule enumeration, checkpoint
planning, free events-list fetch, ID resolution via §1.2, per-run
projection, cap check — and makes **zero `/odds` calls**. Output is an
immutable JSON artifact (uploaded per run): resolved (canonical,
provider) ID pairs, due checkpoints, projected credits vs cap, would-abort
decisions. Dry-run makes **zero DB writes** to production tables (attempts
rows with a `dry_run` status would pollute completeness accounting, so they
are not written; the artifact is the record).

---

## 5. Schema (migration 020; migration 021 for §7)

Style: additive migrations like 017/018; append-only triggers like 018.
Raw snapshots are immutable (insert-only); nothing is reconstructed later.
Consensus/dispersion are **derived in views** from raw quotes — never
stored twice, never recomputed into the raw tables.

### 5.1 `nfl_prop_snapshots` — one row per (game, checkpoint)

```sql
CREATE TABLE public.nfl_prop_snapshots (
    snapshot_key TEXT PRIMARY KEY,          -- sha256(canonical|checkpoint|provider_id|captured_at)
    canonical_game_id TEXT NOT NULL,        -- nflverse game_id (never a provider id)
    provider_event_id_at_snapshot TEXT,    -- resolved THIS tick; NULL if unresolvable
    match_type TEXT,                        -- from the identity matcher (§1.2)
    resolved_at TIMESTAMPTZ,                -- the tick that resolved the ID
    season INTEGER NOT NULL,
    week INTEGER,
    checkpoint_name TEXT NOT NULL,          -- T-24h / T-12h / T-6h / T-3h / T-90m / Close
    checkpoint_target_at TIMESTAMPTZ NOT NULL,
    kickoff TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    credits_consumed INTEGER NOT NULL DEFAULT 0,
    quota_before INTEGER, quota_after INTEGER,
    context JSONB NOT NULL DEFAULT '{}',    -- mos_station + eligible MOS runtime ref (§5.5)
    UNIQUE (canonical_game_id, checkpoint_name, season)
);
-- append-only trigger (same pattern as 018's nfl_mos_archive_no_rewrite)
```

### 5.2 `nfl_prop_quotes` — one row per (snapshot, book, market, participant, side)

```sql
CREATE TABLE public.nfl_prop_quotes (
    snapshot_key TEXT NOT NULL REFERENCES public.nfl_prop_snapshots(snapshot_key),
    book TEXT NOT NULL,
    market TEXT NOT NULL,                   -- player_pass_yds | player_pass_attempts | player_rush_attempts
    participant_raw TEXT NOT NULL,          -- provider's player string, verbatim
    participant_key TEXT,                   -- canonical player key; NULL until alias-resolved (§5.6)
    side TEXT NOT NULL CHECK (side IN ('over','under')),
    line NUMERIC NOT NULL,
    price_american INTEGER NOT NULL,        -- as quoted; fair probs derived in views
    captured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_key, book, market, participant_raw, side)
);
-- append-only trigger
```

### 5.3 `nfl_prop_collection_attempts` — append-only per (game, checkpoint, run)

Every due checkpoint gets a row, including failures — missing snapshots are
visible immediately (§6):

```sql
CREATE TABLE public.nfl_prop_collection_attempts (
    id SERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id TEXT NOT NULL,
    canonical_game_id TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    provider_event_id_resolved TEXT,        -- NULL when unresolvable
    status TEXT NOT NULL CHECK (status IN
        ('ok','no_event_id','empty_response','request_failed',
         'skipped_budget','aborted_cap','ambiguous_match')),
    credits_consumed INTEGER NOT NULL DEFAULT 0,
    quota_before INTEGER, quota_after INTEGER,
    detail TEXT
);
CREATE INDEX ... ON (canonical_game_id, checkpoint_name, attempted_at);
```

### 5.4 Derived views (no duplicated state)

- `nfl_prop_consensus`: per (snapshot, market, participant_raw): `book_count`,
  `median_line`, `line_range` (dispersion), same-book de-vigged `fair_over` /
  `fair_under` (median across books, matching the C/D spec's proposed
  consensus rule: median line, ≥2 books, same-book de-vig).
- `nfl_prop_completeness`: per (week, checkpoint_name): `due`, `captured`,
  `missed` (= due − captured − empty), `empty`, `no_event_id` — the table
  the health check reads (§6).

### 5.5 Context at snapshot time

`context` JSONB on each snapshot carries what was knowable at capture:
`mos_station` + the mechanically-latest MOS runtime eligible under the
frozen H-N2 rule (`runtime + 4h ≤ kickoff − 24h`) — a free join against
`nfl_mos_forecast_archive`, recorded as a reference, not copied data.
Injury/lineup context: recorded as source + retrieval timestamp only
(e.g. final designations source); building a new injury/lineup pipeline is
an explicit non-goal for v1.

### 5.6 Settlement / outcomes

Settlement rows live in `nfl_prop_settlements` (populated by a later free
settlement job from nflverse `player_stats`; not part of the v1 build):
`(canonical_game_id, market, participant_raw, participant_key, actual,
line, residual, status)` where status ∈ `settled | push (actual==line) |
void (DNP/inactive)`. Join is on (canonical game, participant name);
`nfl_prop_participant_aliases` (provider string → canonical player key,
explicit entries like the team alias table, NULL until resolved) bridges
provider naming to nflverse. Designation (which participant is "the"
starter) is analysis design, frozen in a future prereg — never a collection
filter (§3.4). No outcomes are inspected during collection.

### 5.7 Closing price

The `Close` checkpoint (−5m) **is** the closing-price capture; no separate
mechanism. A future prereg may define "closing" as the last captured
checkpoint before kickoff; the raw rows support either definition.

---

## 6. Completeness / health monitoring

### 6.1 Heartbeat

The collector writes the existing heartbeat after every run
(`python ops/heartbeat.py prop_collector` with `HEARTBEAT_DETAIL` JSON):

```json
{"due": 42, "captured": 39, "missed": 1, "empty": 2,
 "credits_used": 117, "run_id": "…", "dry_run": false,
 "credits_remaining": 15400}
```

Detail keys are fixed so the health check and any dashboard can rely on them.

### 6.2 `/api/health` integration (extend `ops/health.py`)

- Add `THRESHOLDS["prop_collector"] = 6h`.
- `prop_collector` is **non-core**: staleness escalates to `degraded`, never
  `down` — the prop layer is research infrastructure and must not flap the
  shadow pipeline's status. (This is a deliberate departure from CORE; the
  featured collector stays core.)
- Missing-snapshot visibility: `check()` additionally reads
  `nfl_prop_completeness` for the latest checkpoint window and exposes
  `missed` in the component detail — a missed snapshot is visible in the
  same health payload, immediately, not discovered weeks later in analysis.
- Out-of-season: the existing `SEASON_WINDOW` idle semantics apply unchanged.

### 6.3 Health-watch cron pattern

The health-watch runbook gains a `prop_collector` section reading the same
heartbeat: alert (not page) when in-season staleness exceeds 6h or when the
latest window's `missed` count is nonzero for two consecutive runs.
Staleness semantics: a missed single snapshot is `degraded`-worthy context
in the detail payload; the component status flips on sustained staleness,
not on one gap.

---

## 7. Empirical credit tracking (extend `quota_log.py`)

Standing rule: future research economics are **measured, not assumed**.

- Migration 021 (additive columns on `ncaaf_quota_log` — the table is
  already shared across sports via its `sport` column):
  `endpoint TEXT` (e.g. `nfl_prop_odds_call`, `nfl_prop_events_list`),
  `n_markets INTEGER`, `billed_credits INTEGER` (from `x-requests-*`
  response headers, which the odds-api CLI already surfaces),
  `run_id TEXT`.
- The prop collector logs **every** provider call through the extended
  `log_quota()` — including the free events-list calls (logged at 0, so a
  future audit can see the full request pattern) — with per-call
  `credits_used` computed from before/after `x-requests-used`.
- New view `nfl_prop_cost_model`: rolling 30-day empirical
  avg/p50/p90 **billed credits per odds call** by (endpoint, n_markets),
  plus per-game-checkpoint realized cost. This view — not A1 — is what any
  future prereg or authorization request cites for budgeting. It is the
  standing answer to the H-D lesson.

---

## 8. Workflow sketch (`nfl-prop-collect.yml`)

- `on`: `schedule` every 60 min in-season + `workflow_dispatch` with a
  `live` boolean input (default false).
- Steps mirror `collect-odds.yml`'s bootstrap (tarball source load,
  secrets check, `ops/migrate.py`, schedule sync) then run
  `ops/nfl_prop_collect.py`:
  - `--dry-run` (default): §4.4 — full cycle, zero `/odds` calls, JSON
    artifact upload, zero DB writes to prod tables.
  - live: only when the dispatch input is true **and**
    `NFL_PROP_LIVE_OK=1`; refuses otherwise.
- Concurrency group `nfl-prop-collect`, `cancel-in-progress: false`.

---

## 9. Explicit non-goals

1. **No hypothesis optimization.** This layer collects data; it tests nothing.
   No thresholds, no feature selection, no market selection happens here.
2. **No shadow signals as actionable picks.** The prop layer has no pick
   engine, no EV computation, no app surface. It never emits anything a
   user could mistake for a recommendation.
3. **No new alpha lane.** Opening a research lane requires (a) enough
   prospective data for a preregistered test, or (b) a new historical source
   passing free feasibility — neither is claimed here.
4. **No changes to frozen artifacts.** The frozen 2022–2024 NFL dataset, the
   frozen v1.3 shadow model, the NCAAF dataset, and all frozen preregs are
   untouched. Migrations are additive; no existing table is altered
   incompatibly.
5. **No live paid collection in the build phase.** Dry-run validation only;
   zero paid calls. The calibration run (§3.3) and any standing schedule
   require separate explicit user authorization at exact credit numbers.
6. **No new injury/lineup pipeline in v1** — context references only (§5.5).
7. **No resurrection of retired families.** C/D and H-N2/N3/N6/N7/N8 stay
   retired; this layer does not re-litigate them.

---

## 10. Build-phase acceptance (what "done" means for the implementing worker)

- [ ] Migration 020 (+021) applies cleanly via `ops/migrate.py`; append-only
      triggers verified (UPDATE/DELETE raise).
- [ ] `normalize_live_event()` + `match_events()` unit-tested on fixture
      live-events payloads (matched / ambiguous / unmatched / no_alias).
- [ ] Dry-run over a real in-season tick: JSON artifact shows resolved
      (canonical, provider) ID pairs fresh per checkpoint, projection math,
      and cap-abort behavior; **zero `/odds` calls, zero prod-table writes**.
- [ ] Live mode refuses without `NFL_PROP_LIVE_OK=1`.
- [ ] Per-run cap abort verified in dry-run projection (force a low cap).
- [ ] Health: `prop_collector` heartbeat written; `health.py` reports it as
      non-core `degraded` on staleness, never `down`.
- [ ] Quota logging: free events-list calls logged at 0; cost-model view
      returns rows after any paid calibration call.
- [ ] CI green; design doc unchanged by the implementation (any deviation
      returns here for a design amendment, not a silent fix).

---

## Amendments

### 2026-09-16 — `prop_collector` health component: missing heartbeat reports `missing`, no escalation

Implementation decision for the never-commissioned `prop_collector` health
component. "Never commissioned": pre-calibration, no standing schedule
exists yet — Gate B (§3.3) and any standing collection require separate
explicit user authorization.

- A **missing** `prop_collector` heartbeat reports component status
  `missing` with **no escalation** — not `degraded`, and no health-watch
  alert. Rationale: before calibration there is no standing schedule and
  therefore no staleness to measure; escalating would hold the health pill
  at DEGRADED and spam alerts from day one.
- Once a heartbeat **exists**, the original §6.2/§6.3 semantics apply
  unchanged: in-season staleness beyond the 6h threshold → `degraded`
  (never `down`; non-core), and health-watch alerts (not pages) on
  sustained staleness or nonzero `missed` counts for two consecutive runs.
- This changes no threshold, no gate, and no other component's behavior.
  The component transitions from `missing` to live exactly once — when the
  first heartbeat is written.

*Design only. Zero API credits spent. Zero outcomes inspected. No pull,
no collection, and no research lane authorized by this document.*
