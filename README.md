# EV Betting / Football Edge Research Workspace

This repository is no longer just a single Streamlit EV calculator. It now contains several related betting-analysis projects and research artifacts, with **Football Edge** (`nfl-edge/`) as the active production/research track.

## Repository map

| Area | Purpose | Current role |
|---|---|---|
| [`nfl-edge/`](nfl-edge/) | Football Edge platform: NFL production app + NFL/NCAAF research, live odds collection, checkpoints, settlement, CLV, model validation, and forward-shadow evidence | **Primary active project** |
| [`nfl-edge/docs/ncaaf-plan.md`](nfl-edge/docs/ncaaf-plan.md) | Frozen NCAAF research contract | Active NCAAF source of truth |
| [`nfl-edge/research/AGENT.md`](nfl-edge/research/AGENT.md) | Research/agent policy governing NFL Edge experimentation and promotion | Active policy |
| [`nfl-edge/research/experiments.json`](nfl-edge/research/experiments.json) | Durable experiment registry preserving completed, failed, and queued trials | Active evidence registry |
| [`docs/`](docs/) | Historical NFL research outputs, preregistrations, dataset-freeze evidence, promotion-gate material, and forward-shadow reporting specs created before the project was fully consolidated under `nfl-edge/` | Historical/research evidence; retained for provenance |
| `app.py`, `pages/`, `storage.py`, `evsharps_alerts.py`, `strategy_rules.py` | Original Streamlit EV dashboard, bet journal, alerts, strategy rules, and Google Sheets/local persistence | Legacy but retained/usable |
| `options-desk-risk` | Linked Git repository entry for the separate options-desk-risk project | Separate project reference; not part of Football Edge runtime |
| `.github/workflows/` | CI, historical-data, collector, settlement, reporting, and research workflows | Shared automation |

## Active project: Football Edge

Football Edge is the current focus of this repository. It uses shared infrastructure while keeping sport-specific models and evidence isolated.

### NFL Edge

- Deployed app source lives under `nfl-edge/`.
- NFL v1.3 is frozen for forward-shadow measurement.
- Forward observations are measurement-only and are not used to tune the frozen engine.
- The current milestone is **healthy, uncontaminated prospective evidence accumulation**, not a winning week.
- Checkpoint reliability is **mitigated, not yet proven healthy**; late captures remain diagnostic-only.
- Any model/market change requires a new preregistered research cycle.

### NCAAF Edge

- Separate model/evidence stack sharing Football Edge infrastructure.
- Development seasons: **2022–2025 only**.
- Entire **2026 season is sealed prospective shadow** and cannot be used for training, feature selection, or threshold tuning.
- Phase-1 markets: FBS spreads and totals.
- Exact-line identity, same-book de-vigging, immutable checkpoints, dual timestamps, settlement, CLV, and promotion discipline mirror the NFL framework where applicable.
- Current contract and A–G plan: [`nfl-edge/docs/ncaaf-plan.md`](nfl-edge/docs/ncaaf-plan.md).

See [`nfl-edge/docs/README.md`](nfl-edge/docs/README.md) for the current project/workstream map.

## Legacy Streamlit EV dashboard

The original root application remains available and is separate from the Football Edge Vercel app.

It provides:

- manual book/fair-odds EV calculations
- implied/true probability and edge calculations
- fractional Kelly sizing utilities
- bankroll and unit tracking
- parlay/boost handling
- bet logging and CLV tracking
- Google Sheets persistence with local fallback
- mobile-oriented Streamlit pages
- EV alert and strategy-rule utilities

Run locally with:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The root Streamlit application is **not** the deployed `nfl-edge-v2-1` Football Edge application.

## Football Edge deployment

Production Vercel project: `nfl-edge-v2-1` with repository Root Directory set to `nfl-edge`.

The Odds API credential is server-only. Diagnostic/build-time odds samples are not treated as actionable live feeds. Recommendations may only surface when their exact market family has earned the required evidence and all live price/freshness/NY-eligibility gates pass.

## Research principles

The repository deliberately preserves failed experiments and negative results. In particular:

- historical success does not itself authorize promotion;
- prospective evidence is not reused for tuning;
- missed checkpoints are never reconstructed from later information;
- provider observation time governs quote freshness while collector time governs decision-window membership;
- pushes/ties and exact-line identity are explicit settlement concerns;
- a PASS is preferable to manufacturing a betting signal;
- no profitability claim is made without reproducible evidence meeting the governing promotion gate.

## Current high-level status

- **NFL:** frozen forward-shadow engine; accumulating prospective evidence; no shortcut around the promotion gate.
- **NCAAF:** historical/live infrastructure being built under the frozen 2022–2025 development / 2026 prospective split; no modeling before probe + dataset audit/freeze gates pass.
- **Legacy EV dashboard:** retained as a separate Streamlit utility and journal.
- **Historical research material:** retained intentionally for auditability and provenance.

## Safety / scope

This repository supports analytical research and user decision support. It does not autonomously place wagers or alter personal stakes.
