# Preregistration: steam-v1 (NEW HYPOTHESIS)

**Status: PREREGISTERED — no results viewed. No analysis code run.**
**All work under this preregistration is SHADOW (research only).**

- Preregistered: 2026-09-12
- Question: do coordinated multi-book line moves ("steam") predict
  continuation to the close? This tests market-wide momentum, which is
  distinct from market-alpha-v1 Track A (single-book leadership, which
  found nothing) — a market can have no persistent leader yet still
  exhibit predictable coordinated moves.

## Design

- Dataset: frozen 2022–2024 spread/total (fingerprint
  `43f853a44bf93937d85149ca5fd7241b`), 162/162 snapshots, 291,586
  quotes. Zero API credits; read-only Neon.
- Unit: one (game, market) with ≥3 pre-close snapshots.
- For each consecutive pre-close snapshot pair (s1 → s2):
  - For each book present at both s1 and s2 with a valid pair at the
    modal line, compute the fair-prob move Δ = f(s2) − f(s1).
  - **Steam event**: ≥5 books move the same direction, each by |Δ| ≥ 0.01
    (1 probability point), and the median |Δ| across all books ≥ 0.005.
    (Thresholds fixed here; not tuned.)
  - Direction d = sign of the median move.
- Outcome: the close move from s2 to the closing snapshot, measured on
  the exact-line LOBO consensus (all books; no evaluated-book exclusion
  needed — the bet is placed at consensus, not at a book's price).
  - **Direction test**: does the close move in direction d more often
    than 0.5? One-sided binomial, game-level majority vote (a game
    "agrees" if its mean steam-event agreement > 0.5; ties excluded).
  - **Tradeability test**: CLV of betting direction d at the s2 consensus
    price, scored vs the closing consensus. One-sided t-test of game-level
    mean CLV > 0. Practical bar: mean ≥ 0.01.
- Holm correction across the two confirmatory tests (α = 0.05).
- Min 30 games with ≥1 steam event, else `infeasible`.
- Point-in-time safety: s2's consensus uses only s2 information; the close
  is an ex-post benchmark. Multiplicative de-vig throughout.

## Verdicts

`infeasible` | `no_edge` | `steam_edge` (direction significant AND
CLV significant AND mean CLV ≥ 0.01). A positive finding selects the
hypothesis for FORWARD-SHADOW validation only — the mandatory promotion
gate remains positive CLV against actual closing lines on forward data.
Historical results never promote.

## Why this might work where Track A failed

Track A asked whether a *specific book* persistently leads. Steam asks
whether *coordinated* moves — which arise from news, injury information,
or correlated sharp action hitting multiple books at once — carry
information beyond the move itself. The mechanism is information diffusion
speed, not book hierarchy.
