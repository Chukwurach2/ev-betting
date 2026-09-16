# H-D precipitation feasibility — free MOS count → minimum-cost pull proposal

**Date:** 2026-09-16. **Status:** FEASIBILITY ONLY — no pull authorized, no credits spent,
no outcomes inspected, no prereg frozen, no other families touched.
**Credits spent producing this document: 0.** 359 free IEM MOS archive calls (anonymous, ~1.2s pacing).

Implements the **frozen** precipitation definition from
`docs/nfl-cd-priced-pull-spec-2026-09-16.md` §2.0 **verbatim** (IEM MOS GFS only;
mechanically latest 6h-grid runtime with runtime+4h ≤ T_dec; F = max q06 over valid
periods overlapping [kickoff, kickoff+3.5h]; treated iff F ≥ 0.25";
roof ∈ {outdoors, open} from the nflverse schedules roof column; domes/closed excluded;
missingness censored never backfilled; PoP descriptive only).
Script: `nfl-edge/ops/nfl_h_d_precip_feasibility.py`.

## 1. Eligible-N funnel (every step, exact counts)

| # | Step | N | Notes |
|---|---|---|---|
| 1 | 2023–2024 REG games (nflverse schedules) | 544 | live `games.csv`; only the 10 non-outcome columns read (strict whitelist — scores/realized weather never accessed) |
| 2 | roof ∈ {outdoors, open} | 369 | 358 outdoors + 11 open (2023: 191, 2024: 178). domes/closed excluded wholesale |
| 3 | Stadium → MOS station resolved | 359 | 30 verified stations; see missingness ledger |
| 4 | MOS cycle retrieved, numeric q06 in window | 359 | **zero** fetch failures, zero absent cycles, zero no-usable-q06 windows |
| 5 | **G_precip: F ≥ 0.25" (treated)** | **61** | 2023: 34, 2024: 27. 298 controls not purchased (per spec §2.1) |
| 6 | Treated ∩ frozen identity (≥1 Odds event id) | **60** | 1 treated game has no identity match → excluded from pull |

### Missingness ledger (all censored, never backfilled)

- **10 international games, no NWS MOS coverage:** Allianz Arena 1 (2023), Arena Corinthians 1
  (2024 São Paulo), Deutsche Bank Park 2 (2023 Frankfurt), Tottenham Stadium 4, Wembley 2.
  Explicit `unmatched` states in the station map; never nearest-guessed.
- **0** MOS request failures across 359 fetches (one same-cycle retry allowed, then censor).
- **0** absent cycles; **0** windows without a numeric q06 row.
- **1 treated game without identity:** `2024_18_HOU_TEN` (HOU@TEN, Nissan Stadium,
  F=2.0") — no Odds event id in the frozen canonical-identity layer (run 35041420559
  artifact). Cannot be pulled; recorded, not imputed.

### Feature distribution (treated only; predictor-side, never outcomes)

- F (inches): n=61, min **1.0**, p25 1.0, median 1.0, p75 2.0, max 5.0, mean 1.92.
- p06 descriptor (same ftime rows): min 31%, median 65%, max 100%. PoP is descriptive
  only — never part of the treatment rule, and must not become a rescue variable.
- Treated games by roof: all 61 `outdoors` (none of the 11 `open` retractable-roof games
  reached threshold — see §5 wrinkle).

## 2. Power / MDE (frozen constants — planning assumptions, never estimated from outcomes)

One-sided α=0.05, power=0.8 → z-sum **2.4865**. Primary: one-sample one-sided test of the
treated-unit residual mean vs 0. MDE_mean = 2.4865 × σ_res / √N.

| Scenario | N | MDE rush (σ=3.5) | MDE pass (σ=4.5) |
|---|---|---|---|
| G_precip (free count) | 61 | 1.11 attempts | 1.43 attempts |
| G_matched (pull-eligible) | 60 | 1.12 attempts | 1.45 attempts |
| Expected usable after 10–15% coverage/inactive attrition | 51–54 | 1.18–1.22 | 1.52–1.57 |

**Frozen sensitivity rule:** N < 25 → retire/park, no threshold weakening. N=60 clears the
floor with margin; the mechanism's central prediction (+1.5–2.0 rush attempts for the lead
back in heavy rain) clears the rush MDE of 1.12. Power is viable at every scenario.

## 3. Timestamp-safety evidence

- **Cycle rule is mechanical:** selected runtime = max 6h-grid runtime with
  `runtime + 4h ≤ T_dec` (= kickoff − 24h). The 4h dissemination lag is structural
  (spike-verified): the selected cycle was necessarily disseminated by T_dec, so the
  forecast is one a bettor could actually have seen at T_dec — **by construction**.
- **Realized weather permanently disqualified:** the schedule loader reads only
  `game_id/season/game_type/week/gameday/gametime/away_team/home_team/stadium/roof`
  (whitelist projection; score/outcome columns present in the source file but never
  accessed). MOS rows carry `runtime` (issue time) + `ftime` (valid time) — forecast
  semantics, never observations.
- **Prop quotes** (at pull time) come from snapshots ≤ T_dec via the `date` parameter —
  by construction. Final injury designations (~−48h) precede T_dec.

## 4. Minimum-cost pull proposal

| Item | Value |
|---|---|
| Endpoint | `GET /v4/historical/sports/americanfootball_nfl/events/{eventId}/odds` |
| Markets | `player_pass_attempts`, `player_rush_attempts` (both in **one** call per event) |
| Regions | `us` only |
| Timestamp | `date` = per-game T_dec floored to the 5-minute grid (column in table below) |
| Events | **60** treated + identity-matched games (table below) |
| **Exact credit ceiling** | **60 × 2 × 10 = 1,200 credits** |
| Expected actual | ≤ ceiling — empty-market responses cost nothing; at ~85–90% market presence ≈ 1,020–1,080 |
| Expected usable N | **51–54 units per endpoint** after ~10–15% coverage/inactive attrition |

**What the minimality rule cut:** controls not purchased (per spec §2.1 — buying all 359
would ~6× the cost for a parked sensitivity); treated-only funnel (no paid coverage
pre-screen exists, so coverage attrition is expected, not pre-cut); per-event calls with
both markets in one request; `regions=us` only. This is the smallest pull that answers
the preregistered question. Nothing in this document authorizes the spend — the exact
number returns for explicit user approval after the prereg freezes (gate order: counts →
feasibility → frozen preregs → identity → authorization).

**Event-ID multiplicity:** the frozen layer carries 1–2 re-issued ids per game here
(40 games × 1 id, 20 × 2 ids). Pull execution resolves the T_dec-active id per snapshot
via the identity layer — never assumes one id spans time (confirmed in the wild by the
2026-09-16 C/D probe).

### Exact event-ID list (60 events)

| nflverse game | matchup | T_dec (5-min, UTC) | F (in) | provider event id(s) |
|---|---|---|---|---|
| 2023_01_HOU_BAL | HOU@BAL wk1 | 2023-09-09T17:00:00 | 2.0 | 33368d4979d3475b77f5e9eb49648203 |
| 2023_01_ARI_WAS | ARI@WAS wk1 | 2023-09-09T17:00:00 | 2.0 | 175d9f07fc52d99b21bc772b1499c3d8 |
| 2023_01_PHI_NE | PHI@NE wk1 | 2023-09-09T20:25:00 | 1.0 | 0a368c64b212ecbad4ec46f6ff6c1af2 |
| 2023_02_KC_JAX | KC@JAX wk2 | 2023-09-16T17:00:00 | 1.0 | 450d0600bc30565494c6df8096e04976 |
| 2023_03_IND_BAL | IND@BAL wk3 | 2023-09-23T17:00:00 | 4.0 | 2c66bee99eec7cca649ecb6b249868fb |
| 2023_03_DEN_MIA | DEN@MIA wk3 | 2023-09-23T17:00:00 | 4.0 | 2ff30bbaf02729644dc5dd83dd02ab12 |
| 2023_03_NE_NYJ | NE@NYJ wk3 | 2023-09-23T17:00:00 | 5.0 | 66edacbc3505803f401192da0fb09b49 |
| 2023_03_BUF_WAS | BUF@WAS wk3 | 2023-09-23T17:00:00 | 3.0 | f049d09c797baccf11023b4cb7bc9b0b |
| 2023_03_CAR_SEA | CAR@SEA wk3 | 2023-09-23T20:05:00 | 1.0 | 4b3ed4ec08352199ff98bdf14b247c88 |
| 2023_03_PHI_TB | PHI@TB wk3 | 2023-09-24T23:15:00 | 2.0 | 1e006c91ca2e69c339ed517495a2f428 |
| 2023_05_NYG_MIA | NYG@MIA wk5 | 2023-10-07T17:00:00 | 1.0 | 6ed123d675ff7026b7b8ae34d3b4a59f, d72ca5073fe1d69e6cd3beb37e52c081 |
| 2023_06_SF_CLE | SF@CLE wk6 | 2023-10-14T17:00:00 | 1.0 | 417e02503976f3971e0750e60fff16e2, 640fd44761340126d8d482cd2a819c4a |
| 2023_06_NYG_BUF | NYG@BUF wk6 | 2023-10-15T00:20:00 | 1.0 | 68b7be06e7590bd403fd52db5c1d1c35, a9dd32ca4851d736514da497d2a24bbb |
| 2023_08_NYJ_NYG | NYJ@NYG wk8 | 2023-10-28T17:00:00 | 1.0 | 259e887fe43f70831d9ff5f0b13d20ea, b82f30e931a965bc7277d187549ca294 |
| 2023_08_JAX_PIT | JAX@PIT wk8 | 2023-10-28T17:00:00 | 2.0 | b1ac8bca9348e03321aa9bb276a6aadb, d00525c4b0701d8762321e851ea97063 |
| 2023_08_ATL_TEN | ATL@TEN wk8 | 2023-10-28T17:00:00 | 2.0 | 97819f71c474956d24e436f0465f1591, a13c16fa181d37e4603c793125a187fe |
| 2023_10_WAS_SEA | WAS@SEA wk10 | 2023-11-11T21:25:00 | 1.0 | e658ecc30213f84c4b68dc3cf5f3f44c, f7e8366f4bc62b8490b4324f8088bcab |
| 2023_11_PHI_KC | PHI@KC wk11 | 2023-11-20T01:15:00 | 1.0 | 34e87b63f58243a144a3c716fa79c61e, 7ada5a3ce1269f9c0953a196b5fb4a0c |
| 2023_12_PIT_CIN | PIT@CIN wk12 | 2023-11-25T18:00:00 | 2.0 | 4dc52e888473f9d445419321b93606f9, 4fc56a4d2185021682448f394e367742 |
| 2023_12_BUF_PHI | BUF@PHI wk12 | 2023-11-25T21:25:00 | 1.0 | 5e766a287ba24d40d9e40aa41efe19de, ed749f58474979a2601336af4596d47d |
| 2023_13_LAC_NE | LAC@NE wk13 | 2023-12-02T18:00:00 | 4.0 | ad1bbe3e94716ca03c3059092cbd1eee, aee7b722e1c8ea7e70d3dc4165b8d48e |
| 2023_13_ATL_NYJ | ATL@NYJ wk13 | 2023-12-02T18:00:00 | 1.0 | 8adf20ae245b218aa0f008be93442a19, d9498cb661062746dfc500a20c3a87e8 |
| 2023_13_ARI_PIT | ARI@PIT wk13 | 2023-12-02T18:00:00 | 2.0 | 76ae384526017414d68d617bf80b8aab, da4df5ecb534454b366e047b1909e1bb |
| 2023_13_CAR_TB | CAR@TB wk13 | 2023-12-02T21:05:00 | 1.0 | 43df28defee70f608e4cf44f51c3c351, b70cc20a6410503c2661e5074c3bedd8 |
| 2023_14_LA_BAL | LA@BAL wk14 | 2023-12-09T18:00:00 | 4.0 | 82734cac4ffb5c36e55d0b5dd6bc62f8, c4fa37180b109663749897f5e7ddb134 |
| 2023_14_HOU_NYJ | HOU@NYJ wk14 | 2023-12-09T18:00:00 | 5.0 | ad9641812be259565c9f27efacbd6078, affdea10708b9e802b8b0c445be6ce4b |
| 2023_15_ATL_CAR | ATL@CAR wk15 | 2023-12-16T18:00:00 | 4.0 | 89a76d31332effa87075701f164a8e0a |
| 2023_15_CHI_CLE | CHI@CLE wk15 | 2023-12-16T18:00:00 | 2.0 | abf4a23e7c6b9dd94d13a3070a32a65f |
| 2023_15_DAL_BUF | DAL@BUF wk15 | 2023-12-16T21:25:00 | 2.0 | 16807f51a2a89040e6573ff24bfcd9ab, b4a925302ef4769f63a996a6ba6548e0 |
| 2023_15_PHI_SEA | PHI@SEA wk15 | 2023-12-18T01:15:00 | 1.0 | 7856ae92af5a45990c0673041c46917b, f51da6d224ba8d719fe220e807cd0442 |
| 2023_16_CIN_PIT | CIN@PIT wk16 | 2023-12-22T21:30:00 | 1.0 | b99a12370b4b0a34cde90d66f19b4841, d86898556d2e1132370891f1fc23603f |
| 2023_18_PIT_BAL | PIT@BAL wk18 | 2024-01-05T21:30:00 | 4.0 | f1db1ea65e63375c8f6f9766dac5d4f7, f5160997517aa6f8f1876ee7e84f227e |
| 2023_18_NYJ_NE | NYJ@NE wk18 | 2024-01-06T18:00:00 | 1.0 | fb5a05459a7e5616f95e2caa7732c20f |
| 2023_18_PHI_NYG | PHI@NYG wk18 | 2024-01-06T21:25:00 | 1.0 | 66c7e39643ecae341addae9fff981d0b |
| 2024_01_WAS_TB | WAS@TB wk1 | 2024-09-07T20:25:00 | 4.0 | ba439e5505ce1ee745d2e48f2d2f31e6 |
| 2024_02_BUF_MIA | BUF@MIA wk2 | 2024-09-12T00:15:00 | 1.0 | ddd9b67cf1d68282e1e8652000c0d015 |
| 2024_02_CLE_JAX | CLE@JAX wk2 | 2024-09-14T17:00:00 | 1.0 | 111ac41e21c6f16a2d3d1511f07e2004 |
| 2024_03_JAX_BUF | JAX@BUF wk3 | 2024-09-22T23:30:00 | 1.0 | 96be3ba73fc562e49a0120b5923c3213 |
| 2024_04_DAL_NYG | DAL@NYG wk4 | 2024-09-26T00:15:00 | 1.0 | b08196b0745d9e2e0bebfe8627fc5a1f |
| 2024_04_BUF_BAL | BUF@BAL wk4 | 2024-09-29T00:20:00 | 1.0 | ce925c8cb892e1806399345e7885828d |
| 2024_05_IND_JAX | IND@JAX wk5 | 2024-10-05T17:00:00 | 4.0 | dba45197b1cd2dbafe7f83ebffd2be90 |
| 2024_05_DAL_PIT | DAL@PIT wk5 | 2024-10-06T00:20:00 | 2.0 | 504a7b09264059083346ab086141da30 |
| 2024_06_ARI_GB | ARI@GB wk6 | 2024-10-12T17:00:00 | 2.0 | fb08594af963cff8fae2f93b2e0f4269 |
| 2024_08_BUF_SEA | BUF@SEA wk8 | 2024-10-26T20:05:00 | 1.0 | 9a263061dd1c784a20f393971744d176 |
| 2024_09_DET_GB | DET@GB wk9 | 2024-11-02T21:25:00 | 3.0 | 84d2187ad0c2a42b32879d3bcbaee8dd |
| 2024_09_TB_KC | TB@KC wk9 | 2024-11-04T01:15:00 | 2.0 | 8f751d18969af50ec341e0b282e5b4d6 |
| 2024_10_MIN_JAX | MIN@JAX wk10 | 2024-11-09T18:00:00 | 1.0 | a59741270a5f21dfb288838acf98537b |
| 2024_10_PIT_WAS | PIT@WAS wk10 | 2024-11-09T18:00:00 | 1.0 | 928f8f873c2bf276d0b2a63a8f8941b4 |
| 2024_12_ARI_SEA | ARI@SEA wk12 | 2024-11-23T21:25:00 | 1.0 | d9a2e1ead2367a3b0f46d247ed5b2906 |
| 2024_15_LA_SF | LA@SF wk15 | 2024-12-12T01:15:00 | 1.0 | b89ffb3a984d693fd6e69cdc3f2d585a |
| 2024_15_KC_CLE | KC@CLE wk15 | 2024-12-14T18:00:00 | 2.0 | 9db2aef33225a4bda04bbc7c33e50e63 |
| 2024_15_PIT_PHI | PIT@PHI wk15 | 2024-12-14T21:25:00 | 1.0 | 9cec3324e93aa4ece67f4ed4447bc212 |
| 2024_16_MIN_SEA | MIN@SEA wk16 | 2024-12-21T21:05:00 | 2.0 | 87ed34522672eabe6cb5b85c34c4e8ce |
| 2024_17_SEA_CHI | SEA@CHI wk17 | 2024-12-26T01:15:00 | 1.0 | 1cc92f3601760bad475efd70d52ca482 |
| 2024_17_DEN_CIN | DEN@CIN wk17 | 2024-12-27T21:30:00 | 3.0 | aa15f18a1c3124af14045b32185ff166 |
| 2024_17_NYJ_BUF | NYJ@BUF wk17 | 2024-12-28T18:00:00 | 1.0 | eb5a71180b3558fdc20a896e98c00385 |
| 2024_17_TEN_JAX | TEN@JAX wk17 | 2024-12-28T18:00:00 | 1.0 | 68e8e424d470fb6d60f1b2f0a3f3d1e3 |
| 2024_17_CAR_TB | CAR@TB wk17 | 2024-12-28T18:00:00 | 1.0 | cb4060acc9d027ca0069619ef9983415 |
| 2024_17_MIA_CLE | MIA@CLE wk17 | 2024-12-28T21:05:00 | 1.0 | 7c63b1e23a0c65ee07312131e1b7be7e |
| 2024_17_ATL_WAS | ATL@WAS wk17 | 2024-12-29T01:20:00 | 4.0 | 5f99b5d40cf65d48be81c3812cd1e95e |

## 4a. Manifest correction addendum (2026-09-16, pre-freeze)

Final free validation against the frozen canonical-identity artifact
(run 35041420559, `canonical-identity-nfl`) found **2 transcription errors**
in the §4 table above (verified: all other 58 games' id sets match the
artifact exactly; all ids 32-hex; T_dec = floor5(kickoff−24h) verified
against live nflverse schedules for all 60; 2024_18_HOU_TEN correctly
absent from the artifact):

- `2023_15_PHI_SEA`: `f51da6d224ba8e807cd0442` (23 chars, truncated) →
  `f51da6d224ba8d719fe220e807cd0442`
- `2023_18_PIT_BAL`: `f5160997517aa4f8f1876ee7e84f227e` →
  `f5160997517aa6f8f1876ee7e84f227e` (one hex digit)

Both rows above are corrected. The pull manifest
(`nfl-edge/ops/nfl_hd_pull_manifest.json`) carries the corrected ids,
ordered by first-seen odds_date; pull execution tries candidate ids in
order and uses the first returning non-empty bookmakers at T_dec
(empty responses are free). This is a mechanical data correction against
the authoritative identity layer — not a specification change.

## 5. Findings the prereg must resolve (documented, not altered)

These do not change the feasibility verdict, but the frozen prereg DRAFT must pin them
before any pull:

1. **q06 valid-period convention.** Adopted: q06 at ftime F is the 6h accumulation valid
   for **[F−6h, F]** (the NWS MOS ending convention; PoP shares the same period
   structure). Sensitivity check on the 61 treated games under the forward convention
   ([F, F+6h]): 48 stay treated, 13 flip to control. Both conventions give N (61 / 48)
   far above the 25 floor with viable power (MDE_rush 1.12 / 1.26), so the verdict is
   robust — but the prereg must cite the meteorological source and pin the convention.
2. **q06 quantization.** Every non-null q06 value in the archive interface is
   integer-valued (verified across 12 random cycles, 132 values, 0 fractional). The
   frozen rule F ≥ 0.25" is applied exactly as written, but the effective treatment
   margin in practice is F ≥ 1.0" — no game fell in [0.25, 1.0). The treated set is
   therefore genuinely heavy-rain forecasts (median PoP 65%), which strengthens the
   mechanism test; it also means the 0.25" threshold never binds at the margin.
3. **Retractable-roof games.** The verbatim nflverse roof rule includes 11 games at
   retractable venues marked `open` (AT&T 1, Lucas Oil 3, State Farm 2, Mercedes-Benz
   Stadium 3, NRG 2) — gameday roof position is decided ~90 min before kickoff, i.e.
   **after** T_dec. None of the 11 reached treatment, so the treated set is unaffected;
   the prereg should still decide structurally (exclude retractable venues vs. accept
   the verbatim rule) rather than discover it post-pull.
4. **SoFi canopy.** nflverse marks all 34 SoFi games `dome` → excluded wholesale by the
   verbatim rule; no canopy judgment was needed in practice.

## 6. Coverage planning assumption (stated, not pre-cut)

The 2026-09-16 C/D probe executed per-event historical pulls at kickoff−24h:
`player_pass_attempts` ✓ and `player_rush_attempts` ✓ both returned multi-book US depth
in the 2023 probes. Coverage at T_dec is therefore a **verified planning assumption
(~85–90%)**, not a guess — but the pull itself is the exact coverage screen, since empty
responses cost nothing and no paid pre-screen exists. Expected usable N: 51–54 units per
endpoint.

## 7. GO / NO-GO

**CONDITIONAL GO.** The free MOS count lands (G_precip = 61 ≥ 25 floor), the identity join
keeps 60 pull-eligible events, and power is viable at every scenario (MDE_rush 1.12 at
N=60; 1.18–1.22 at expected usable N=51–54 — the mechanism's central prediction of
+1.5–2.0 rush attempts clears it). The exact pull price is **1,200 credits (ceiling)**.
No rescue, no relaxed thresholds, no PoP, no pooled weather effects were used or needed.

**Remaining gates before any spend** (per the user's credit directive, in order):
frozen prereg DRAFT adopting §2.0 verbatim **plus** resolutions of §5 items 1–4 →
identity (done here: 60 events, exact ids above) → **explicit user authorization of the
1,200-credit pull**. This document authorizes nothing.

---
*Method: `nfl-edge/ops/nfl_h_d_precip_feasibility.py` (359 free IEM calls, 1.2s pacing;
outcome columns whitelisted out; identity from canonical-identity run 35041420559
artifact). Zero Odds API credits spent. Zero outcomes inspected. No prereg frozen.*
