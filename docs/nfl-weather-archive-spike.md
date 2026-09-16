# NFL Weather Archive Spike — can we reconstruct pre-kickoff forecast information point-in-time?

**Date:** 2026-09-15 | **Status:** spike complete | **API cost:** $0 (3 free HTTP calls)

## Verdict: AVAILABLE

Point-in-time forecast information for 2022–2024 NFL games **can** be reconstructed from a free,
timestamped forecast archive. Primary source: the **Iowa Environmental Mesonet (IEM) Model Output
Statistics (MOS) archive**, proven with a real retrieval below. This is *forecast* data with explicit
issue-time vs valid-time semantics — not observations, not reanalysis.

## Proven source: IEM MOS archive

- **What it is:** NWS Model Output Statistics — site-specific statistical forecast guidance derived
  from operational NWP models (GFS, NAM, NBS/NBE, LAMP). Issued at fixed model runtimes with
  3-hourly projections. The IEM archives every runtime and serves it back with both timestamps.
- **Coverage (verified from IEM archive status page):**
  - GFS MOS: 16 Dec 2003 → realtime (covers all of 2022–2024)
  - NAM MOS: 9 Dec 2008 → realtime
  - NBS / NBE: Nov 2018 / Jul 2020 → realtime
- **Access:** free, no key. Two interfaces:
  - Single-run JSON: `GET https://mesonet.agron.iastate.edu/api/1/mos.json?station=KBUF&model=GFS&runtime=2023-10-01T12:00:00Z`
  - Bulk ranges: `https://mesonet.agron.iastate.edu/cgi-bin/request/mos.py` (scriptable CGI service,
    per IEM API docs)
- **Variables relevant to football:** `tmp` (°F), `dpt`, `wdr` (wind direction, degrees), `wsp`
  (sustained wind speed, **knots**, 3-hourly), `p06`/`p12` (PoP %), `q06`/`q12` (QPF), `sky`, `cig`, `vis`.
- **Point-in-time semantics:** response rows carry `runtime` (model issue time) and `ftime` (valid
  time). A pre-kickoff information set = rows with `runtime` + dissemination lag **strictly before**
  kickoff. GFS MOS for a 12Z cycle is typically disseminated within ~2–4h of cycle time; for a 1pm ET
  (17Z) kickoff, use the 06Z cycle or earlier to be safe, and record the exact `runtime` used.

## One-game proof (real retrieval, 2026-09-15)

Game: Dolphins @ Bills, **2023-10-01**, Highmark Stadium (Orchard Park, NY), kickoff **1:00 PM ET
= 17:00 UTC**. Nearest MOS site: **KBUF** (Buffalo Niagara Intl, ~15 km from stadium).

Call:
```
GET https://mesonet.agron.iastate.edu/api/1/mos.json?station=KBUF&model=GFS&runtime=2023-10-01T12:00:00Z
→ 200, 21 rows, runtime=2023-10-01 12:00 for all rows
```

Forecast actually available before kickoff (12Z cycle, issued hours before 17:00Z kickoff):

| valid (ftime, UTC) | lead vs kickoff | temp (°F) | wind | PoP |
|---|---|---|---|---|
| 2023-10-01 18:00 | +1h (nearest 3h projection) | 78 | 260° at 6 kt | — |
| 2023-10-01 21:00 | +4h | 78 | 260° at 6 kt | — |
| 2023-10-02 00:00 | +7h | 70 | 220° at 4 kt | 3% |

The 12Z GFS MOS said: ~78°F, light westerly wind ~6 kt (≈7 mph) around kickoff — the forecast
information a bettor could have seen pre-kickoff, with a known issue timestamp. No realized
weather was touched at any step.

## Secondary source (verified present, value extraction not executed): HRRR gridded archive

- **AWS Open Data:** `s3://noaa-hrrr-bdp-pds` (GRIB2, archive begins **30 Sep 2014**; hourly 3-km
  CONUS cycles) and `s3://hrrrzarr` (University of Utah Zarr re-chunking of the same archive,
  surface fields in 150×150-point chunks). Both public, anonymous access, no key.
- Verified 2026-09-15: `s3://hrrrzarr/sfc/20231001/20231001_12z_anl.zarr/` exists (forecast
  zarrs follow the same naming with `_fcst`); GRIB2 per-cycle `.idx` files exist for byte-range
  subsetting. Registry: https://registry.opendata.aws/noaa-hrrr-pds/
- Use case: hourly gridded wind/temperature at exact stadium lat/lon (better temporal resolution
  than MOS 3-hourly). Cost: reading one chunk per (cycle, variable) ≈ KBs–MBs; no egress charge
  concern at this volume. Caveat: point extraction requires HRRR Lambert-conformal grid mapping
  (documented parameters: ref lat 38.5, ref lon −97.5, 1799×1059, 3 km) — implement once, reuse.
- Status: **not needed to answer the spike** (MOS proof suffices); keep as the upgrade path if
  hourly resolution or off-airport locations become necessary.

## What explicitly does NOT qualify (trap list)

- nflverse `schedules` `temp`/`wind` columns — **realized** game-time weather (already flagged in
  the research-lane inventory; stays closed).
- ERA5 or any reanalysis — analysis of what happened, not a forecast.
- api.weather.gov / OpenWeather current endpoints — current-only, no historical issue times.
- Any observation (METAR/ASOS) used as a "forecast" — observations are fine for *verifying*
  forecasts, never as the pre-kickoff information set.

## Scale-up path (not executed in this spike)

1. Build the static 32-stadium → nearest-MOS-site map (nflverse `load_stadiums()` has stadium
   lat/lon; IEM MOS sites are the ~1,700 NWS MOS stations, i.e. major airports — nearest-airport
   join, recorded explicitly).
2. Retrieval script: for each game, choose the latest GFS (or NAM) MOS `runtime` with
   `runtime + 4h < kickoff`; pull rows; take the two `ftime` projections bracketing kickoff.
   ~272 games/season × 1 HTTP call ≈ trivial load on a free API; add polite pacing.
3. Mark dome/retractable-roof games in the identity layer (weather-irrelevant) rather than
   feeding them noise.
4. Validation: compare MOS wind/temp against METAR observations at the same site to quantify
   forecast error — keeps the information set honest.

## Cost

$0. No paid services, no API keys, no Odds API credits. Three anonymous HTTP GETs used for
verification (IEM latest, IEM historical, S3 listings).

## Bottom line

Weather historical research is **available**, not forward-only. The IEM MOS archive gives free,
timestamped, pre-kickoff forecast information (temp, wind speed/direction, PoP) for every
2022–2024 NFL game via a stable JSON API, proven above. The H-N2 hypothesis sketch (forecast
wind vs totals in outdoor stadiums) is **unblocked** at the data layer; it still carries LOW
prior and needs preregistration before any test.
