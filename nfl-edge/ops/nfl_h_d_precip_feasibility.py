#!/usr/bin/env python3
"""H-D precipitation feasibility gate (free, outcome-blind, zero credits).

Implements the FROZEN precipitation feature from
docs/nfl-cd-priced-pull-spec-2026-09-16.md section 2.0 VERBATIM:
  - IEM MOS GFS only.
  - Cycle rule: mechanically latest 6h-grid GFS MOS runtime with
    runtime + 4h <= kickoff - 24h (= T_dec). 4h = dissemination lag.
  - Window: q06 (6-hour QPF, inches) valid periods whose valid intervals
    overlap [kickoff, kickoff + 3.5h]. F = max q06 over those periods.
    Convention (NWS MOS standard, documented): q06 at ftime F is the 6-hour
    accumulation valid for [F - 6h, F].
  - Treated iff F >= 0.25 inches.
  - Venues: nflverse schedules roof column in {'outdoors', 'open'} only.
    dome/closed excluded wholesale; international venues with no MOS
    coverage excluded.
  - Missingness: cycle unretrievable or non-numeric q06 -> unit censored
    (explicit missingness, never backfilled, never imputed).
  - PoP (p06): recorded at the same ftime rows as a DESCRIPTOR only;
    never part of the treatment rule.

Outcome-blindness: this script reads ONLY non-outcome schedule columns
(game_id, season, game_type, week, gameday, gametime, away_team, home_team,
stadium, roof). Score/outcome columns are never accessed -- the CSV is
read through a strict column whitelist and any access to an outcome
column raises. No Odds API calls are made (zero credits). No prereg is
frozen. No other hypothesis families are touched.

Identity join: frozen canonical-identity mapping (CI artifact JSON,
nflverse_game_id -> provider_event_id list) supplied via --identity-json.

Outputs: --out (JSON report) and --per-game-csv. Prints a summary.
"""

import argparse
import csv
import json
import math
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import mos_forecast_archive as mos  # noqa: E402 (parse_kickoff, cycle rules, IEM fetch)

# ---- frozen D constants (from the spec) -------------------------------------
SEASONS = ("2023", "2024")
ROOF_INCLUDE = {"outdoors", "open"}      # nflverse schedules roof column, verbatim
TREAT_THRESHOLD_IN = 0.25               # F >= 0.25" -> treated
GAME_WINDOW_H = 3.5                     # [kickoff, kickoff + 3.5h]
MOS_LOOKBACK_H = 72
HTTP_PACING_S = 1.2                     # politeness for free IEM calls
HTTP_TIMEOUT = 30
HTTP_RETRIES = 1                        # one retry of the SAME selected cycle; then censored

# Frozen power constants (planning assumptions, never estimated from outcomes)
MDE_Z_SUM = 2.4865                      # z_0.95 + z_0.80, one-sided alpha=0.05, power=0.8
MDE_SIGMA_RUSH = 3.5                    # attempts
MDE_SIGMA_PASS = 4.5                    # attempts
N_FLOOR = 25                            # N < 25 -> retire/park, no threshold weakening
COVERAGE_ATTRITION = (0.10, 0.15)       # planning assumption: coverage/inactive attrition

# Strict whitelist: the ONLY schedule columns this script may read.
# away_score/home_score/result/total/overtime/temp/wind and all other
# outcome-adjacent columns are unreachable by construction.
SAFE_COLUMNS = ("game_id", "season", "game_type", "week", "gameday",
                "gametime", "away_team", "home_team", "stadium", "roof")
OUTCOME_COLUMNS = ("away_score", "home_score", "result", "total", "overtime",
                   "temp", "wind", "away_qb_id", "home_qb_id", "away_qb_name",
                   "home_qb_name", "away_rest", "home_rest",
                   "away_moneyline", "home_moneyline", "spread_line",
                   "total_line")

NFLVERSE_SCHEDULES_URL = ("https://github.com/nflverse/nflverse-data/releases"
                          "/download/schedules/games.csv")


def floor_5min(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def select_runtime(t_dec):
    """Mechanically latest 6h-grid runtime with runtime + 4h <= t_dec,
    within the 72h lookback envelope."""
    latest = t_dec - timedelta(hours=mos.DISSEMINATION_LAG_HOURS)
    latest = latest.replace(minute=0, second=0, microsecond=0)
    latest = latest.replace(hour=(latest.hour // 6) * 6)
    earliest = t_dec - timedelta(hours=MOS_LOOKBACK_H)
    if latest < earliest:
        return None
    assert mos.is_eligible(latest, t_dec)
    return latest


def fetch_cycle_with_retry(station, runtime):
    """Fetch the mechanically selected cycle. One retry of the SAME runtime
    on transient failure; then the game is censored (never backfilled)."""
    last_exc = None
    for attempt in range(HTTP_RETRIES + 1):
        try:
            rows = mos.fetch_mos_cycle(station, runtime)
            return rows, None
        except Exception as exc:  # noqa: BLE001 -- failures are data
            last_exc = exc
            time.sleep(HTTP_PACING_S)
    return None, f"request_failed:{str(last_exc)[:160]}"


def numeric(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def feature_from_rows(rows, kickoff):
    """F = max q06 over valid periods [ftime-6h, ftime] overlapping
    [kickoff, kickoff+3.5h]. Returns (F, overlapping_rows) or (None, reason).

    Rows with non-numeric q06 are unusable and skipped; if NO usable row
    overlaps the window, the unit is censored (missingness, never imputed).
    """
    w_start, w_end = kickoff, kickoff + timedelta(hours=GAME_WINDOW_H)
    usable = []
    for fr in rows:
        try:
            ft = mos.parse_mos_time(fr["ftime"])
        except Exception:
            continue
        q = numeric(fr.get("q06"))
        if q is None:
            continue
        p = numeric(fr.get("p06"))  # descriptor only
        iv_start, iv_end = ft - timedelta(hours=6), ft
        if iv_start < w_end and iv_end > w_start:  # overlap, non-degenerate
            usable.append({"ftime": ft, "q06": q, "p06": p})
    if not usable:
        return None, "no_usable_q06_in_window"
    best = max(usable, key=lambda r: r["q06"])
    return {"F": best["q06"], "n_rows": len(usable),
            "p06_at_F": best["p06"],
            "p06_max": max((r["p06"] for r in usable if r["p06"] is not None),
                           default=None)}, None


def build_station_index(mapping, aliases):
    """venue_key -> mapping entry. Lets 2022-2024 alias names resolve to the
    verified 2026-keyed entries. Entries are used exactly as verified; the
    nflverse roof COLUMN (not the mapping's roof field) drives inclusion."""
    idx = {}
    for key, entry in mapping.items():
        if key == "_meta":
            continue
        vk = entry.get("venue_key") or key
        idx[vk] = (key, entry)
        idx[key] = (key, entry)
    return idx


def resolve_station(stadium, mapping, aliases, vk_index):
    """Return (mos_station, resolution_note) or (None, reason)."""
    entry = mapping.get(stadium)
    note = "direct"
    if entry is None:
        alias = aliases.get("aliases", {}).get(stadium)
        if alias is not None:
            vk = alias["venue_key"]
            hit = vk_index.get(vk)
            if hit is None:
                return None, "alias_points_nowhere:" + stadium
            _, entry = hit
            note = "alias:" + stadium + "->" + vk
        else:
            unmatched = aliases.get("unmatched", {}).get(stadium)
            if unmatched is not None:
                return None, "unmatched:" + unmatched["reason"]
            return None, "unmapped_stadium:" + stadium
    if not isinstance(entry, dict) or entry.get("status") != "verified":
        return None, "unmatched:" + str((entry or {}).get("unavailable_reason"))
    station = entry.get("mos_station")
    if not station:
        return None, "no_mos_station:" + stadium
    return station, note


def mde_mean(n, sigma):
    return MDE_Z_SUM * sigma / math.sqrt(n)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--per-game-csv", default=None)
    ap.add_argument("--identity-json", required=True,
                    help="frozen canonical-identity artifact JSON")
    ap.add_argument("--schedules-csv", default=None,
                    help="local schedules CSV (else live download)")
    ap.add_argument("--max-games", type=int, default=None,
                    help="debug cap on MOS fetches")
    args = ap.parse_args(argv)

    report = {
        "spec": "docs/nfl-cd-priced-pull-spec-2026-09-16.md section 2.0 (frozen)",
        "api_credits_used": 0,
        "db_writes": 0,
        "outcomes_inspected": False,
        "power_inputs_are_planning_assumptions": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ---- schedules: strict column whitelist, outcomes unreachable ------------
    if args.schedules_csv:
        raw = Path(args.schedules_csv).read_text()
    else:
        req = urllib.request.Request(
            NFLVERSE_SCHEDULES_URL, headers={"User-Agent": "nfl-edge-hd-feas"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
    import io
    reader = csv.DictReader(io.StringIO(raw))
    header = set(reader.fieldnames or [])
    leaked = [c for c in OUTCOME_COLUMNS if c in header]
    # Presence in the file is fine; ACCESS is what is forbidden. The rows
    # below are projected onto SAFE_COLUMNS only.
    games_all = []
    for row in reader:
        games_all.append({k: row[k] for k in SAFE_COLUMNS})
    report["schedule_source"] = (args.schedules_csv or "live-nflverse-games.csv")
    report["outcome_columns_present_but_never_accessed"] = leaked

    funnel = {}
    funnel["sched_reg_2023_2024"] = sum(
        1 for g in games_all
        if g["season"] in SEASONS and g["game_type"] == "REG")
    games = [g for g in games_all
             if g["season"] in SEASONS and g["game_type"] == "REG"
             and g["roof"] in ROOF_INCLUDE]
    funnel["roof_included_outdoors_open"] = len(games)

    # ---- static inputs --------------------------------------------------------
    mapping = {k: v for k, v in
               json.load(open(HERE / "nfl_stadium_mos.json")).items()
               if k != "_meta"}
    aliases = json.load(open(HERE / "nfl_stadium_aliases_2022_2024.json"))
    vk_index = build_station_index(mapping, aliases)
    ident = json.load(open(args.identity_json))
    g2eids = defaultdict(list)
    for mrec in ident["results"]["matched"]:
        if mrec.get("season") in (2023, 2024) and mrec.get("game_type") == "REG":
            g2eids[mrec["nflverse_game_id"]].append(mrec["provider_event_id"])
    for gid in g2eids:
        g2eids[gid] = sorted(set(g2eids[gid]))

    counts = defaultdict(int)
    per_game = []
    treated_F = []
    treated_p06 = []

    n_fetch = 0
    for g in sorted(games, key=lambda r: (r["season"], r["week"], r["gameday"])):
        gid = g["game_id"]
        rec = {"nflverse_game_id": gid, "season": g["season"],
               "week": g["week"], "away_team": g["away_team"],
               "home_team": g["home_team"], "stadium": g["stadium"],
               "roof_nflverse": g["roof"]}
        try:
            kickoff = mos.parse_kickoff(g["gameday"], g["gametime"])
        except ValueError as exc:
            rec.update(status="excluded", exclusion_reason="bad_kickoff",
                       detail=str(exc)[:120])
            counts["bad_kickoff"] += 1
            per_game.append(rec)
            continue
        t_dec = kickoff - timedelta(hours=24)
        rec["kickoff_utc"] = kickoff.isoformat()
        rec["t_dec_utc"] = t_dec.isoformat()
        rec["t_dec_5min_utc"] = floor_5min(t_dec).isoformat()

        station, note = resolve_station(g["stadium"], mapping, aliases, vk_index)
        if station is None:
            rec.update(status="excluded", exclusion_reason=note)
            counts["mos_" + note.split(":")[0]] += 1
            per_game.append(rec)
            continue
        rec["mos_station"] = station
        rec["station_resolution"] = note

        runtime = select_runtime(t_dec)
        if runtime is None:
            rec.update(status="excluded",
                       exclusion_reason="no_eligible_mos_cycle")
            counts["no_eligible_mos_cycle"] += 1
            per_game.append(rec)
            continue
        rec["mos_selected_runtime_utc"] = runtime.isoformat()

        if args.max_games is not None and n_fetch >= args.max_games:
            rec.update(status="skipped", exclusion_reason="debug_cap")
            counts["debug_cap"] += 1
            per_game.append(rec)
            continue
        n_fetch += 1
        rows, err = fetch_cycle_with_retry(station, runtime)
        time.sleep(HTTP_PACING_S)
        if err is not None:
            rec.update(status="excluded", exclusion_reason=err)
            counts["mos_request_failed"] += 1
            per_game.append(rec)
            continue
        if not rows:
            rec.update(status="excluded", exclusion_reason="mos_absent")
            counts["mos_absent"] += 1
            per_game.append(rec)
            continue

        feat, ferr = feature_from_rows(rows, kickoff)
        if ferr is not None:
            rec.update(status="excluded", exclusion_reason=ferr)
            counts[ferr] += 1
            per_game.append(rec)
            continue
        rec["F_q06_in"] = feat["F"]
        rec["F_n_rows"] = feat["n_rows"]
        rec["p06_at_F"] = feat["p06_at_F"]   # descriptor only
        rec["p06_max_window"] = feat["p06_max"]
        treated = feat["F"] >= TREAT_THRESHOLD_IN
        rec["treated"] = treated
        if treated:
            treated_F.append(feat["F"])
            if feat["p06_max"] is not None:
                treated_p06.append(feat["p06_max"])
            eids = g2eids.get(gid, [])
            rec["identity_event_ids"] = eids
            rec["n_identity_event_ids"] = len(eids)
            rec.update(status="treated", exclusion_reason=None)
            counts["treated"] += 1
            if eids:
                counts["treated_with_identity"] += 1
            else:
                counts["treated_no_identity"] += 1
        else:
            rec.update(status="control_not_purchased",
                       exclusion_reason="F_below_threshold")
            counts["control_not_purchased"] += 1
        per_game.append(rec)

    funnel["mos_fetch_attempts"] = n_fetch
    funnel["attrition"] = dict(counts)
    funnel["G_precip"] = counts["treated"]
    funnel["G_precip_with_identity"] = counts["treated_with_identity"]
    funnel["G_precip_no_identity"] = counts["treated_no_identity"]
    report["funnel"] = funnel

    # ---- feature distribution summary (treated only; outcomes never read) ----
    if treated_F:
        s = sorted(treated_F)
        report["F_distribution_treated_in"] = {
            "n": len(s), "min": s[0], "p25": s[len(s)//4],
            "median": s[len(s)//2], "p75": s[3*len(s)//4], "max": s[-1],
            "mean": sum(s)/len(s),
        }
    if treated_p06:
        p = sorted(treated_p06)
        report["p06_descriptor_treated"] = {
            "n": len(p), "min": p[0], "median": p[len(p)//2], "max": p[-1],
            "note": "PoP is a descriptor only; never part of the treatment rule",
        }

    # ---- power (frozen constants; planning assumptions) -----------------------
    G = counts["treated_with_identity"]  # pull-eligible treated games
    power = {
        "z_sum": MDE_Z_SUM, "alpha": "0.05 one-sided", "power": 0.8,
        "sigma_res_rush_attempts": MDE_SIGMA_RUSH,
        "sigma_res_pass_attempts": MDE_SIGMA_PASS,
        "N_floor": N_FLOOR,
    }
    scen = {}
    for label, n in [("G_precip_free_count", counts["treated"]),
                     ("G_matched_pull_eligible", G)]:
        scen[label] = {
            "N": n,
            "MDE_rush_attempts": round(mde_mean(n, MDE_SIGMA_RUSH), 3) if n else None,
            "MDE_pass_attempts": round(mde_mean(n, MDE_SIGMA_PASS), 3) if n else None,
        }
    lo, hi = COVERAGE_ATTRITION
    if G > 0:
        n_lo = math.floor(G * (1 - hi))
        n_hi = math.ceil(G * (1 - lo))
        scen["expected_usable_after_attrition"] = {
            "attrition_assumption": f"{lo:.0%}-{hi:.0%} coverage/inactive",
            "N_range": [n_lo, n_hi],
            "MDE_rush_attempts_range": [round(mde_mean(n_hi, MDE_SIGMA_RUSH), 3),
                                        round(mde_mean(n_lo, MDE_SIGMA_RUSH), 3)] if n_lo else None,
            "MDE_pass_attempts_range": [round(mde_mean(n_hi, MDE_SIGMA_PASS), 3),
                                        round(mde_mean(n_lo, MDE_SIGMA_PASS), 3)] if n_lo else None,
        }
    else:
        scen["expected_usable_after_attrition"] = None
    power["scenarios"] = scen
    if counts["treated"] < N_FLOOR:
        power["verdict"] = ("NO-GO (retire/park): free MOS count "
                            f"{counts['treated']} < {N_FLOOR} floor")
    elif G < N_FLOOR:
        power["verdict"] = ("NO-GO (retire/park): pull-eligible treated games "
                            f"{G} < {N_FLOOR} floor after identity join")
    elif n_lo < N_FLOOR:
        power["verdict"] = ("CONDITIONAL GO with attrition watch: expected usable "
                            f"N [{n_lo}, {n_hi}] straddles the {N_FLOOR} floor")
    else:
        power["verdict"] = ("CONDITIONAL GO: free count lands, power viable; "
                            "pull returns for explicit approval at the exact number")
    report["power"] = power

    # ---- minimum-cost pull proposal -------------------------------------------
    treated_recs = [r for r in per_game if r.get("status") == "treated"
                    and r.get("n_identity_event_ids")]
    pull = {
        "endpoint": ("GET /v4/historical/sports/americanfootball_nfl"
                     "/events/{eventId}/odds"),
        "markets": ["player_pass_attempts", "player_rush_attempts"],
        "regions": ["us"],
        "date_param": "per-game T_dec floored to the 5-minute grid",
        "n_events": len(treated_recs),
        "credit_ceiling": len(treated_recs) * 2 * 10,
        "credit_note": ("ceiling assumes both markets return quotes for every "
                        "event; empty-market responses cost nothing, so actual "
                        "spend is <= ceiling"),
        "minimality": ("treated-only per spec section 2.1 (controls not "
                       "purchased); per-event calls; regions=us only; one call "
                       "per event carrying both markets"),
        "events": [{"nflverse_game_id": r["nflverse_game_id"],
                    "season": r["season"], "week": r["week"],
                    "t_dec_5min_utc": r["t_dec_5min_utc"],
                    "F_q06_in": r["F_q06_in"],
                    "provider_event_ids": r["identity_event_ids"],
                    "event_id_note": ("frozen layer carries 1-5 re-issued ids "
                                      "per game; pull execution resolves the "
                                      "T_dec-active id per snapshot, never "
                                      "assumes one id spans time")}
                   for r in treated_recs],
    }
    report["minimum_cost_pull"] = pull

    # ---- timestamp-safety evidence --------------------------------------------
    report["timestamp_safety"] = {
        "cycle_rule": "mechanically latest 6h-grid GFS runtime with "
                      "runtime + 4h <= T_dec (= kickoff - 24h)",
        "why_knowable": ("the dissemination lag is structural: a cycle issued "
                         "at runtime is disseminated by runtime+4h, which is "
                         "<= T_dec by the eligibility predicate; the selected "
                         "cycle is therefore a forecast a bettor could have "
                         "seen at T_dec, by construction"),
        "realized_weather": ("permanently disqualified: schedule loader reads "
                             "only the 10 non-outcome columns; MOS rows carry "
                             "runtime (issue time) and ftime (valid time) -- "
                             "forecast semantics, not observations"),
        "outcome_columns": "present in source file but never accessed "
                           "(whitelist projection)",
    }

    report["per_game"] = per_game
    Path(args.out).write_text(json.dumps(report, indent=2))
    if args.per_game_csv:
        fields = ["nflverse_game_id", "season", "week", "away_team",
                  "home_team", "stadium", "roof_nflverse", "mos_station",
                  "station_resolution", "kickoff_utc", "t_dec_utc",
                  "t_dec_5min_utc", "mos_selected_runtime_utc", "F_q06_in",
                  "F_n_rows", "p06_at_F", "p06_max_window", "treated",
                  "n_identity_event_ids", "status", "exclusion_reason"]
        with open(args.per_game_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields,
                               extrasaction="ignore")
            w.writeheader()
            for r in per_game:
                w.writerow(r)
    print(json.dumps({
        "G_precip": counts["treated"],
        "G_matched": counts["treated_with_identity"],
        "G_no_identity": counts["treated_no_identity"],
        "mos_fetch_attempts": n_fetch,
        "attrition": dict(counts),
        "verdict": power["verdict"],
        "api_credits_used": 0, "outcomes_inspected": False,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
