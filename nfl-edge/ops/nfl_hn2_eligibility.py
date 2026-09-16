#!/usr/bin/env python3
"""H-N2 eligible-N gate (DRAFT prereg
`docs/preregistrations/nfl-h-n2-forecast-wind-totals-DRAFT.md`).

Computes the exact eligible sample size for the H-N2 preregistration
(pre-kickoff forecast wind -> NFL totals) MECHANICALLY, with zero outcome
inspection and zero Odds API credits. READ-ONLY on the database (SELECTs
only); free IEM HTTP for archived MOS forecast availability; free nflverse
HTTP for schedule kickoff times (gameday/gametime/stadium columns only --
scores never read).

Eligibility funnel (each step is a hard filter; attrition reported):
  1. NFL regular season 2022-2024 game in the bundled nflverse schedules.
  2. Canonical identity match -> >=1 Odds event id (frozen dataset).
  3. Stadium resolves to a venue in the stadium->MOS mapping
     (nfl_stadium_mos.json + nfl_stadium_aliases_2022_2024.json).
  4. Venue classification INCLUDE: roof in {outdoor, canopy_open_air}.
     Domes and retractable-roof venues are excluded BY STRUCTURAL RULE
     (never by gameday roof position, which is post-cutoff information).
     Unmatched (non-US, no NWS MOS coverage) venues excluded.
  5. Decision snapshot = the latest frozen totals snapshot with
     snapshot_instant < kickoff - 24h (game's own season/week cadence
     slots). Totals consensus at that snapshot: >=3 books incl. Pinnacle
     with Over quotes at the consensus line (median-of-medians, within
     0.01) -- same construction as the frozen de-vig work. No fallbacks:
     a game without consensus at its decision snapshot is OUT.
  6. MOS: mechanically selected GFS cycle = max 6h-grid runtime with
     runtime + 4h <= kickoff - 24h. The archived cycle must be retrievable
     from the IEM MOS archive with a numeric `wsp` (sustained wind, knots)
     at the ftime nearest kickoff.

Output: JSON funnel + per-game CSV (NO outcomes, NO residuals, NO model
fits). The script STOPS at eligibility; it never runs the test.

Feasibility verdict (frozen in the draft prereg):
  - INFEASIBLE if eligible N < 250.
  - RETIRE-ON-POWER if MDE > 0.40 pts/mph at exact N, where
      MDE = 2.4865 * 13.5 / (5.0 * sqrt(N))   (one-sided alpha=0.05,
      power=0.8; sigma_res=13.5 pts planning constant; sd_w=5 mph
      planning assumption, replaced by the observed feature SD --
      a feature summary, not an outcome -- at freeze time).
  - else PROCEED-TO-FREEZE (still requires user approval to freeze).
"""

import argparse
import csv
import io
import json
import math
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import sports  # noqa: E402
import nfl_canonical_identity as nci  # noqa: E402
import nfl_hn6_coverage_gate as gate  # noqa: E402 (build_consensus, slot machinery)
import mos_forecast_archive as mos  # noqa: E402 (MOS rules, kickoff parsing)

# ---- frozen constants -------------------------------------------------------
MANIFEST_V1_FINGERPRINT = "0c79d2cdbb7e89f294778f70ded092f151c059ccf6276dca19fd489cebd69920"
FROZEN_QUOTE_COUNT = 291586
FROZEN_N_SNAPSHOTS = 162
SEASONS = (2022, 2023, 2024)

KNOTS_TO_MPH = 1.15078          # NWS MOS wsp is sustained wind in knots
CUTOFF_HOURS = 24               # prediction cutoff = kickoff - 24h
MOS_LOOKBACK_H = 96             # runtime candidates in [cutoff-96h, cutoff]
HTTP_PACING_S = 1.0             # politeness for free IEM calls

N_FLOOR = 250                   # eligible N below this -> INFEASIBLE
MDE_Z_SUM = 2.4865              # z_0.95 + z_0.80, one-sided alpha=0.05, power=0.8
MDE_SIGMA_RES = 13.5            # planning constant: residual SD (pts), literature
MDE_SD_W = 5.0                  # planning assumption: wind SD (mph)
MDE_CAP = 0.40                  # MDE above this (pts/mph) -> RETIRE-ON-POWER

NFLVERSE_SCHEDULES_URL = ("https://github.com/nflverse/nflverse-data/releases"
                          "/download/schedules/schedules.csv")

# Venue roof classes. INCLUDED = genuinely weather-exposed by structure.
# Retractable-roof venues are excluded wholesale: gameday roof position is
# post-cutoff information, so it can never be used to include them.
ROOF_INCLUDE = {"outdoor", "canopy_open_air"}
ROOF_EXCLUDE_DOME = {"dome"}
ROOF_EXCLUDE_RETRACTABLE = {"retractable"}


# ---- pure functions (unit-tested) -------------------------------------------

def knots_to_mph(knots: float) -> float:
    return float(knots) * KNOTS_TO_MPH


def resolve_venue(stadium_name, mapping, aliases):
    """Return (venue_key, mapping_entry) or (None, reason).

    mapping: nfl_stadium_mos.json (2026 keys). aliases: 2022-2024 name ->
    venue_key plus explicit unmatched internationals.
    """
    entry = mapping.get(stadium_name)
    if entry is not None:
        key = stadium_name
    else:
        alias = aliases.get("aliases", {}).get(stadium_name)
        if alias is not None:
            key = alias["venue_key"]
            entry = mapping.get(key)
            # alias may point at a 2026 key name present in mapping
            if entry is None:
                # venue_key IS the mapping key for int_* entries
                entry = mapping.get(key)
        else:
            unmatched = aliases.get("unmatched", {}).get(stadium_name)
            if unmatched is not None:
                return None, "unmatched:" + unmatched["reason"]
            return None, "unmapped_stadium:" + stadium_name
    if entry is None:
        return None, "alias_points_nowhere:" + stadium_name
    return key, entry


def classify_venue(entry):
    """Structural weather-exposure classification.

    Returns (included: bool, reason: str). Domes and retractable roofs are
    excluded by structural rule; only outdoor/canopy_open_air are included.
    Unknown roof values fail loud -- never defaulted.
    """
    if entry.get("status") != "verified":
        return False, "unmatched:" + str(entry.get("unavailable_reason"))
    roof = entry.get("roof")
    if roof in ROOF_INCLUDE:
        return True, "include:" + roof
    if roof in ROOF_EXCLUDE_DOME:
        return False, "exclude:dome"
    if roof in ROOF_EXCLUDE_RETRACTABLE:
        return False, "exclude:retractable"
    raise ValueError(f"unknown roof class {roof!r}; refusing to default")


def decision_slot_for_game(season, week, kickoff, slots):
    """Latest frozen cadence slot strictly before kickoff - 24h.

    slots: list of (season, week, slot, instant) for the game's season/week.
    Returns (slot, instant) or (None, None).
    """
    cutoff = kickoff - timedelta(hours=CUTOFF_HOURS)
    cands = [(slot, inst) for (s, w, slot, inst) in slots
             if s == season and w == week and inst < cutoff]
    if not cands:
        return None, None
    return max(cands, key=lambda t: t[1])


def eligible_mos_runtimes(cutoff, lookback_h=MOS_LOOKBACK_H):
    """6h-grid candidate runtimes with runtime + 4h <= cutoff (frozen rule)."""
    grid_start = cutoff - timedelta(hours=lookback_h)
    # floor to 6h grid
    h = (grid_start.hour // 6) * 6
    cur = grid_start.replace(hour=h, minute=0, second=0, microsecond=0)
    out = []
    while cur <= cutoff:
        if mos.is_eligible(cur, cutoff):
            out.append(cur)
        cur += timedelta(hours=6)
    return out


def pick_nearest_ftime(rows, kickoff):
    """Row (dict with 'ftime' datetime) whose valid time is nearest kickoff.

    Tie -> earlier ftime. Returns None if rows is empty.
    """
    best = None
    for r in rows:
        ft = r["ftime"]
        d = abs((ft - kickoff).total_seconds())
        if best is None or d < best[0] or (d == best[0] and ft < best[1]["ftime"]):
            best = (d, r)
    return best[1] if best else None


def mde_slope(n, z_sum=MDE_Z_SUM, sigma=MDE_SIGMA_RES, sd_w=MDE_SD_W):
    """Minimum detectable slope (pts/mph), one-sided alpha=0.05, power=0.8."""
    return z_sum * sigma / (sd_w * math.sqrt(n))


def feasibility_verdict(n):
    """(verdict, mde) under the frozen draft rules."""
    if n < N_FLOOR:
        return "INFEASIBLE", None
    m = mde_slope(n)
    if m > MDE_CAP:
        return "RETIRE-ON-POWER", m
    return "PROCEED-TO-FREEZE", m


# ---- I/O --------------------------------------------------------------------

def http_get_json(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-hn2-elig"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_kickoffs(url=NFLVERSE_SCHEDULES_URL):
    """game_id -> (kickoff_utc, stadium). Reads ONLY gameday/gametime/stadium.

    nflverse schedules carry scores; this loader never reads them.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-hn2-elig"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read().decode("utf-8")
    out = {}
    for row in csv.DictReader(io.StringIO(raw)):
        if row.get("season") not in ("2022", "2023", "2024"):
            continue
        if row.get("game_type") != "REG":
            continue
        gid = row.get("game_id")
        try:
            ko = mos.parse_kickoff(row["gameday"], row["gametime"])
        except Exception:
            continue
        out[gid] = (ko, row.get("stadium"))
    return out


def fetch_mos_cycle(station, runtime):
    """Archived GFS MOS cycle rows for (station, runtime). Free IEM HTTP."""
    rt = runtime.strftime("%Y-%m-%dT%H:00:00Z")
    url = f"{mos.IEM_MOS_URL}?station={station}&model={mos.FORECAST_MODEL}&runtime={rt}"
    payload = http_get_json(url)
    return payload.get("data", []) or []


def run_eligibility(conn, skip_mos=False):
    report = {
        "prereg": "docs/preregistrations/nfl-h-n2-forecast-wind-totals-DRAFT.md",
        "status": "DRAFT -- not frozen",
        "manifest_v1_fingerprint": MANIFEST_V1_FINGERPRINT,
        "api_credits_used": 0,
        "db_writes": 0,
        "outcomes_inspected": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    funnel = {}
    per_game = []

    # ---- 0. freeze check (frozen scope: spread+total) -------------------------
    hq_table = sports.historical_quotes_table("nfl")
    n_quotes = conn.execute(
        "SELECT COUNT(*) FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')" % hq_table
    ).fetchone()[0]
    n_snaps = conn.execute(
        "SELECT COUNT(DISTINCT observed_at) FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')" % hq_table
    ).fetchone()[0]
    freeze_ok = (n_quotes == FROZEN_QUOTE_COUNT and n_snaps == FROZEN_N_SNAPSHOTS)
    report["freeze_check"] = {"n_quotes": n_quotes, "n_snapshots": n_snaps,
                              "ok": freeze_ok}
    if not freeze_ok:
        report["gate"] = "ABORTED_FREEZE_MISMATCH"
        return report

    # ---- reference data -------------------------------------------------------
    bundle_games = [g for g in json.load(open(str(nci.NFLVERSE_PATH)))
                    if g.get("season") in SEASONS and g.get("game_type") == "REG"]
    funnel["bundle_reg_games"] = len(bundle_games)
    kickoffs = load_kickoffs()
    mapping = json.load(open(HERE / "nfl_stadium_mos.json"))
    # strip the _meta key; resolve_venue handles the rest
    mapping = {k: v for k, v in mapping.items() if k != "_meta"}
    aliases = json.load(open(HERE / "nfl_stadium_aliases_2022_2024.json"))

    # canonical identity (in-memory, same as the H-N6 gate)
    mh_table = sports.market_history_table("nfl")
    rows = conn.execute(gate.ODDS_EVENTS_SQL.format(mh=mh_table)).fetchall()
    odds_events = [{"event_id": eid, "home": home, "away": away,
                    "kickoff": kickoff}
                   for (eid, home, away, kickoff) in rows]
    results = nci.match_events(odds_events, bundle_games, nci.load_aliases(),
                               list(SEASONS))
    game_to_eids = defaultdict(list)
    for m in results["matched"]:
        game_to_eids[m["nflverse_game_id"]].append(m["provider_event_id"])
    funnel["identity_matched"] = len(game_to_eids)
    funnel["identity_unmatched"] = len(results["unmatched_nflverse"])
    funnel["identity_ambiguous"] = len(results["ambiguous"])

    # ---- slot machinery (frozen cadence) --------------------------------------
    slots = gate.snapshot_slots()
    actual_oas = [r[0] for r in conn.execute(
        "SELECT DISTINCT observed_at FROM public.%s "
        "WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL') ORDER BY 1"
        % hq_table).fetchall()]
    slot_oa, unmatched_oas = gate.match_observed_to_slots(actual_oas, slots)
    report["snapshot_slot_match"] = {"matched": len(slot_oa),
                                     "expected": len(slots),
                                     "unmatched_observed_at": len(unmatched_oas)}

    # ---- totals quotes bucketed by slot ---------------------------------------
    all_eids = sorted({eid for eids in game_to_eids.values() for eid in eids})
    qrows = conn.execute(
        "SELECT provider_event_id, book_key, selection, line, observed_at "
        "FROM public.%s "
        "WHERE market = 'FULL_GAME_TOTAL' AND provider_event_id = ANY(%%s)"
        % hq_table, (all_eids,)).fetchall()
    oa_to_slot = {(s, w, slot): oa for (s, w, slot), oa in slot_oa.items()}
    slot_by_oa = {oa: key for key, oa in oa_to_slot.items()}
    quotes_by_slot = defaultdict(list)  # (eid, s, w, slot) -> [(book, sel, line)]
    n_unbucketed = 0
    for eid, book, sel, line, oa in qrows:
        key = slot_by_oa.get(oa)
        if key is None:
            n_unbucketed += 1
            continue
        quotes_by_slot[(eid,) + key].append((book, sel, line))
    report["quotes_accounting"] = {"totals_quote_rows_read": len(qrows),
                                   "rows_outside_frozen_slots": n_unbucketed}

    # ---- per-game funnel --------------------------------------------------------
    counts = defaultdict(int)
    for g in bundle_games:
        gid = g["game_id"]
        s, w = g["season"], g["week"]
        rec = {"nflverse_game_id": gid, "season": s, "week": w,
               "home_team": g["home_team"], "away_team": g["away_team"],
               "stadium": g["stadium"], "status": None, "exclusion_reason": None}
        eids = game_to_eids.get(gid, [])
        if not eids:
            rec.update(status="excluded", exclusion_reason="no_identity")
            counts["no_identity"] += 1
            per_game.append(rec)
            continue
        ko_stad = kickoffs.get(gid)
        if ko_stad is None:
            rec.update(status="excluded", exclusion_reason="no_kickoff")
            counts["no_kickoff"] += 1
            per_game.append(rec)
            continue
        kickoff, _sched_stad = ko_stad
        rec["kickoff_utc"] = kickoff.isoformat()
        cutoff = kickoff - timedelta(hours=CUTOFF_HOURS)
        rec["cutoff_utc"] = cutoff.isoformat()

        venue_key, entry_or_reason = resolve_venue(g["stadium"], mapping, aliases)
        if venue_key is None:
            rec.update(status="excluded",
                       exclusion_reason=entry_or_reason)
            counts["venue_unresolved"] += 1
            per_game.append(rec)
            continue
        rec["venue_key"] = venue_key
        try:
            included, reason = classify_venue(entry_or_reason)
        except ValueError as exc:
            rec.update(status="excluded", exclusion_reason="bad_roof:" + str(exc))
            counts["venue_bad_roof"] += 1
            per_game.append(rec)
            continue
        rec["mos_station"] = entry_or_reason.get("mos_station")
        rec["roof_class"] = reason
        if not included:
            rec.update(status="excluded", exclusion_reason=reason)
            counts[reason.split(":")[0]] += 1
            per_game.append(rec)
            continue

        slot, inst = decision_slot_for_game(s, w, kickoff, slots)
        if slot is None:
            rec.update(status="excluded", exclusion_reason="no_pre_cutoff_slot")
            counts["no_pre_cutoff_slot"] += 1
            per_game.append(rec)
            continue
        rec["decision_slot"] = slot
        rec["decision_instant_utc"] = inst.isoformat()
        oa = oa_to_slot.get((s, w, slot))
        over = []
        if oa is not None:
            for eid in eids:
                for (book, sel, line) in quotes_by_slot.get((eid, s, w, slot), []):
                    if sel == "Over":
                        over.append((book, line))
        cons = gate.build_consensus(over)
        rec["consensus_line"] = cons["consensus_line"]
        rec["n_books_at_line"] = cons["n_books_at_line"]
        rec["pinnacle_present"] = cons["pinnacle_present"]
        if not cons["eligible"]:
            rec.update(status="excluded",
                       exclusion_reason="no_decision_consensus")
            counts["no_decision_consensus"] += 1
            per_game.append(rec)
            continue

        # MOS availability (free IEM archive; no outcomes)
        if skip_mos:
            rec.update(status="mos_skipped")
            counts["mos_skipped"] += 1
            per_game.append(rec)
            continue
        runtimes = eligible_mos_runtimes(cutoff)
        selected = mos.select_cycle(runtimes, cutoff)
        if selected is None:
            rec.update(status="excluded", exclusion_reason="no_eligible_mos_cycle")
            counts["no_eligible_mos_cycle"] += 1
            per_game.append(rec)
            continue
        rec["mos_selected_runtime_utc"] = selected.isoformat()
        try:
            rows_mos = fetch_mos_cycle(rec["mos_station"], selected)
            time.sleep(HTTP_PACING_S)
        except Exception as exc:  # noqa: BLE001 -- failures are data
            rec.update(status="excluded",
                       exclusion_reason="mos_request_failed:" + str(exc)[:120])
            counts["mos_request_failed"] += 1
            per_game.append(rec)
            continue
        if not rows_mos:
            rec.update(status="excluded", exclusion_reason="mos_absent")
            counts["mos_absent"] += 1
            per_game.append(rec)
            continue
        parsed = []
        for fr in rows_mos:
            try:
                ft = mos.parse_mos_time(fr["ftime"])
            except Exception:
                continue
            try:
                wsp = float(fr["wsp"])
                if math.isnan(wsp):
                    raise ValueError
            except (TypeError, ValueError):
                continue
            parsed.append({"ftime": ft, "wsp_knots": wsp})
        chosen = pick_nearest_ftime(parsed, kickoff)
        if chosen is None:
            rec.update(status="excluded",
                       exclusion_reason="mos_no_usable_wsp")
            counts["mos_no_usable_wsp"] += 1
            per_game.append(rec)
            continue
        rec["mos_nearest_ftime_utc"] = chosen["ftime"].isoformat()
        rec["mos_wsp_knots"] = chosen["wsp_knots"]
        rec["wind_mph"] = round(knots_to_mph(chosen["wsp_knots"]), 3)
        rec.update(status="eligible", exclusion_reason=None)
        counts["eligible"] += 1
        per_game.append(rec)

    funnel["attrition"] = dict(counts)
    funnel["eligible_n"] = counts["eligible"]
    report["funnel"] = funnel
    verdict, mde = feasibility_verdict(counts["eligible"])
    report["feasibility"] = {
        "n_floor": N_FLOOR,
        "mde_cap_pts_per_mph": MDE_CAP,
        "mde_pts_per_mph": (round(mde, 3) if mde is not None else None),
        "mde_inputs": {"z_sum": MDE_Z_SUM, "sigma_res_pts": MDE_SIGMA_RES,
                       "sd_w_mph_assumed": MDE_SD_W},
        "verdict": verdict,
    }
    report["per_game"] = per_game
    return report


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--per-game-csv", default=None)
    ap.add_argument("--skip-mos", action="store_true",
                    help="skip the IEM MOS availability step (dry run)")
    args = ap.parse_args(argv)
    dsn = __import__("os").environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    import psycopg  # lazy: runner only
    with psycopg.connect(dsn) as conn:
        report = run_eligibility(conn, skip_mos=args.skip_mos)
    Path(args.out).write_text(json.dumps(report, indent=2))
    if args.per_game_csv:
        with open(args.per_game_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "nflverse_game_id", "season", "week", "home_team", "away_team",
                "stadium", "venue_key", "mos_station", "roof_class",
                "kickoff_utc", "cutoff_utc", "decision_slot",
                "decision_instant_utc", "consensus_line", "n_books_at_line",
                "pinnacle_present", "mos_selected_runtime_utc",
                "mos_nearest_ftime_utc", "mos_wsp_knots", "wind_mph",
                "status", "exclusion_reason"])
            w.writeheader()
            for r in report.get("per_game", []):
                w.writerow({k: r.get(k) for k in w.fieldnames})
    n = report.get("funnel", {}).get("eligible_n")
    v = report.get("feasibility", {}).get("verdict")
    print(json.dumps({"eligible_n": n, "verdict": v,
                      "api_credits_used": 0, "db_writes": 0}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
