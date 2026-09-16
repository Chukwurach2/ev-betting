#!/usr/bin/env python3
"""H-N6 read-only coverage gate (frozen prereg
`docs/preregistrations/nfl-h-n6-officiating-crew-totals-frozen.md`).

Measures per-game totals-quote presence at the preregistered decision
snapshots. READ-ONLY: only SELECTs on the database; free HTTP for
nflverse reference data (officials, games). NO outcomes inspected
(realized totals stay sealed -- only score *presence* is checked as
availability metadata, values never read or printed). Zero Odds API
credits. No DB writes.

Decision snapshot per game = Wed 12:00 UTC of its game week (every
snapshot is strictly post-crew-announcement). Fallbacks: Sat 12:00 UTC,
then Sun 15:30 UTC. Fallback usage counts as missingness at the decision
snapshot, never as coverage. The decision window is never widened.

Eligibility (frozen prereg):
  - NFL regular season 2022-2024, identity-matched into the frozen dataset
  - decision-snapshot totals consensus: >=3 books incl. Pinnacle with
    Over quotes at the consensus line (median Over line across books,
    within 0.01), same construction as the frozen de-vig work
  - exactly one referee from nflverse officials (load_officials source)
  - realized total present in nflverse games (availability only)
  - postseason excluded

INFEASIBLE: < 700 eligible games with complete data -> retire without
running. This script STOPS at the coverage report; it never runs the test.
"""

import argparse
import csv
import gzip
import io
import json
import os
import statistics
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import sports  # noqa: E402
import nfl_canonical_identity as nci  # noqa: E402

# ---- frozen constants -----------------------------------------------------
FROZEN_FINGERPRINT = "43f853a44bf93937d85149ca5fd7241b"
FROZEN_QUOTE_COUNT = 291586
FROZEN_N_SNAPSHOTS = 162
SNAP_MATCH_TOL_S = 1800  # audit tolerance: provider observed_at vs cadence slot
LINE_TOL = 0.01          # "within 0.01" of the consensus line (NCAAF v1.0 rule)
MIN_BOOKS = 3
PINNACLE_KEY = "pinnacle"
INFEASIBLE_N = 700
SEASONS = (2022, 2023, 2024)

OFFICIALS_URL = ("https://github.com/nflverse/nflverse-data/releases"
                 "/download/officials/officials.csv")
GAMES_URL = ("https://github.com/nflverse/nflverse-data/releases"
             "/download/schedules/games.csv.gz")

ODDS_EVENTS_SQL = """
    SELECT DISTINCT
        event->>'id' AS odds_event_id,
        event->>'home_team' AS home_team,
        event->>'away_team' AS away_team,
        (event->>'commence_time')::timestamptz AS commence_time
    FROM public.{mh} mh
    CROSS JOIN LATERAL jsonb_array_elements(mh.payload->'data') AS event
    WHERE event->>'id' IS NOT NULL
      AND event->>'home_team' IS NOT NULL
      AND event->>'away_team' IS NOT NULL
      AND event->>'commence_time' IS NOT NULL
"""


# ---- pure functions (unit-tested) -----------------------------------------

def snapshot_slots(seasons=SEASONS):
    """All (season, week, slot, expected_instant) for the frozen cadence.

    Slots: 'wed' = Wed 12:00 UTC (decision), 'sat' = Sat 12:00 UTC,
    'sun' = Sun 15:30 UTC of the game's week.
    """
    out = []
    for s in seasons:
        for w in range(1, 19):
            instants = sports.snapshot_times("nfl", s, w)
            assert len(instants) == 3, (s, w, instants)
            for slot, inst in zip(("wed", "sat", "sun"), instants):
                out.append((s, w, slot, inst))
    return out


def match_observed_to_slots(actual_oas, slots, tol_s=SNAP_MATCH_TOL_S):
    """Map each actual provider observed_at to its cadence slot.

    Returns (mapping, unmatched): mapping is
    {(season, week, slot): observed_at}; unmatched is the list of
    observed_at values with no slot within tolerance. Nearest match wins;
    a slot claims at most one observed_at.
    """
    by_dist = []
    for oa in actual_oas:
        best = None
        for (s, w, slot, inst) in slots:
            d = abs((oa - inst).total_seconds())
            if d <= tol_s and (best is None or d < best[0]):
                best = (d, s, w, slot)
        by_dist.append((oa, best))
    # assign greedily by distance so each slot is claimed at most once
    claimed = {}
    claimed_oa = set()
    for oa, best in sorted(by_dist,
                           key=lambda t: t[1][0] if t[1] else float("inf")):
        if best is None:
            continue
        _, s, w, slot = best
        if (s, w, slot) not in claimed:
            claimed[(s, w, slot)] = oa
            claimed_oa.add(oa)
    unmatched = [oa for oa in actual_oas if oa not in claimed_oa]
    return claimed, unmatched


def build_consensus(over_quotes, line_tol=LINE_TOL, min_books=MIN_BOOKS,
                    pinnacle_key=PINNACLE_KEY):
    """Consensus eligibility for one (game, snapshot).

    over_quotes: iterable of (book_key, line) for Over selection.
    Construction (frozen de-vig work): per-book median Over line, then
    median across books = consensus line; books "at the consensus line"
    are those within `line_tol`.
    Returns dict with consensus_line, n_books, books_at_line,
    pinnacle_present, eligible.
    """
    per_book = defaultdict(list)
    for book, line in over_quotes:
        per_book[book].append(float(line))
    book_lines = {b: statistics.median(ls) for b, ls in per_book.items()}
    if not book_lines:
        return {"consensus_line": None, "n_books": 0, "books_at_line": [],
                "pinnacle_present": False, "eligible": False}
    consensus = statistics.median(book_lines.values())
    at_line = sorted(b for b, ln in book_lines.items()
                     if abs(ln - consensus) <= line_tol)
    pinnacle_present = pinnacle_key in at_line
    eligible = len(at_line) >= min_books and pinnacle_present
    return {"consensus_line": consensus,
            "n_books": len(book_lines),
            "n_books_at_line": len(at_line),
            "books_at_line": at_line,
            "pinnacle_present": pinnacle_present,
            "eligible": eligible}


def classify_game(wed_ok, sat_ok, sun_ok):
    """Coverage state for one game: decision / fallback / none.

    Fallback usage is missingness at the decision snapshot, never coverage.
    """
    if wed_ok:
        return "decision"
    if sat_ok or sun_ok:
        return "fallback"
    return "none"


def freeze_forensic(conn, hq_table, mh_table):
    """Read-only characterization of a freeze mismatch. No outcomes."""
    out = {}
    out["quotes_by_source_market"] = [
        {"source": s, "market": m, "n": n}
        for (s, m, n) in conn.execute(
            "SELECT source, market, COUNT(*) FROM public.%s "
            "GROUP BY 1, 2 ORDER BY 3 DESC" % hq_table).fetchall()]
    out["quotes_by_book"] = [
        {"book_key": b, "n": n}
        for (b, n) in conn.execute(
            "SELECT book_key, COUNT(*) FROM public.%s "
            "GROUP BY 1 ORDER BY 2 DESC" % hq_table).fetchall()]
    out["quotes_collected_range"] = [
        str(v) for v in conn.execute(
            "SELECT MIN(collected_at), MAX(collected_at) FROM public.%s"
            % hq_table).fetchone()]
    out["quotes_per_snapshot"] = [
        {"observed_at": str(oa), "n": n}
        for (oa, n) in conn.execute(
            "SELECT observed_at, COUNT(*) FROM public.%s "
            "GROUP BY 1 ORDER BY 1" % hq_table).fetchall()]
    out["market_history_by_region_market"] = [
        {"regions": r, "markets": m, "n": n}
        for (r, m, n) in conn.execute(
            "SELECT regions, markets, COUNT(*) FROM public.%s "
            "GROUP BY 1, 2 ORDER BY 1, 2" % mh_table).fetchall()]
    return out


# ---- I/O ------------------------------------------------------------------

def http_get(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-hn6-gate"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_bundle_games(path=None):
    src = path or str(nci.NFLVERSE_PATH)
    with open(src) as f:
        games = json.load(f)
    return [g for g in games
            if g.get("season") in SEASONS and g.get("game_type") == "REG"]


def load_officials_referees(url=OFFICIALS_URL):
    """Referee (crew chief) per officials game_id (GSIS), 2022-2024 REG.

    Returns (ref_by_gsis, anomalies): ref_by_gsis maps GSIS game_id ->
    official name; anomalies lists GSIS ids with != 1 referee.
    """
    raw = http_get(url).decode("utf-8")
    refs = defaultdict(list)
    for row in csv.DictReader(io.StringIO(raw)):
        if (row["season"] in ("2022", "2023", "2024")
                and row["season_type"] == "REG"
                and row["position"] == "Referee"):
            refs[row["game_id"]].append(row["official_name"])
    ref_by_gsis = {}
    anomalies = []
    for gid, names in refs.items():
        if len(names) == 1:
            ref_by_gsis[gid] = names[0]
        else:
            anomalies.append({"gsis_game_id": gid, "n_referees": len(names),
                              "names": sorted(set(names))})
    return ref_by_gsis, anomalies


def load_games_reference(url=GAMES_URL):
    """nflverse games.csv.gz: gsis->game_id map + score presence.

    Returns (gsis_to_game_id, score_present): score_present maps bundle
    game_id -> bool (availability only; values never read).
    """
    raw = http_get(url)
    with gzip.open(io.BytesIO(raw), "rt") as f:
        rows = list(csv.DictReader(f))
    gsis_to_game_id = {}
    score_present = {}
    for r in rows:
        if r["season"] not in ("2022", "2023", "2024"):
            continue
        if r["game_type"] != "REG":
            continue
        if r["old_game_id"]:
            gsis_to_game_id[r["old_game_id"]] = r["game_id"]
        # availability only: never inspect the values
        score_present[r["game_id"]] = bool(r["home_score"]
                                           and r["away_score"])
    return gsis_to_game_id, score_present


def run_gate(conn, bundle_path=None, officials_url=OFFICIALS_URL,
             games_url=GAMES_URL):
    """Execute the read-only coverage gate. Returns the report dict."""
    report = {
        "prereg": "docs/preregistrations/nfl-h-n6-officiating-crew-totals-frozen.md",
        "frozen_fingerprint": FROZEN_FINGERPRINT,
        "api_credits_used": 0,
        "db_writes": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # ---- 1. freeze verification -------------------------------------------
    mh_table = sports.market_history_table("nfl")
    hq_table = sports.historical_quotes_table("nfl")
    n_quotes = conn.execute(
        "SELECT COUNT(*) FROM public.%s" % hq_table).fetchone()[0]
    oa_rows = conn.execute(
        "SELECT DISTINCT observed_at FROM public.%s ORDER BY 1"
        % hq_table).fetchall()
    actual_oas = [r[0] for r in oa_rows]
    freeze_ok = (n_quotes == FROZEN_QUOTE_COUNT
                 and len(actual_oas) == FROZEN_N_SNAPSHOTS)
    report["freeze_check"] = {
        "n_quotes": n_quotes, "expected_quotes": FROZEN_QUOTE_COUNT,
        "n_snapshots": len(actual_oas),
        "expected_snapshots": FROZEN_N_SNAPSHOTS,
        "ok": freeze_ok,
    }
    if not freeze_ok:
        report["gate"] = "ABORTED_FREEZE_MISMATCH"
        report["forensic"] = freeze_forensic(conn, hq_table, mh_table)
        return report

    # ---- 2. slot identification (provider observed_at -> cadence slot) -----
    slots = snapshot_slots()
    slot_oa, unmatched_oas = match_observed_to_slots(actual_oas, slots)
    report["snapshot_slot_match"] = {
        "matched_slots": len(slot_oa),
        "expected_slots": len(slots),
        "unmatched_observed_at": [o.isoformat() for o in unmatched_oas],
    }

    # ---- 3. canonical identity: game -> odds event ids --------------------
    bundle_games = load_bundle_games(bundle_path)
    aliases = nci.load_aliases()
    rows = conn.execute(ODDS_EVENTS_SQL.format(mh=mh_table)).fetchall()
    odds_events = [{"event_id": eid, "home": home, "away": away,
                    "kickoff": kickoff}
                   for (eid, home, away, kickoff) in rows]
    results = nci.match_events(odds_events, bundle_games, aliases,
                               list(SEASONS))
    game_to_eids = defaultdict(list)
    for m in results["matched"]:
        game_to_eids[m["nflverse_game_id"]].append(m["provider_event_id"])
    # missing_from_source must be present and never counted as eligible
    mfs = results.get("missing_from_source", [])
    report["identity"] = {
        "n_bundle_reg_games": len(bundle_games),
        "n_matched_games": len(game_to_eids),
        "n_unmatched_nflverse": len(results["unmatched_nflverse"]),
        "n_ambiguous": len(results["ambiguous"]),
        "missing_from_source": [
            {"season": e["season"], "week": e["week"],
             "home": e["home"], "away": e["away"]}
            for e in mfs],
    }

    # ---- 4. totals quotes, bucketed by (game, slot) ----------------------
    all_eids = sorted({eid for eids in game_to_eids.values()
                       for eid in eids})
    qrows = conn.execute(
        """SELECT provider_event_id, book_key, selection, line, observed_at
           FROM public.%s
           WHERE market = 'FULL_GAME_TOTAL'
             AND provider_event_id = ANY(%%s)""" % hq_table,
        (all_eids,)).fetchall()
    # observed_at -> slot for fast bucketing
    oa_to_slot = {oa: key for key, oa in slot_oa.items()}
    quotes_by_game_slot = defaultdict(list)  # (eid,s,w,slot) -> [(book,line)]
    n_quotes_unbucketed = 0
    for eid, book, sel, line, oa in qrows:
        key = oa_to_slot.get(oa)
        if key is None:
            n_quotes_unbucketed += 1
            continue
        s, w, slot = key
        quotes_by_game_slot[(eid, s, w, slot)].append((book, sel, line))
    report["quotes_accounting"] = {
        "totals_quote_rows_read": len(qrows),
        "rows_outside_cadence_slots": n_quotes_unbucketed,
    }

    # ---- 5. reference data: officials + games -----------------------------
    ref_by_gsis, ref_anomalies = load_officials_referees(officials_url)
    gsis_to_game_id, score_present = load_games_reference(games_url)
    game_to_ref = {}
    for gsis, name in ref_by_gsis.items():
        gid = gsis_to_game_id.get(gsis)
        if gid:
            game_to_ref[gid] = name
    report["officials"] = {
        "n_gsis_with_referee": len(ref_by_gsis),
        "referee_anomalies": ref_anomalies,
    }

    # ---- 6. per-game coverage + eligibility --------------------------------
    by_season = {s: {"n_games": 0, "identity_matched": 0,
                     "wed_consensus": 0, "sat_consensus": 0,
                     "sun_consensus": 0, "crew_present": 0,
                     "score_present": 0, "eligible": 0,
                     "fallback_only": 0}
                 for s in SEASONS}
    no_coverage = []   # matched games with no eligible consensus at any slot
    no_identity = []   # bundle games with no Odds event at all
    per_game = {}

    for g in bundle_games:
        gid = g["game_id"]
        s, w = g["season"], g["week"]
        st = by_season[s]
        st["n_games"] += 1
        eids = game_to_eids.get(gid, [])
        if not eids:
            no_identity.append({"game_id": gid, "season": s, "week": w,
                                "home": g["home_team"], "away": g["away_team"]})
            continue
        st["identity_matched"] += 1

        slot_ok = {}
        for slot in ("wed", "sat", "sun"):
            over = []
            for eid in eids:
                for (book, sel, line) in quotes_by_game_slot.get(
                        (eid, s, w, slot), []):
                    if sel == "Over":
                        over.append((book, line))
            cons = build_consensus(over)
            slot_ok[slot] = cons["eligible"]
            if cons["eligible"]:
                st[slot + "_consensus"] += 1

        state = classify_game(slot_ok["wed"], slot_ok["sat"], slot_ok["sun"])
        crew_ok = gid in game_to_ref
        score_ok = score_present.get(gid, False)
        if crew_ok:
            st["crew_present"] += 1
        if score_ok:
            st["score_present"] += 1
        eligible = slot_ok["wed"] and crew_ok and score_ok
        if eligible:
            st["eligible"] += 1
        if state == "fallback":
            st["fallback_only"] += 1
        if state == "none":
            no_coverage.append({
                "game_id": gid, "season": s, "week": w,
                "home": g["home_team"], "away": g["away_team"],
                "reason": "no eligible totals consensus at any of the "
                          "three post-announcement snapshot slots"})
        per_game[gid] = {"season": s, "week": w, "state": state,
                         "crew_present": crew_ok, "score_present": score_ok,
                         "eligible": eligible}

    eligible_n = sum(st["eligible"] for st in by_season.values())
    fallback_n = sum(st["fallback_only"] for st in by_season.values())
    report["by_season"] = {str(s): v for s, v in by_season.items()}
    report["eligible_n"] = eligible_n
    report["fallback_only_n"] = fallback_n
    report["no_coverage_any_slot"] = sorted(
        no_coverage, key=lambda x: (x["season"], x["week"], x["game_id"]))
    report["no_identity_match"] = sorted(
        no_identity, key=lambda x: (x["season"], x["week"], x["game_id"]))

    # ---- 7. missing-data pull sizing (prereg formula; NOT authorized) -----
    n_missing = len(no_coverage)
    report["missing_data_pull"] = {
        "games_missing": n_missing,
        "credits_formula": "10 credits x 1 region x 1 market x 1 timestamp "
                           "x games_missing",
        "credits": 10 * n_missing,
        "authorized": False,
    }

    # ---- 8. gate -----------------------------------------------------------
    report["gate"] = "PASS" if eligible_n >= INFEASIBLE_N else "INFEASIBLE"
    report["infeasible_threshold"] = INFEASIBLE_N
    return report


def print_summary(rep):
    fc = rep["freeze_check"]
    print("H-N6 coverage gate (read-only)")
    print("  freeze check: quotes=%d (expected %d), snapshots=%d "
          "(expected %d) -> %s"
          % (fc["n_quotes"], fc["expected_quotes"], fc["n_snapshots"],
             fc["expected_snapshots"],
             "OK" if fc["ok"] else "MISMATCH - STOP"))
    if rep.get("gate") == "ABORTED_FREEZE_MISMATCH":
        print("  ABORTED: frozen dataset fingerprint mismatch")
        fz = rep.get("forensic", {})
        for row in fz.get("quotes_by_source_market", []):
            print("    source=%s market=%s n=%d"
                  % (row["source"], row["market"], row["n"]))
        print("    collected_at range: %s"
              % " .. ".join(fz.get("quotes_collected_range", [])))
        print("    market_history (regions/markets): %s"
              % fz.get("market_history_by_region_market"))
        top_books = fz.get("quotes_by_book", [])[:8]
        print("    top books: %s"
              % ", ".join("%s=%d" % (b["book_key"], b["n"])
                          for b in top_books))
        return
    ident = rep["identity"]
    print("  bundle REG games: %d | identity-matched: %d | unmatched: %d | "
          "ambiguous: %d"
          % (ident["n_bundle_reg_games"], ident["n_matched_games"],
             ident["n_unmatched_nflverse"], ident["n_ambiguous"]))
    print("  snapshot slots matched: %d/%d"
          % (rep["snapshot_slot_match"]["matched_slots"],
             rep["snapshot_slot_match"]["expected_slots"]))
    for s in sorted(rep["by_season"]):
        st = rep["by_season"][s]
        print("  season %s: games=%d matched=%d wed=%d sat=%d sun=%d "
              "crew=%d score=%d ELIGIBLE=%d fallback_only=%d"
              % (s, st["n_games"], st["identity_matched"],
                 st["wed_consensus"], st["sat_consensus"], st["sun_consensus"],
                 st["crew_present"], st["score_present"], st["eligible"],
                 st["fallback_only"]))
    print("  eligible N = %d (threshold %d)"
          % (rep["eligible_n"], rep["infeasible_threshold"]))
    print("  no eligible consensus at any slot: %d games"
          % len(rep["no_coverage_any_slot"]))
    print("  no identity match (not in frozen dataset): %d games"
          % len(rep["no_identity_match"]))
    mdp = rep["missing_data_pull"]
    print("  missing-data pull sizing: %d games -> %d credits (NOT authorized)"
          % (mdp["games_missing"], mdp["credits"]))
    print("  GATE: %s" % rep["gate"])
    print("  api_credits_used=0 db_writes=0")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/nfl_hn6_coverage_gate.json")
    ap.add_argument("--nflverse-file", default=None)
    ap.add_argument("--officials-url", default=OFFICIALS_URL)
    ap.add_argument("--games-url", default=GAMES_URL)
    args = ap.parse_args(argv)

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    import psycopg
    # read-only guard at the session level
    with psycopg.connect(dsn,
                         options="-c default_transaction_read_only=on") as conn:
        rep = run_gate(conn, bundle_path=args.nflverse_file,
                       officials_url=args.officials_url,
                       games_url=args.games_url)
    with open(args.out, "w") as f:
        json.dump(rep, f, indent=1, default=str)
    print_summary(rep)
    if rep.get("gate") == "ABORTED_FREEZE_MISMATCH":
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
