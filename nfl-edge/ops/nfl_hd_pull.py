#!/usr/bin/env python3
"""H-D minimum viable historical pull (PAID — authorized 2026-09-16 13:33 EDT).

Reads the frozen manifest (nfl_hd_pull_manifest.json: 60 treated events,
corrected provider ids). For each event, tries candidate event ids in order
against:

    GET /v4/historical/sports/americanfootball_nfl/events/{eventId}/odds
        ?regions=us
        &markets=player_pass_attempts,player_rush_attempts
        &date={T_dec}            (kickoff-24h, 5-min grid; closest snapshot <= T_dec)
        &oddsFormat=american

Uses the first id returning non-empty bookmakers. Empty responses cost
nothing; non-200 responses are recorded as explicit gaps and never retried
into a different timestamp.

Spend tracking: baseline quota from GET /v4/sports (free). Before every paid
call, abort if spent_so_far + 20 > 1200 (per-call max). HARD 1,200-credit
ceiling — the script exits with a blocker record rather than breaching it.

Writes the immutable pull log (requests, statuses, credit headers, parsed
quotes, full raw bodies) to --out. Zero DB writes. Zero outcome access:
this script never touches nflverse player_stats, scores, or anything
derived from game outcomes.
"""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
API = "https://api.the-odds-api.com"
SPORT = "americanfootball_nfl"
MARKETS = "player_pass_attempts,player_rush_attempts"
CREDIT_CEILING = 1200
PER_CALL_MAX = 20
PACE_S = 1.0
TIMEOUT_S = 60


def api_get(path, params, api_key):
    qs = urllib.parse.urlencode({**params, "apiKey": api_key})
    req = urllib.request.Request(f"{API}{path}?{qs}")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode()[:500]
        except Exception:
            body = "<unreadable>"
        return e.code, dict(e.headers), {"http_error": body}
    except Exception as e:  # transport-level: record, do not retry
        return -1, {}, {"transport_error": f"{type(e).__name__}: {e}"[:300]}


def spent(rem_before, rem_now):
    try:
        return int(rem_before) - int(rem_now)
    except (TypeError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--manifest", default=str(HERE / "nfl_hd_pull_manifest.json"))
    args = ap.parse_args()
    import os
    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("THE_ODDS_API_KEY is not set")

    manifest = json.load(open(args.manifest))
    events = manifest["events"]
    assert len(events) == 60, f"manifest has {len(events)} events, expected 60"

    st, hd, _ = api_get("/v4/sports", {}, api_key)
    rem0 = hd.get("x-requests-remaining")
    log = {
        "pull": "nfl-h-d minimum viable historical pull",
        "authorized": "2026-09-16T13:33:00-04:00 (user)",
        "prereg": "nfl-edge/docs/preregistrations/nfl-h-d-precipitation-attempts-frozen.md",
        "manifest_version": manifest["manifest_version"],
        "credit_ceiling": CREDIT_CEILING,
        "run_started_utc": datetime.now(timezone.utc).isoformat(),
        "quota_before": {"remaining": rem0, "sports_status": st},
        "events": [],
        "aborted": False,
        "abort_reason": None,
    }

    total_spent = 0
    for ev in events:
        rec = {"nflverse_game_id": ev["nflverse_game_id"],
               "matchup": ev["matchup"],
               "t_dec": ev["t_dec_utc"],
               "f_inches": ev["f_inches"],
               "tries": [], "id_used": None, "status": None,
               "n_bookmakers": 0, "quotes": []}
        for eid in ev["event_ids"]:
            if total_spent + PER_CALL_MAX > CREDIT_CEILING:
                log["aborted"] = True
                log["abort_reason"] = (
                    f"hard ceiling: spent={total_spent}, next call could reach "
                    f"{total_spent + PER_CALL_MAX} > {CREDIT_CEILING}")
                rec["status"] = "skipped_ceiling"
                break
            st, hd, data = api_get(
                f"/v4/historical/sports/{SPORT}/events/{eid}/odds",
                {"regions": "us", "markets": MARKETS,
                 "date": ev["t_dec_utc"], "oddsFormat": "american"},
                api_key)
            rem = hd.get("x-requests-remaining")
            s = spent(rem0, rem)
            if s is not None:
                total_spent = max(total_spent, s)
            books = []
            if st == 200 and isinstance(data, dict):
                books = data.get("bookmakers", []) or []
            rec["tries"].append({"event_id": eid, "http_status": st,
                                 "requests_remaining": rem,
                                 "n_bookmakers": len(books)})
            if books:
                rec["id_used"] = eid
                rec["status"] = "ok"
                rec["n_bookmakers"] = len(books)
                rec["raw"] = data
                for b in books:
                    bk = b.get("key")
                    for m in b.get("markets", []) or []:
                        mk = m.get("key")
                        if mk not in ("player_pass_attempts", "player_rush_attempts"):
                            continue
                        for o in m.get("outcomes", []) or []:
                            rec["quotes"].append({
                                "book": bk, "market": mk,
                                "player": o.get("description"),
                                "name": o.get("name"),
                                "line": o.get("point"),
                                "price": o.get("price"),
                            })
                break
            time.sleep(PACE_S)
        else:
            # loop exhausted without break -> no id returned bookmakers
            if rec["status"] is None:
                rec["status"] = "no_bookmakers_all_ids"
        if rec["status"] == "skipped_ceiling":
            log["events"].append(rec)
            break
        log["events"].append(rec)
        time.sleep(PACE_S)

    st, hd, _ = api_get("/v4/sports", {}, api_key)
    rem1 = hd.get("x-requests-remaining")
    final_spent = spent(rem0, rem1)
    log["quota_after"] = {"remaining": rem1}
    log["credits_consumed"] = final_spent if final_spent is not None else total_spent
    log["run_finished_utc"] = datetime.now(timezone.utc).isoformat()

    Path(args.out).write_text(json.dumps(log))
    n_ok = sum(1 for e in log["events"] if e["status"] == "ok")
    print(json.dumps({"events_attempted": len(log["events"]),
                      "events_ok": n_ok,
                      "credits_consumed": log["credits_consumed"],
                      "aborted": log["aborted"],
                      "abort_reason": log["abort_reason"]}, indent=2))


if __name__ == "__main__":
    main()
