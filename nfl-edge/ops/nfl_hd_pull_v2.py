#!/usr/bin/env python3
"""H-D minimum viable priced pull, v2 (CORRECTED ID resolution).

v1 failed 0/60: the manifest's provider event IDs were first observed
in provider snapshots AFTER T_dec (game-day-era IDs); the historical
/events/{id}/odds?date=X endpoint returns empty bookmakers for an ID not
yet issued at X. See nfl-edge/docs/nfl-h-d-pull-incident-2026-09-16.md.

v2 resolves the T_dec-active event ID per game via the historical events
list at the game's own T_dec (the probe-validated method), then queries
that ID's odds at T_dec. Same 60-event universe, same T_dec values, same
markets, same frozen statistical spec as v1 - only the ID resolution
implementation is corrected.

Cost: 1 credit per distinct-T_dec events-list lookup + 10 per odds call.
NOT executed without explicit --credit-cap authorization.
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

API = "https://api.the-odds-api.com"
SPORT = "americanfootball_nfl"
MARKETS = "player_pass_attempts,player_rush_attempts"
MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "nfl_hd_pull_manifest.json")

MANIFEST_ABBR_ALIAS = {
    # nflverse team abbreviations (used in the frozen manifest's matchup
    # strings) differ from the provider's in one case: nflverse "LA"
    # (Rams) vs provider "LAR". Normalize manifest-side before matching.
    "LA": "LAR",
}

NAME_TO_ABBR = {
    "Arizona Cardinals": "ARI", "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL", "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN", "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL", "Denver Broncos": "DEN",
    "Detroit Lions": "DET", "Green Bay Packers": "GB",
    "Houston Texans": "HOU", "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX", "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV", "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR", "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN", "New England Patriots": "NE",
    "New Orleans Saints": "NO", "New York Giants": "NYG",
    "New York Jets": "NYJ", "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT", "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA", "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN", "Washington Commanders": "WAS",
}


def api_get(path, params, api_key):
    qs = urllib.parse.urlencode({**params, "apiKey": api_key})
    req = urllib.request.Request(f"{API}{path}?{qs}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, dict(resp.headers), json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return e.code, dict(e.headers), {"http_error": e.code, "body": body[:500]}


def credits_remaining(api_key):
    st, hd, _ = api_get("/v4/sports", {}, api_key)
    if st != 200:
        raise RuntimeError(f"quota check failed: {st}")
    return hd.get("x-requests-remaining")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--credit-cap", type=int, required=True,
                    help="hard cap on ADDITIONAL credits this run may consume")
    ap.add_argument("--pace", type=float, default=1.0)
    args = ap.parse_args()

    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("THE_ODDS_API_KEY is not set")

    man = json.load(open(MANIFEST))
    events = man["events"]
    assert len(events) == 60, f"manifest has {len(events)} events, want 60"

    # distinct T_dec values -> one events-list lookup each
    tdecs = sorted({e["t_dec_utc"] for e in events})
    projected = len(tdecs) * 1 + len(events) * 10
    if projected > args.credit_cap:
        raise SystemExit(
            f"projected {projected} credits exceeds cap {args.credit_cap}; aborting")

    t0 = time.time()
    st, hd, _ = api_get("/v4/sports", {}, api_key)

    def _used(h):
        try:
            return int(float(h.get("x-requests-used")))
        except (TypeError, ValueError):
            return None

    used0 = _used(hd)
    total_spent = 0          # header-delta spend incl. concurrent background
    aborted = False
    abort_reason = None

    def note_spend(h):
        nonlocal total_spent
        u = _used(h)
        if u is not None and used0 is not None:
            total_spent = max(total_spent, u - used0)

    def guard(next_max=20):
        # True if another paid call (costing at most next_max) stays in-cap.
        return total_spent + next_max <= args.credit_cap

    def do_abort(why):
        nonlocal aborted, abort_reason
        aborted = True
        abort_reason = why
        print(f"ABORT: {why}", flush=True)

    # Phase A: resolve T_dec-active event IDs via the events list
    resolved = {}  # game_id -> provider event id (or None)
    lookups = []
    for tdec in tdecs:
        if aborted:
            break
        if not guard():
            do_abort(f"hard ceiling: spent={total_spent}, next lookup could "
                     f"exceed cap {args.credit_cap}")
            lookups.append({"t_dec": tdec, "status": "skipped_ceiling"})
            break
        st, hd, data = api_get(
            f"/v4/historical/sports/{SPORT}/events", {"date": tdec}, api_key)
        note_spend(hd)
        ev_list = []
        if st == 200:
            ev_list = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(ev_list, list):
                ev_list = []
        for e in events:
            if e["t_dec_utc"] != tdec or e["nflverse_game_id"] in resolved:
                continue
            raw_away, raw_home = e["matchup"].split(" ")[0].split("@")
            away = MANIFEST_ABBR_ALIAS.get(raw_away, raw_away)
            home = MANIFEST_ABBR_ALIAS.get(raw_home, raw_home)
            kick = (datetime.fromisoformat(tdec.replace("Z", "+00:00"))
                    + timedelta(hours=24))
            match = None
            for ev in ev_list:
                ha = NAME_TO_ABBR.get(ev.get("home_team"), "")
                aa = NAME_TO_ABBR.get(ev.get("away_team"), "")
                try:
                    ct = datetime.fromisoformat(
                        ev.get("commence_time", "").replace("Z", "+00:00"))
                except ValueError:
                    continue
                if (ha == home and aa == away
                        and abs((ct - kick).total_seconds()) < 3600):
                    match = ev["id"]
                    break
            resolved[e["nflverse_game_id"]] = match
        lookups.append({"t_dec": tdec, "http_status": st,
                        "n_events_in_list": len(ev_list),
                        "n_newly_resolved": sum(
                            1 for g, v in resolved.items() if v),
                        "requests_used_after": _used(hd)})
        time.sleep(args.pace)

    # Phase B: per-event odds at T_dec with the resolved ID
    out_events = []
    ok = 0
    for i, e in enumerate(events):
        gid = e["nflverse_game_id"]
        tdec = e["t_dec_utc"]
        rid = resolved.get(gid)
        rec = {"nflverse_game_id": gid, "matchup": e["matchup"],
               "t_dec": tdec, "request_date": None,
               "f_inches": e["f_inches"],
               "resolved_id": rid, "id_used": None,
               "status": "no_resolved_id" if not rid else "pending",
               "quotes": []}
        if aborted:
            rec["status"] = "skipped_ceiling"
            out_events.append(rec)
            continue
        if rid:
            if not guard():
                do_abort(f"hard ceiling: spent={total_spent}, next odds call "
                         f"could exceed cap {args.credit_cap}")
                rec["status"] = "skipped_ceiling"
                out_events.append(rec)
                continue
            st, hd, data = api_get(
                f"/v4/historical/sports/{SPORT}/events/{rid}/odds",
                {"regions": "us", "markets": MARKETS, "date": tdec,
                 "oddsFormat": "american"}, api_key)
            note_spend(hd)
            rec["request_date"] = tdec  # the date param actually sent
            if st == 200:
                payload = data.get("data", data) if isinstance(data, dict) else data
                bms = payload.get("bookmakers", []) if isinstance(payload, dict) else []
                if bms:
                    rec["id_used"] = rid
                    rec["status"] = "ok"
                    for bm in bms:
                        for m in bm.get("markets", []):
                            for o in m.get("outcomes", []):
                                rec["quotes"].append({
                                    "book": bm.get("key"),
                                    "market": m.get("key"),
                                    "player": o.get("description"),
                                    "name": o.get("name"),
                                    "line": o.get("point"),
                                    "price": o.get("price")})
                    ok += 1
                else:
                    rec["status"] = "empty_bookmakers_resolved_id"
            else:
                rec["status"] = f"http_{st}"
            time.sleep(args.pace)
        out_events.append(rec)
        if (i + 1) % 10 == 0:
            print(f"[{i+1}/60] ok={ok}", flush=True)

    st, hd, _ = api_get("/v4/sports", {}, api_key)
    used1 = hd.get("x-requests-used")
    try:
        spent = int(float(used1)) - int(float(used0))
    except (TypeError, ValueError):
        spent = None

    result = {"pull_version": "v2", "manifest_source": MANIFEST,
              "events_attempted": 60, "events_ok": ok,
              "distinct_tdec_lookups": len(tdecs),
              "resolved_ids": sum(1 for v in resolved.values() if v),
              "credits_consumed": spent, "credit_cap": args.credit_cap,
              "tracked_spend": total_spent,
              "aborted": aborted, "abort_reason": abort_reason,
              "elapsed_s": round(time.time() - t0, 1),
              "lookups": lookups,
              "events": out_events}
    json.dump(result, open(args.out, "w"), indent=1)
    print(json.dumps({k: v for k, v in result.items() if k != "events"},
                     indent=1))


if __name__ == "__main__":
    main()
