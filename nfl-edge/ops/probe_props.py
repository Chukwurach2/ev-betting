#!/usr/bin/env python3
"""160-credit verification probe for hypotheses C/D (priced prop pulls).

Step 0 (bulk historical with prop markets) already answered locally: the
bulk historical endpoint returns 422 INVALID_MARKET for prop markets --
props are per-event only.

This script (runs in GitHub Actions with the production key):
  Step 1a: resolve 3 probe games' provider event IDs via the historical
           events endpoint (1 credit each).
  Step 1b: per-event historical odds at each game's T_dec (kickoff - 24h,
           floored to the 5-minute grid) with markets=
           player_pass_yds,player_pass_attempts,player_rush_attempts,player_sacks,
           regions=us (up to 40 credits each; markets absent from the
           response cost nothing).

Read-only on the provider; zero DB writes. Writes a JSON summary to --out.
"""
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API = "https://api.the-odds-api.com"
SPORT = "americanfootball_nfl"
MARKETS = "player_pass_yds,player_pass_attempts,player_rush_attempts,player_sacks"

# (label, events_lookup_date_utc, home_fragment, away_fragment)
GAMES = [
    ("2023-W1-Thu", "2023-09-08T06:00:00Z", "Kansas City Chiefs", "Detroit Lions"),
    ("2023-W1-Sun1pm", "2023-09-11T06:00:00Z", "Chicago Bears", "Green Bay Packers"),
    ("2024-W1-Sun1pm", "2024-09-09T06:00:00Z", "Indianapolis Colts", "Houston Texans"),
]


def api_get(path, params, api_key):
    qs = urllib.parse.urlencode({**params, "apiKey": api_key})
    req = urllib.request.Request(f"{API}{path}?{qs}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode()
            return resp.status, dict(resp.headers), json.loads(body)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), {"error": e.read().decode()[:500]}


def floor_5min(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("THE_ODDS_API_KEY is not set")

    # Baseline quota
    st, hd, _ = api_get("/v4/sports", {}, api_key)
    used0 = hd.get("x-requests-used")
    rem0 = hd.get("x-requests-remaining")

    results = {"probe": "C/D prop verification", "quota_before": {"used": used0, "remaining": rem0},
               "games": [], "quota_after": None}
    total_spent = 0

    for label, lookup_date, home_frag, away_frag in GAMES:
        g = {"label": label, "lookup_date": lookup_date}
        st, hd, data = api_get(f"/v4/historical/sports/{SPORT}/events",
                              {"date": lookup_date}, api_key)
        g["events_status"] = st
        if st != 200:
            g["events_error"] = data
            results["games"].append(g)
            continue
        events = data.get("data", data) if isinstance(data, dict) else data
        match = None
        for e in events:
            if (home_frag.lower() in (e.get("home_team") or "").lower()
                    and away_frag.lower() in (e.get("away_team") or "").lower()):
                match = e
                break
        if match is None:
            g["match"] = None
            g["candidates"] = [(e.get("id"), e.get("home_team"), e.get("away_team"))
                               for e in (events[:40] if isinstance(events, list) else [])]
            results["games"].append(g)
            continue
        event_id = match["id"]
        kickoff = datetime.fromisoformat(match["commence_time"].replace("Z", "+00:00"))
        t_dec = floor_5min(kickoff - timedelta(hours=24))
        g["match"] = {"event_id": event_id,
                      "home": match.get("home_team"), "away": match.get("away_team"),
                      "commence_time": match.get("commence_time"),
                      "t_dec": t_dec.strftime("%Y-%m-%dT%H:%M:%SZ")}

        st, hd, data = api_get(f"/v4/historical/sports/{SPORT}/events/{event_id}/odds",
                              {"regions": "us", "markets": MARKETS,
                               "date": g["match"]["t_dec"]}, api_key)
        g["odds_status"] = st
        g["odds_used_header"] = hd.get("x-requests-used")
        if st != 200:
            g["odds_error"] = data
            results["games"].append(g)
            continue
        payload = data.get("data", data) if isinstance(data, dict) else data
        bookmakers = payload.get("bookmakers", []) if isinstance(payload, dict) else []
        markets_seen = {}
        for bm in bookmakers:
            for m in bm.get("markets", []):
                key = m.get("key")
                entry = markets_seen.setdefault(key, {"books": [], "n_outcomes_total": 0,
                                                     "sample_outcomes": []})
                entry["books"].append(bm.get("key"))
                for o in m.get("outcomes", []):
                    entry["n_outcomes_total"] += 1
                    if len(entry["sample_outcomes"]) < 3:
                        entry["sample_outcomes"].append(
                            {"name": o.get("name"), "line": o.get("point"),
                             "price": o.get("price"),
                             "desc": o.get("description")})
        g["markets_seen"] = markets_seen
        g["n_bookmakers"] = len(bookmakers)
        g["bookmaker_keys"] = [bm.get("key") for bm in bookmakers]
        results["games"].append(g)

    st, hd, _ = api_get("/v4/sports", {}, api_key)
    results["quota_after"] = {"used": hd.get("x-requests-used"),
                              "remaining": hd.get("x-requests-remaining")}

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps({"quota_before": results["quota_before"],
                      "quota_after": results["quota_after"],
                      "games": [{k: g.get(k) for k in ("label", "odds_status",
                                "n_bookmakers", "bookmaker_keys")
                                 if k in g} | {"markets": sorted(g.get("markets_seen", {}).keys())}
                                for g in results["games"]]}, indent=2))


if __name__ == "__main__":
    main()
