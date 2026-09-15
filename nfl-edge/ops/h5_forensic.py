"""H5 forensic: decompose 2022-24 FBSvFCS cover rate by window.
BURNED data only. Explanatory, not confirmatory. Zero API credits.

Question: The 55.6% (n=942 event-windows) — which windows drove it?
Was Wednesday part of it, or was it Fri/Sat?
"""
import json
import os
import statistics
import sys
from math import sqrt
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_outcomes as mo
import market_structure as ms

def wilson(k, n, z=1.96):
    if n == 0:
        return (0, 0)
    p = k / n
    denom = 1 + z*z/n
    center = (p + z*z/(2*n)) / denom
    half = z * sqrt(p*(1-p)/n + z*z/(4*n*n)) / denom
    return (center - half, center + half)

def main():
    seasons = [2022, 2023, 2024]
    # Fetch CFBD games
    import urllib.request
    sys.path.insert(0, "/opt/hatch/skills/skill-creator/bin")
    import dynamic_credentials as dc
    
    games = []
    for season in seasons:
        for st in ["regular", "postseason"]:
            url = f"https://api.collegefootballdata.com/games?year={season}&seasonType={st}"
            req = urllib.request.Request(url)
            dc.add_surrogate_to_request(req, "custom.collegefootballdata",
                                        allowed_hosts=["api.collegefootballdata.com"])
            with urllib.request.urlopen(req, timeout=30) as resp:
                games.extend(dc.read_json_response(resp))
    print(f"CFBD games: {len(games)}", file=sys.stderr)
    
    # Build games index (same as F2)
    games_idx = mo.build_games_index(games)
    
    # Query historical quotes
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    hq = ms.sports.historical_quotes_table("ncaaf")
    cols = ("provider_event_id, home_team, away_team, kickoff, book_key,"
            " market, selection, line, american_odds, fair_probability,"
            " observed_at")
    with psycopg.connect(dsn) as conn:
        qrows = conn.execute(f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "home_team", "away_team", "kickoff",
         "book_key", "market", "selection", "line", "american_odds",
         "fair_probability", "observed_at"], r)) for r in qrows]
    print(f"Quote rows: {len(recs)}", file=sys.stderr)
    
    # Select pairs (same as F2)
    plan = ms.build_plan("ncaaf", seasons)
    selected, integrity, matched_instants = ms.select_pairs(recs, plan, seasons)
    print(f"Selected: {len(selected)}", file=sys.stderr)
    
    # Match events
    events = {}
    for (eid, season, week, window, book, market), v in selected.items():
        events.setdefault((eid, season),
                          (v["home"], v["away"], mo.parse_ts(v["kickoff"])))
    matched, match_integrity = mo.match_events(events, games_idx)
    print(f"Matched events: {len(matched)}", file=sys.stderr)
    
    # Build consensus per (eid, season, window, market) for spreads
    cons = defaultdict(lambda: {"lines": [], "books": set()})
    for (eid, season, week, window, book, market), v in selected.items():
        if market != "FULL_GAME_SPREAD":
            continue
        key = (eid, season, window)
        cons[key]["lines"].append(v["line"])
        cons[key]["books"].add(book)
    
    # For each consensus, determine FBSvFCS and cover
    by_window = defaultdict(lambda: {"obs": 0, "games": set(), "covers": 0, 
                                     "decisive": 0, "pushes": 0, "n_books": []})
    by_season_window = defaultdict(lambda: {"covers": 0, "decisive": 0})
    
    # Need CFBD lines for sign oracle
    lines_by_id = {}
    for season in seasons:
        for st in ["regular", "postseason"]:
            url = f"https://api.collegefootballdata.com/lines?year={season}&seasonType={st}"
            req = urllib.request.Request(url)
            dc.add_surrogate_to_request(req, "custom.collegefootballdata",
                                        allowed_hosts=["api.collegefootballdata.com"])
            with urllib.request.urlopen(req, timeout=30) as resp:
                for l in dc.read_json_response(resp):
                    if l.get("lines"):
                        try:
                            lines_by_id[l["id"]] = float(l["lines"][0].get("spread", 0))
                        except:
                            pass
    
    for (eid, season, window), c in cons.items():
        m = matched.get((eid, season))
        if not m:
            continue
        g = m["game"]
        # Check FBSvFCS
        hc = (g.get("homeClassification") or "").lower()
        ac = (g.get("awayClassification") or "").lower()
        if m.get("swapped"):
            hc, ac = ac, hc
        is_fbsfcs = (hc == "fbs" and ac == "fcs") or (hc == "fcs" and ac == "fbs")
        # H5 is specifically FBS home vs FCS away
        is_h5 = (hc == "fbs" and ac == "fcs")
        if not is_h5:
            continue
        
        # Consensus (median, min 3 books)
        if len(c["books"]) < 3:
            continue
        consensus = statistics.median(c["lines"])
        
        # Outcome
        if m.get("swapped"):
            hp, ap = g["awayPoints"], g["homePoints"]
        else:
            hp, ap = g["homePoints"], g["awayPoints"]
        if hp is None or ap is None:
            continue
        margin = hp - ap
        diff = margin + consensus
        if abs(diff) < 0.001:
            result = "push"
        elif diff > 0:
            result = "cover"
        else:
            result = "no_cover"
        
        bw = by_window[window]
        bw["obs"] += 1
        bw["games"].add((eid, season))
        bw["n_books"].append(len(c["books"]))
        if result == "push":
            bw["pushes"] += 1
        else:
            bw["decisive"] += 1
            if result == "cover":
                bw["covers"] += 1
        
        sw = by_season_window[(season, window)]
        if result != "push":
            sw["decisive"] += 1
            if result == "cover":
                sw["covers"] += 1
    
    # Output
    output = {"by_window": {}, "by_season_window": {}}
    print("\n=== H5 FORENSIC: 2022-24 FBSvFCS by window ===")
    print("(BURNED data. Explanatory only.)\n")
    for w in ["early", "mid", "late"]:
        bw = by_window[w]
        n, k = bw["decisive"], bw["covers"]
        L, U = wilson(k, n)
        avg_books = statistics.mean(bw["n_books"]) if bw["n_books"] else 0
        print(f"{w.upper()}:")
        print(f"  Event-windows: {bw['obs']}, Unique games: {len(bw['games'])}")
        print(f"  Covers: {k}/{n} = {k/n*100:.1f}%" if n else "  n=0")
        print(f"  Wilson 95% CI: [{L*100:.1f}%, {U*100:.1f}%]" if n else "")
        print(f"  Pushes: {bw['pushes']}, Avg books: {avg_books:.1f}")
        output["by_window"][w] = {
            "obs": bw["obs"], "unique_games": len(bw["games"]),
            "covers": k, "decisive": n,
            "cover_rate": k/n if n else 0,
            "wilson_L": L, "wilson_U": U,
            "pushes": bw["pushes"], "avg_books": avg_books,
        }
    
    print("\n=== By season x window ===")
    for s in seasons:
        for w in ["early", "mid", "late"]:
            sw = by_season_window[(s, w)]
            n, k = sw["decisive"], sw["covers"]
            if n:
                print(f"  {s} {w}: {k}/{n} = {k/n*100:.1f}%")
                output["by_season_window"][f"{s}_{w}"] = {
                    "covers": k, "decisive": n, "rate": k/n}
    
    json.dump(output, open("h5_forensic_output.json", "w"), indent=1)
    print("\nSaved to h5_forensic_output.json")

if __name__ == "__main__":
    main()
