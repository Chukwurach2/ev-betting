"""H4: Underdog Mechanism Analysis (downgraded to discovered).

Prereg: docs/preregistrations/ncaaf-h4-underdog-mechanism.md
Status: DISCOVERED hypothesis. The split was preregistered (F3); the
direction was not. This is mechanism analysis on the discovery sample —
explanatory only. Cannot claim confirmatory evidence from 2022-24.

M1 (primary): fav/dog x |line| bucket interaction.
M2 (secondary): home-dog vs away-dog.
M3: MOOT (H3 failed, no signal to mediate).
"""
import argparse, json, os, sys, statistics, math
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_outcomes as mo
import market_structure as ms

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"

def prop_test(x1, n1, x2, n2):
    """Two-proportion z-test. Returns (diff, z, p)."""
    if n1 < 10 or n2 < 10: return None
    p1, p2 = x1/n1, x2/n2
    p_pool = (x1 + x2) / (n1 + n2)
    se = (p_pool * (1 - p_pool) * (1/n1 + 1/n2)) ** 0.5
    if se == 0: return None
    z = (p1 - p2) / se
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
    return (p1 - p2, z, p)

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--games", required=True)
    ap.add_argument("--lines", required=True)
    ap.add_argument("--out", default="/tmp/h4_underdog.json")
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]

    games_idx = {}
    for gp in args.games.split(","):
        for key, glist in mo.load_games(gp.strip()).items():
            games_idx.setdefault(key, []).extend(glist)
    lines_by_id = {}
    for lp in args.lines.split(","):
        lines_by_id.update(mo.load_lines(lp.strip()))

    plan = ms.build_plan(args.sport, seasons)
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    hq = ms.sports.historical_quotes_table(args.sport)
    cols = ("provider_event_id, home_team, away_team, kickoff, book_key,"
            " market, selection, line, american_odds, fair_probability,"
            " observed_at")
    with psycopg.connect(dsn) as conn:
        qrows = conn.execute(f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "home_team", "away_team", "kickoff",
         "book_key", "market", "selection", "line", "american_odds",
         "fair_probability", "observed_at"], r)) for r in qrows]

    selected, integrity, matched_instants = ms.select_pairs(recs, plan, seasons)
    events = {}
    for (eid, season, week, window, book, market), v in selected.items():
        events.setdefault((eid, season),
                          (v["home"], v["away"], mo.parse_ts(v["kickoff"])))
    matched, _ = mo.match_events(events, games_idx)

    # Per game: use late window consensus spread
    games = {}
    for (eid, season, week, window, book, market), v in selected.items():
        if season not in seasons: continue
        if market != "FULL_GAME_SPREAD": continue
        games.setdefault((eid, season), {}).setdefault(window, []).append(v["line"])

    rows = []  # (season, is_home_fav, abs_line, home_covered)
    n_no_sign = n_not_fbs = 0
    
    for (eid, season), by_win in games.items():
        m = matched.get((eid, season))
        if not m: continue
        game = m["game"]
        if mo.matchup_class(game) != "FBSvFBS":
            n_not_fbs += 1
            continue
        
        lines = None
        for w in ["late", "mid", "early"]:
            if w in by_win:
                lines = by_win[w]
                break
        if not lines: continue
        cons = statistics.median(lines)
        
        gid = game.get("id")
        cfbd = lines_by_id.get(gid)
        if cfbd is None:
            n_no_sign += 1
            continue
        home_fav = (cfbd > 0) if m["swapped"] else (cfbd < 0)
        
        hs = game.get("homePoints")
        aws = game.get("awayPoints")
        if hs is None or aws is None: continue
        if m["swapped"]:
            hs, aws = aws, hs
        
        # Home cover: (hs - aws) + spread_home > 0, where spread_home is
        # negative if home favored
        spread_home = -abs(cons) if home_fav else abs(cons)
        margin = hs - aws
        # Push = margin + spread_home == 0, exclude
        diff = margin + spread_home
        if abs(diff) < 0.01:  # push
            continue
        home_covered = diff > 0
        
        rows.append({
            "season": season,
            "home_fav": home_fav,
            "abs_line": abs(cons),
            "home_covered": home_covered,
        })

    # M1: Cover rate by fav/dog x |line| bucket
    buckets = [(0, 3), (3, 7), (7, 14), (14, 100)]
    m1 = {}
    for lo, hi in buckets:
        bkey = f"[{lo},{hi})"
        fav = [r for r in rows if r["home_fav"] and lo <= r["abs_line"] < hi]
        dog = [r for r in rows if not r["home_fav"] and lo <= r["abs_line"] < hi]
        # Home fav cover = home covered. Home dog cover = home covered.
        # For fav/dog comparison: fav_cover = P(fav covers), dog_cover = P(dog covers)
        # If home is fav: fav_cover = P(home covered). If home is dog: dog_cover = P(home covered).
        fav_cover = sum(1 for r in fav if r["home_covered"]) / len(fav) if fav else None
        dog_cover = sum(1 for r in dog if r["home_covered"]) / len(dog) if dog else None
        m1[bkey] = {
            "fav_n": len(fav), "fav_cover": round(fav_cover, 4) if fav_cover else None,
            "dog_n": len(dog), "dog_cover": round(dog_cover, 4) if dog_cover else None,
        }
    
    # M1 interaction: Does the dog lean increase with |line|?
    # Simple test: correlation between bucket midpoint and (dog_cover - fav_cover)
    diffs = []
    for lo, hi in buckets:
        bkey = f"[{lo},{hi})"
        d = m1[bkey]
        if d["fav_cover"] is not None and d["dog_cover"] is not None:
            mid = (lo + hi) / 2
            diffs.append((mid, d["dog_cover"] - d["fav_cover"]))
    # If lean grows with line, diffs should increase with mid
    
    # M2: Home-dog vs away-dog
    # Home-dog: home is dog, cover = home covered
    # Away-dog: home is fav, dog cover = NOT home covered (away covered)
    home_dog = [r for r in rows if not r["home_fav"]]
    away_dog = [r for r in rows if r["home_fav"]]
    hd_cover = sum(1 for r in home_dog if r["home_covered"])
    ad_cover = sum(1 for r in away_dog if not r["home_covered"])  # away (dog) covered
    m2 = prop_test(hd_cover, len(home_dog), ad_cover, len(away_dog))
    
    # Overall (for reference)
    all_fav = [r for r in rows if r["home_fav"]]
    all_dog = [r for r in rows if not r["home_fav"]]
    fav_cover_all = sum(1 for r in all_fav if r["home_covered"]) / len(all_fav)
    dog_cover_all = sum(1 for r in all_dog if r["home_covered"]) / len(all_dog)

    result = {
        "preregistration": "docs/preregistrations/ncaaf-h4-underdog-mechanism.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "status": "DISCOVERED (downgraded). Explanatory only.",
        "scope": {"sport": args.sport, "seasons": seasons, "universe": "FBSvFBS"},
        "n_games": len(rows),
        "n_no_sign": n_no_sign,
        "n_not_fbs": n_not_fbs,
        "overall": {
            "fav_cover": round(fav_cover_all, 4),
            "fav_n": len(all_fav),
            "dog_cover": round(dog_cover_all, 4),
            "dog_n": len(all_dog),
            "gap_pp": round((dog_cover_all - fav_cover_all) * 100, 2),
        },
        "M1_by_line_bucket": m1,
        "M1_lean_vs_magnitude": [
            {"midpoint": mid, "dog_minus_fav_pp": round(d * 100, 2)}
            for mid, d in diffs
        ],
        "M2_home_dog_vs_away_dog": {
            "home_dog_cover": round(hd_cover / len(home_dog), 4) if home_dog else None,
            "home_dog_n": len(home_dog),
            "away_dog_cover": round(ad_cover / len(away_dog), 4) if away_dog else None,
            "away_dog_n": len(away_dog),
            "diff": round(m2[0], 4) if m2 else None,
            "z": round(m2[1], 3) if m2 else None,
            "p_value": round(m2[2], 4) if m2 else None,
        },
        "M3": "MOOT (H3 failed, no Elo signal to mediate)",
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {args.out}")
    print(f"Fav cover: {fav_cover_all:.3f} (n={len(all_fav)})")
    print(f"Dog cover: {dog_cover_all:.3f} (n={len(all_dog)})")
    if m2:
        print(f"M2: diff={m2[0]:.4f}, p={m2[2]:.4f}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
