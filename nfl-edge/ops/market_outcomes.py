"""Phase F(b): market vs outcomes (prereg ncaaf-market-outcomes-f2.md).

Read-only SELECTs against ncaaf_edge_historical_quotes + a CFBD /games JSON
(years 2022-2024 only, fetched in the workflow). Zero Odds API credits.

Consensus per event x window x market = median line and median de-vigged
fair probability of home covering (spreads) / over hitting (totals).
Outcomes are matched on identity (team names + kickoff) only, never on
scores. Descriptive: calibration bins, splits, closing-efficiency
comparison. Selects nothing.
"""
import argparse
import datetime as dt
import json
import math
import os
import statistics
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_structure as ms

FROZEN_FINGERPRINT = "684c58410968c440a6d7500582ac9ecf"
KICKOFF_TOLERANCE = dt.timedelta(hours=36)
SPREAD_BUCKETS = [(0, 3), (3, 7), (7, 14), (14, float("inf"))]
TOTAL_BUCKETS = [(0, 45), (45, 55), (55, 65), (65, float("inf"))]
P4 = {"SEC", "Big Ten", "Big 12", "ACC", "Pac-12"}


def norm_name(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    # keep the "A&M" abbreviation intact ("Texas A&M" -> "texasam") ...
    s = s.replace("A&M", "AM").replace("a&m", "am")
    # ... but expand a standalone "&" ("William & Mary" -> "williamandmary")
    s = s.replace("&", " and ")
    return "".join(c for c in s.lower() if c.isalnum())


# Frozen school-name alias table (prereg amendment A2, 2026-09-15):
# provider-style normalized prefix -> CFBD canonical form. Applied before
# matching. Order matters: 'albanystate' shadows 'albany' (Albany State is
# a distinct DII school, not UAlbany). Frozen; any addition is a further
# preregistered amendment.
SCHOOL_ALIASES = {
    "appalachianstate": "appstate",          # CFBD "App State"
    "albanystate": "albanystate",             # distinct DII school; shadows 'albany'
    "albany": "ualbany",                     # CFBD "UAlbany"
    "umass": "massachusetts",                # CFBD "Massachusetts"
    "citadel": "thecitadel",                 # CFBD "The Citadel"
    "southeasternlouisiana": "selouisiana",  # CFBD "SE Louisiana"
    "houstonbaptist": "houstonchristian",    # renamed; CFBD "Houston Christian"
    "youngstownst": "youngstownstate",       # CFBD "Youngstown State"
    "southernmississippi": "southernmiss",   # CFBD "Southern Miss"
    "texasamcommerce": "easttexasam",        # renamed Nov 2024; CFBD "East Texas A&M"
    "liu": "longislanduniversity",           # CFBD "Long Island University"
    "stfrancis": "saintfrancis",             # CFBD "Saint Francis" (PA school)
}


def alias_school(n):
    for old, new in SCHOOL_ALIASES.items():
        if n.startswith(old):
            return new + n[len(old):]
    return n


def build_school_index(games_idx):
    """Per-season CFBD school names, longest first (for longest-prefix match)."""
    schools = {}
    for (season, kh, ka), glist in games_idx.items():
        s = schools.setdefault(season, set())
        s.add(kh)
        s.add(ka)
    return {season: sorted(ss, key=len, reverse=True)
            for season, ss in schools.items()}


def best_school(t, schools_sorted):
    """Longest CFBD school name in prefix relation with t (either direction).

    Longest-prefix wins: e.g. provider 'Texas A&M' ('texasam...') resolves
    to CFBD 'texasam' (Texas A&M), never to the shorter 'texas' — so a
    provider-listed Georgia-vs-Texas A&M event cannot false-match CFBD's
    Texas-vs-Georgia game.
    """
    for s in schools_sorted:
        if t.startswith(s) or s.startswith(t):
            return s
    return None


def parse_ts(s):
    s = str(s)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)


def load_games(path):
    with open(path) as f:
        games = json.load(f)
    idx = {}
    for g in games:
        if g.get("season") not in (2022, 2023, 2024):
            continue  # 2025 never enters, even if present in the file
        key = (g["season"], norm_name(g["homeTeam"]), norm_name(g["awayTeam"]))
        idx.setdefault(key, []).append(g)
    return idx


def match_events(events, games_idx):
    """events: {(eid, season): (home, away, kickoff)}. Returns
    (matched, integrity) with matched[(eid, season)] =
    {"game": game dict, "swapped": bool}.

    Identity-only matching on team names + kickoff, never on scores.
    The odds provider names NCAAF teams '{School} {Mascot}' (e.g.
    'Duke Blue Devils') while CFBD uses the school name ('Duke'). Each
    provider team name is resolved to the longest CFBD school name in
    prefix relation (after the frozen SCHOOL_ALIASES substitution), so
    'Texas A&M' can never resolve to 'Texas'. A CFBD game matches when both
    teams resolve to its schools and kickoffs agree within +-36h. When the
    same two teams appear with home/away reversed (neutral-site designation
    differences), the event matches with swapped=True and scores are
    realigned to the provider's orientation in analyze(). Unmatched events
    get a deterministic reason code (phantom / kickoff_miss /
    team_mislabel / conflict / not_completed / null_scores).
    """
    matched = {}
    integrity = {"events_total": len(events), "matched": 0,
                 "swapped_matched": 0, "alias_used": 0,
                 "unmatched_reasons": {}}
    # per-season flat lists for name matching
    season_games = {}
    for (season, kh, ka), glist in games_idx.items():
        season_games.setdefault(season, []).extend(
            (kh, ka, g) for g in glist)
    school_idx = build_school_index(games_idx)

    def bump_reason(reason):
        d = integrity["unmatched_reasons"]
        d[reason] = d.get(reason, 0) + 1

    for (eid, season), (home, away, kickoff) in events.items():
        eh0, ea0 = norm_name(home), norm_name(away)
        eh, ea = alias_school(eh0), alias_school(ea0)
        if (eh, ea) != (eh0, ea0):
            integrity["alias_used"] += 1
        schools = school_idx.get(season, [])
        bh, ba = best_school(eh, schools), best_school(ea, schools)
        cands = []  # (kickoff_diff, game, swapped)
        seen = set()
        if bh is not None and ba is not None:
            for kh, ka, g in season_games.get(season, []):
                if bh == kh and ba == ka:
                    swapped = False
                elif bh == ka and ba == kh:
                    swapped = True
                else:
                    continue
                gid = g.get("id", id(g))
                if gid in seen:
                    continue
                seen.add(gid)
                try:
                    sd = parse_ts(g["startDate"])
                except Exception:
                    continue
                if abs(sd - kickoff) <= KICKOFF_TOLERANCE:
                    cands.append((abs(sd - kickoff), g, swapped))
        if not cands:
            bump_reason(classify_unmatched(
                bh, ba, season, kickoff, season_games))
            continue
        cands.sort(key=lambda c: c[0])
        if len(cands) > 1 and cands[0][0] == cands[1][0]:
            bump_reason("conflict")
            continue
        _, g, swapped = cands[0]
        if not g.get("completed"):
            bump_reason("not_completed")
            continue
        if g.get("homePoints") is None or g.get("awayPoints") is None:
            bump_reason("null_scores")
            continue
        matched[(eid, season)] = {"game": g, "swapped": swapped}
        integrity["matched"] += 1
        if swapped:
            integrity["swapped_matched"] += 1
    return matched, integrity


def classify_unmatched(bh, ba, season, kickoff, season_games):
    """Deterministic reason code for an event with no in-tolerance match.

    bh/ba are the resolved CFBD school names (or None).
    """
    if bh is None and ba is None:
        return "phantom"  # neither team matches any CFBD school
    for kh, ka, g in season_games.get(season, []):
        try:
            sd = parse_ts(g["startDate"])
        except Exception:
            continue
        if abs(sd - kickoff) > KICKOFF_TOLERANCE:
            continue
        if bh in (kh, ka) or ba in (kh, ka):
            # a CFBD game at a close kickoff shares a team, but the
            # pairing didn't match: provider team-name error or a
            # speculative listing (e.g. wrong championship matchup)
            return "team_mislabel"
    # teams resolve to real schools, but no game near this kickoff
    return "kickoff_miss"  # e.g. postponed beyond +-36h, or canceled


def matchup_class(g):
    hc = (g.get("homeClassification") or "").lower()
    ac = (g.get("awayClassification") or "").lower()
    if hc == "fbs" and ac == "fbs":
        return "FBSvFBS"
    # exactly one FBS and one FCS; FCS-vs-FCS (or anything else) is other
    if (hc == "fbs") != (ac == "fbs") and "fcs" in (hc, ac):
        return "FBSvFCS"
    return "other/unknown"


def conf_group(conf, classification):
    if (classification or "").lower() == "fcs":
        return "FCS"
    if conf in P4:
        return "P4"
    if conf:
        return "G5/other"
    return "unknown"


def bucket(v, buckets):
    a = abs(v)
    for lo, hi in buckets:
        if lo <= a < hi:
            return f"[{lo},{hi if hi != float('inf') else 'inf'})"
    return "unknown"


def summarize(rows):
    """rows: list of (p, y) with y in {0,1}. Returns rate/logloss/brier."""
    n = len(rows)
    if not n:
        return {"n": 0}
    rate = sum(y for _, y in rows) / n
    eps = 1e-9
    ll = -sum(y * math.log(min(max(p, eps), 1 - eps)) +
              (1 - y) * math.log(min(max(1 - p, eps), 1 - eps))
              for p, y in rows) / n
    brier = sum((p - y) ** 2 for p, y in rows) / n
    return {"n": n, "rate": round(rate, 4), "log_loss": round(ll, 4),
            "brier": round(brier, 4)}


def decile_bins(rows):
    out = []
    for i in range(10):
        lo, hi = i / 10, (i + 1) / 10
        sub = [(p, y) for p, y in rows if (lo <= p < hi) or (i == 9 and p == 1.0)]
        s = summarize(sub)
        out.append({"bin": f"[{lo:.1f},{hi:.1f})",
                    "mean_p": round(sum(p for p, _ in sub) / len(sub), 4)
                    if sub else None, **s})
    return out


def analyze(selected, matched, seasons):
    # consensus per (eid, season, window, market)
    cons = {}
    for (eid, season, week, window, book, market), v in selected.items():
        cons.setdefault((eid, season, window, market),
                        {"lines": [], "probs": []})
        c = cons[(eid, season, window, market)]
        c["lines"].append(v["line"])
        c["probs"].append(v["fair_prob"])

    rows = []  # one per (event, window, market) with outcome
    for (eid, season, window, market), c in cons.items():
        m = matched.get((eid, season))
        if m is None:
            continue
        g = m["game"]
        # Realign scores to the provider's orientation: the spread line is
        # quoted on the provider's home team, so the home margin must be
        # that team's margin even when CFBD lists home/away reversed
        # (neutral-site designation differences, swapped=True).
        if m["swapped"]:
            hp, ap = g["awayPoints"], g["homePoints"]
            hconf, hclass = g.get("awayConference"), g.get("awayClassification")
        else:
            hp, ap = g["homePoints"], g["awayPoints"]
            hconf, hclass = g.get("homeConference"), g.get("homeClassification")
        line = statistics.median(c["lines"])
        p = statistics.median(c["probs"])
        hm = hp - ap
        if market == "FULL_GAME_SPREAD":
            # Historical quotes store spread lines as |spread|
            # (backfill_history.py pairs by abs(line)). The prereg
            # specifies the HOME side's cover, so recover the signed home
            # line from the consensus home fair probability: a home
            # favorite (p > 0.5) lays points -> negative line.
            # Amendment A3 (2026-09-15).
            signed_line = -line if p > 0.5 else line
            diff = hm + signed_line
            if diff == 0:
                outcome, push = None, True
            else:
                outcome, push = (1 if diff > 0 else 0), False
            lb = bucket(line, SPREAD_BUCKETS)
        else:
            diff = (hp + ap) - line
            if diff == 0:
                outcome, push = None, True
            else:
                outcome, push = (1 if diff > 0 else 0), False
            lb = bucket(line, TOTAL_BUCKETS)
        rows.append({"eid": eid, "season": season, "window": window,
                     "market": market, "line": round(line, 2),
                     "p": round(p, 4), "outcome": outcome, "push": push,
                     "matchup": matchup_class(g),
                     "conf": conf_group(hconf, hclass),
                     "line_bucket": lb})

    played = [r for r in rows if not r["push"]]
    pushes = len(rows) - len(played)

    def split(keyfn):
        groups = {}
        for r in played:
            groups.setdefault(keyfn(r), []).append((r["p"], r["outcome"]))
        return {k: summarize(v) for k, v in sorted(groups.items())}

    cal = {}
    for w in ("early", "mid", "late"):
        for m in ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL"):
            sub = [(r["p"], r["outcome"]) for r in played
                   if r["window"] == w and r["market"] == m]
            cal[f"{m}/{w}"] = decile_bins(sub)

    eff = {}
    for m in ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL"):
        early = [(r["p"], r["outcome"]) for r in played
                 if r["market"] == m and r["window"] == "early"]
        late = [(r["p"], r["outcome"]) for r in played
                if r["market"] == m and r["window"] == "late"]
        # pair on (event, season): provider IDs can repeat across seasons
        eids_e = {(r["eid"], r["season"]) for r in played
                  if r["market"] == m and r["window"] == "early"}
        eids_l = {(r["eid"], r["season"]) for r in played
                  if r["market"] == m and r["window"] == "late"}
        common = eids_e & eids_l
        early_c = [(r["p"], r["outcome"]) for r in played
                   if r["market"] == m and r["window"] == "early"
                   and (r["eid"], r["season"]) in common]
        late_c = [(r["p"], r["outcome"]) for r in played
                  if r["market"] == m and r["window"] == "late"
                  and (r["eid"], r["season"]) in common]
        eff[m] = {"early": summarize(early), "late": summarize(late),
                  "paired_common_events": len(common),
                  "early_paired": summarize(early_c),
                  "late_paired": summarize(late_c)}

    return {
        "n_event_windows": len(rows),
        "n_played": len(played),
        "n_pushes": pushes,
        "calibration": cal,
        "by_season": split(lambda r: r["season"]),
        "by_window": split(lambda r: r["window"]),
        "by_matchup": split(lambda r: r["matchup"]),
        "by_conference": split(lambda r: r["conf"]),
        "by_line_bucket": split(
            lambda r: f"{r['market']}/{r['line_bucket']}"),
        "closing_efficiency": eff,
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--games", required=True,
                    help="CFBD /games JSON file(s), comma-separated "
                         "(years 2022-2024 only)")
    ap.add_argument("--out", default="/tmp/market_outcomes.json")
    args = ap.parse_args(argv)
    seasons = [int(s) for s in args.seasons.split(",")]

    games_idx = {}
    for gp in args.games.split(","):
        for key, glist in load_games(gp.strip()).items():
            games_idx.setdefault(key, []).extend(glist)
    plan = ms.build_plan(args.sport, seasons)
    expected_snapshots = len(plan)

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
        qrows = conn.execute(
            f"SELECT {cols} FROM public.{hq}").fetchall()
    recs = [dict(zip(
        ["provider_event_id", "home_team", "away_team", "kickoff",
         "book_key", "market", "selection", "line", "american_odds",
         "fair_probability", "observed_at"], r)) for r in qrows]

    selected, integrity, matched_instants = ms.select_pairs(recs, plan, seasons)
    freeze_ok = len(matched_instants) == expected_snapshots

    events = {}
    for (eid, season, week, window, book, market), v in selected.items():
        events.setdefault((eid, season),
                          (v["home"], v["away"], parse_ts(v["kickoff"])))
    matched, match_integrity = match_events(events, games_idx)
    integrity["matching"] = match_integrity

    result = {
        "preregistration": "docs/preregistrations/ncaaf-market-outcomes-f2.md",
        "dataset_fingerprint": FROZEN_FINGERPRINT,
        "scope": {"sport": args.sport, "seasons": seasons,
                  "markets": ["FULL_GAME_SPREAD", "FULL_GAME_TOTAL"],
                  "windows": ["early", "mid", "late"]},
        "freeze_check": {"matched_snapshots": len(matched_instants),
                         "expected_snapshots": expected_snapshots,
                         "pass": freeze_ok},
        "integrity": integrity,
        **analyze(selected, matched, seasons),
    }
    if not freeze_ok:
        print(f"FREEZE CHECK FAILED: {len(matched_instants)} != "
              f"{expected_snapshots}", file=sys.stderr)
        return 1
    match_rate = (match_integrity["matched"] /
                  max(1, match_integrity["events_total"]))
    if match_rate < 0.95:
        print(f"MATCH INTEGRITY FAILED: {match_rate:.3f} < 0.95",
              file=sys.stderr)
        print(f"matching detail: {json.dumps(match_integrity)}",
              file=sys.stderr)
        ev_items = list(events.items())[:5]
        print("sample events (home, away, kickoff):",
              [(h, a, str(k)) for (_, _), (h, a, k) in ev_items],
              file=sys.stderr)
        idx_keys = list(games_idx.keys())[:5]
        print("sample cfbd index keys:", idx_keys, file=sys.stderr)
        missing = sorted({(h, a) for (eid, s), (h, a, k)
                          in events.items()
                          if (eid, s) not in matched})
        print(f"unmatched teams ({len(missing)}):", missing[:150],
              file=sys.stderr)
        print(f"unmatched reasons: {json.dumps(match_integrity['unmatched_reasons'])}",
              file=sys.stderr)
        return 1
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"wrote {args.out}: {len(selected)} pairs, "
          f"{match_integrity['matched']}/{match_integrity['events_total']} "
          f"events matched, freeze {len(matched_instants)}/"
          f"{expected_snapshots}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
