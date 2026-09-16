#!/usr/bin/env python3
"""Canonical game identity: nflverse <-> Odds API (NFL).

Builds a deterministic cross-source identity layer with explicit states:
- MATCHED: exactly one nflverse game matches one Odds API event
- AMBIGUOUS: multiple candidates (requires manual review, never auto-resolved)
- UNMATCHED_ODDS: Odds API event with no nflverse counterpart (with reason)
- UNMATCHED_NFLVERSE: nflverse game with no Odds API counterpart
- MISSING_FROM_SOURCE: game known absent from the bundled nflverse schedules
  (explicit KNOWN_MISSING_FROM_SOURCE registry below). Preserved so it can
  never silently drop out of a downstream join: consumers that enumerate
  games from the bundle must treat this list as known-absent, never
  synthesizing or inventing the game.

No silent fuzzy matching. All name resolutions are explicit in
nfl_team_aliases.json.

This is shared infrastructure for: injuries, rosters, officiating,
play-by-play-derived features, and eventually weather.

Read-only on source tables. Writes mapping JSON to --out (uploaded as a
CI artifact). The relational schema for loading this mapping lives in
migrations/017_nfl_game_identity.sql.
"""
import os, sys, json, argparse
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
NFLVERSE_PATH = HERE / "nflverse_schedules_2022_2024.json"
ALIASES_PATH = HERE / "nfl_team_aliases.json"

# Games known to be absent from the bundled nflverse schedules.
#
# The bundle holds 815 of the 816 scheduled 2022-2024 regular-season games.
# The missing game is the 2022 Week 17 Buffalo Bills @ Cincinnati Bengals
# game scheduled for 2023-01-03: suspended in the first quarter after Damar
# Hamlin's cardiac arrest and never completed or resumed. nflverse's schedule
# data excludes it (no completed game, no official result or stats), and it
# is also absent from nflverse schedules generally, so the bundle can never
# contain it by construction.
#
# It is recorded here explicitly (state "missing_from_source") so it cannot
# silently drop out of any join that enumerates games from the bundle:
# consumers must treat these entries as known-absent, never synthesize or
# invent the game, and never pad a season count to 272.
#
# Linked provider-side evidence: the Odds API carries an event for this
# game, which appears under unmatched_odds with reason "no_match"
# (Bengals home vs Bills, commence 2023-01-03).
KNOWN_MISSING_FROM_SOURCE = [
    {
        "season": 2022,
        "week": 17,
        "game_type": "REG",
        "home": "CIN",
        "away": "BUF",
        "gameday": "2023-01-03",
        "nflverse_game_id": None,
        "state": "missing_from_source",
        "reason": (
            "Scheduled 2023-01-03 Bills @ Bengals (2022 Week 17), suspended in "
            "Q1 and never completed or resumed; excluded from nflverse "
            "schedules. No official result or stats. Provider-side Odds event "
            "exists (see unmatched_odds, no_match, 2023-01-03 CIN home vs BUF)."
        ),
    },
]

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402


def load_aliases():
    """Load explicit Odds API -> nflverse team abbreviation aliases.

    32 entries, one per NFL franchise. Deterministic and fixed;
    verified against the bundled nflverse schedules (which use 'LA'
    for the Rams, not 'LAR'). No silent fuzzy matching.
    """
    if ALIASES_PATH.exists():
        with open(ALIASES_PATH) as f:
            return json.load(f)
    return {}


def normalize_nflverse(abbr):
    """nflverse side: already abbreviations; uppercase + strip only."""
    return (abbr or "").upper().strip()


def normalize_odds(name, aliases):
    """Odds API side: resolve via explicit alias table, then normalize.

    Returns (abbr, aliased: bool). abbr is "" when the name is absent
    from the explicit alias table (caller records reason 'no_alias').
    """
    if not name:
        return "", False
    key = name.strip()
    if key in aliases:
        return aliases[key], True
    # Explicit-only: no guessing from the name itself.
    return "", False


def match_events(odds_events, nv_games, aliases, seasons):
    """Deterministic match of Odds events to nflverse games.

    Pure function (no I/O): returns the results dict with
    matched / ambiguous / unmatched_odds / unmatched_nflverse /
    missing_from_source lists.
    """
    # Build nflverse index: (gameday, home_abbr, away_abbr) -> [games]
    nv_index = defaultdict(list)
    for g in nv_games:
        dk = g.get("gameday", "")
        home = normalize_nflverse(g.get("home_team"))
        away = normalize_nflverse(g.get("away_team"))
        nv_index[(dk, home, away)].append(g)

    results = {
        "matched": [],
        "ambiguous": [],
        "unmatched_odds": [],
        "unmatched_nflverse": [],
        # Explicit missing-from-source registry entries: games known absent
        # from the bundled nflverse schedules. Emitted verbatim so joins can
        # never silently drop them.
        "missing_from_source": [
            dict(entry) for entry in KNOWN_MISSING_FROM_SOURCE
        ],
    }
    matched_nv_ids = set()

    def lookup(d, h, a):
        return nv_index.get((d, h, a), [])

    for oe in odds_events:
        kickoff = oe.get("kickoff")
        if hasattr(kickoff, "strftime"):
            date_key = kickoff.strftime("%Y-%m-%d")
            ko_date = kickoff.date()
            adj_dates = [
                (ko_date - timedelta(days=1)).isoformat(),
                (ko_date + timedelta(days=1)).isoformat(),
            ]
            season_year = ko_date.year
        else:
            date_key = str(kickoff)[:10] if kickoff else ""
            adj_dates = []
            season_year = None

        home, home_ok = normalize_odds(oe.get("home", ""), aliases)
        away, away_ok = normalize_odds(oe.get("away", ""), aliases)

        # Out-of-scope season: record and skip matching.
        if season_year not in seasons:
            results["unmatched_odds"].append({
                "provider_event_id": oe["event_id"],
                "date": date_key,
                "home": oe.get("home"),
                "away": oe.get("away"),
                "reason": "outside_seasons",
            })
            continue

        # Missing alias: explicit reason, never guessed.
        if not (home_ok and away_ok):
            missing = []
            if not home_ok:
                missing.append(oe.get("home"))
            if not away_ok:
                missing.append(oe.get("away"))
            results["unmatched_odds"].append({
                "provider_event_id": oe["event_id"],
                "date": date_key,
                "home": oe.get("home"),
                "away": oe.get("away"),
                "reason": "no_alias",
                "missing_aliases": missing,
            })
            continue

        # Deterministic attempts in fixed order:
        # 1. exact UTC date, 2. exact UTC date swapped,
        # 3. -1/+1 day fallback (evening US games land on next UTC day),
        # 4. swapped on fallback dates.
        candidates = []
        match_date = date_key
        date_shift = 0
        swapped = False

        c = lookup(date_key, home, away)
        if c:
            candidates, match_date, date_shift, swapped = c, date_key, 0, False
        else:
            c = lookup(date_key, away, home)
            if c:
                candidates, match_date, date_shift, swapped = c, date_key, 0, True
            else:
                for i, adj in enumerate(adj_dates):
                    c = lookup(adj, home, away)
                    if c:
                        candidates = c
                        match_date = adj
                        date_shift = -1 if i == 0 else 1
                        swapped = False
                        break
                    c = lookup(adj, away, home)
                    if c:
                        candidates = c
                        match_date = adj
                        date_shift = -1 if i == 0 else 1
                        swapped = True
                        break

        if len(candidates) == 1:
            g = candidates[0]
            if date_shift != 0 and swapped:
                mtype = "swapped_date_shift"
            elif date_shift != 0:
                mtype = "alias_date_shift"
            elif swapped:
                mtype = "swapped"
            else:
                mtype = "alias_date"
            results["matched"].append({
                "nflverse_game_id": g["game_id"],
                "provider_event_id": oe["event_id"],
                "season": g["season"],
                "week": g.get("week"),
                "game_type": g.get("game_type"),
                "gameday": match_date,
                "odds_date": date_key,
                "date_shift": date_shift,
                "odds_home": oe.get("home"),
                "odds_away": oe.get("away"),
                "nflverse_home": g.get("home_team"),
                "nflverse_away": g.get("away_team"),
                "match_type": mtype,
            })
            matched_nv_ids.add(g["game_id"])
        elif len(candidates) > 1:
            results["ambiguous"].append({
                "provider_event_id": oe["event_id"],
                "candidates": [
                    {"nflverse_game_id": g["game_id"],
                     "gameday": g.get("gameday"),
                     "home": g.get("home_team"),
                     "away": g.get("away_team")}
                    for g in candidates
                ],
            })
        else:
            results["unmatched_odds"].append({
                "provider_event_id": oe["event_id"],
                "date": date_key,
                "home": oe.get("home"),
                "away": oe.get("away"),
                "reason": "no_match",
            })

    # Unmatched nflverse games (REG + postseason; preseason not in bundle)
    for g in nv_games:
        if g["game_id"] not in matched_nv_ids:
            results["unmatched_nflverse"].append({
                "nflverse_game_id": g["game_id"],
                "season": g["season"],
                "week": g.get("week"),
                "game_type": g.get("game_type"),
                "gameday": g.get("gameday"),
                "home": g.get("home_team"),
                "away": g.get("away_team"),
            })

    return results


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="nfl")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/nfl_canonical_identity.json")
    ap.add_argument("--nflverse-file", default=None,
                    help="Path to nflverse schedules JSON (for local testing)")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",")]

    aliases = load_aliases()
    print(f"Loaded {len(aliases)} NFL team aliases", file=sys.stderr)

    # Load nflverse schedules
    bundled = str(NFLVERSE_PATH) if NFLVERSE_PATH.exists() else None
    nv_source = args.nflverse_file or bundled
    if not nv_source:
        raise SystemExit("nflverse schedules JSON not found")
    print(f"Loading nflverse from {nv_source}...", file=sys.stderr)
    with open(nv_source) as f:
        nv_games = json.load(f)
    nv_games = [g for g in nv_games if g.get("season") in seasons]
    print(f"nflverse: {len(nv_games)} games (seasons {seasons})", file=sys.stderr)

    # Load Odds API events from market_history JSON payloads.
    # Envelope structure (same pipeline as NCAAF):
    # {"timestamp": ..., "data": [{id, home_team, away_team, commence_time, ...}]}
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NFL_EDGE_DATABASE_URL is required")
    mh = sports.market_history_table(args.sport)
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"""
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
        """).fetchall()
        odds_events = [
            {"event_id": eid, "home": home, "away": away, "kickoff": kickoff}
            for (eid, home, away, kickoff) in rows
        ]
    print(f"Odds API events: {len(odds_events)}", file=sys.stderr)

    results = match_events(odds_events, nv_games, aliases, seasons)

    n_odds = len(odds_events)
    n_scope = n_odds - sum(
        1 for u in results["unmatched_odds"] if u["reason"] == "outside_seasons")
    n_matched = len(results["matched"])
    stats = {
        "n_odds_events": n_odds,
        "n_odds_events_in_scope": n_scope,
        "n_nflverse_games": len(nv_games),
        "n_matched": n_matched,
        "n_ambiguous": len(results["ambiguous"]),
        "n_unmatched_odds": len(results["unmatched_odds"]),
        "n_unmatched_nflverse": len(results["unmatched_nflverse"]),
        "n_missing_from_source": len(results["missing_from_source"]),
        "match_rate": (n_matched / n_scope) if n_scope else 0,
        "match_rate_by_type": {
            mt: sum(1 for m in results["matched"] if m["match_type"] == mt)
            for mt in ("alias_date", "alias_date_shift", "swapped",
                       "swapped_date_shift")
        },
        "unmatched_odds_by_reason": {
            r: sum(1 for u in results["unmatched_odds"] if u["reason"] == r)
            for r in ("outside_seasons", "no_alias", "no_match")
        },
    }

    output = {
        "methodology": (
            "Deterministic matching on (date, home_abbr, away_abbr). Odds API "
            "full team names resolve ONLY via the explicit alias table "
            "(nfl_team_aliases.json); names absent from the table are recorded "
            "as reason 'no_alias', never guessed. nflverse side is already "
            "abbreviated (uppercase/strip). Date: exact UTC date first, then "
            "deterministic -1/+1 day fallback for US-evening games landing on "
            "the next UTC day; swapped home/away tried as a separate attempt "
            "at each step. No fuzzy matching. Ambiguous matches (>1 candidate) "
            "require manual review."
        ),
        "normalization_rules": [
            "Odds API: exact key lookup in nfl_team_aliases.json (32 entries); "
            "absent names -> unmatched with reason 'no_alias'",
            "nflverse: uppercase + strip only (source of truth abbreviations)",
            "Date: kickoff::date (UTC) first; fallback ko-1d, ko+1d",
            "NOTE: nflverse uses 'LA' for the Rams (verified in bundled data)",
            "NOTE: abbreviations compared verbatim; no substring/stripping heuristics",
            "NOTE: bundle holds 815 of 816 scheduled 2022-2024 REG games; the "
            "missing 2022 Week 17 BUF@CIN suspended game is carried explicitly "
            "in results['missing_from_source'] with state 'missing_from_source' "
            "— never synthesized, never padded",
        ],
        "stats": stats,
        "results": results,
    }

    with open(args.out, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
