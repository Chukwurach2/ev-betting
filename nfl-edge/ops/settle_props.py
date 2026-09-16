"""Free NFL prop settlement from nflverse weekly player stats.

For every (game, market, participant) captured in prop snapshots whose
kickoff is older than SETTLE_BUFFER_HOURS and which has no settlement row
yet, resolves the player's actual stat from the nflverse stats_player
release (free, no API credits) and inserts one immutable row into
nfl_prop_settlements:

    (canonical_game_id, market, participant_raw, participant_key,
     actual, line, residual, status, line_source)

- line   = Close-checkpoint consensus median_line when captured, else the
           latest captured checkpoint's consensus median_line (line_source
           records which checkpoint the line came from).
- status = 'push' when actual == line, else 'settled'.
- residual = actual - line.

Matching: provider participant string -> nflverse player via
nfl_prop_participant_aliases first (explicit entries only), else normalized
name match against player_display_name (fallback player_name), constrained
to the two teams of the game and the exact season/week. DNP/void detection
is OUT of scope for v1 (no inactive-list pipeline): a participant with no
matching nflverse row stays unsettled (no row inserted) and is logged.

The settlements table is insert-only (BEFORE UPDATE OR DELETE trigger), so
first settlement wins; reruns are idempotent via ON CONFLICT DO NOTHING.

Usage:  python ops/settle_props.py [--dry-run] [--live]
Default is --dry-run (zero writes). --live performs inserts.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import re
import sys
import urllib.request

SETTLE_BUFFER_HOURS = 6
STATS_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
             "stats_player/stats_player_week_{season}.csv")

# market -> nflverse stats_player column
MARKET_COLUMNS = {
    "player_pass_yds": "passing_yards",
    "player_pass_attempts": "attempts",
    "player_rush_attempts": "carries",
}

# checkpoint preference for the settlement line (Close first, then latest)
CHECKPOINT_ORDER = ["Close", "T-90m", "T-3", "T-6", "T-12", "T-24"]

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, drop generational suffixes."""
    s = name.lower().replace(".", "").replace("'", "").replace("-", " ")
    parts = [p for p in re.split(r"\s+", s.strip()) if p]
    while parts and parts[-1] in SUFFIXES:
        parts.pop()
    return " ".join(parts)


def parse_canonical_game_id(gid: str):
    """'2026_02_DET_BUF' -> (2026, 2, 'DET', 'BUF')."""
    season_s, week_s, away, home = gid.split("_")
    return int(season_s), int(week_s), away, home


def fetch_season_stats(season: int):
    """Returns list of dict rows, or None when the season file is absent."""
    url = STATS_URL.format(season=season)
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-settle/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            if resp.status != 200:
                return None
            return list(csv.DictReader(io.StringIO(resp.read().decode("utf-8"))))
    except Exception as e:  # 404, network error, ...
        print(f"  stats file unavailable for season {season}: {e}", flush=True)
        return None


def build_lookup(rows):
    """(season, week, normalized_name) -> list of rows (usually one)."""
    lookup: dict = {}
    for r in rows:
        try:
            key = (int(r["season"]), int(r["week"]),
                   normalize_name(r.get("player_display_name") or ""))
        except (KeyError, ValueError):
            continue
        lookup.setdefault(key, []).append(r)
        alt = normalize_name(r.get("player_name") or "")
        if alt and alt != key[2]:
            lookup.setdefault((key[0], key[1], alt), []).append(r)
    return lookup


def match_player(lookup, season, week, teams, norm_name):
    """Return the single stats row for this player, or None."""
    candidates = lookup.get((season, week, norm_name), [])
    in_game = [r for r in candidates if r.get("team") in teams]
    if len(in_game) == 1:
        return in_game[0]
    return None


def settle_status(actual: float, line: float) -> str:
    return "push" if abs(actual - line) < 1e-9 else "settled"


def pending_positions(conn):
    """(game, market, participant_raw) from completed games lacking settlements."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.canonical_game_id, q.market, q.participant_raw
            FROM public.nfl_prop_snapshots s
            JOIN public.nfl_prop_quotes q ON q.snapshot_key = s.snapshot_key
            WHERE s.kickoff < now() - (%s || ' hours')::interval
              AND NOT EXISTS (
                  SELECT 1 FROM public.nfl_prop_settlements st
                  WHERE st.canonical_game_id = s.canonical_game_id
                    AND st.market = q.market
                    AND st.participant_raw = q.participant_raw)
            GROUP BY s.canonical_game_id, q.market, q.participant_raw
            """,
            (str(SETTLE_BUFFER_HOURS),),
        )
        return cur.fetchall()


def load_aliases(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT provider_string, participant_key "
                    "FROM public.nfl_prop_participant_aliases")
        return dict(cur.fetchall())


def consensus_line(conn, game_id, market, participant_raw):
    """(median_line, line_source) preferring Close, else latest checkpoint."""
    with conn.cursor() as cur:
        order = " ".join(
            f"WHEN '{c}' THEN {i}" for i, c in enumerate(CHECKPOINT_ORDER))
        cur.execute(
            f"""
            SELECT c.median_line, s.checkpoint_name
            FROM public.nfl_prop_consensus c
            JOIN public.nfl_prop_snapshots s ON s.snapshot_key = c.snapshot_key
            WHERE s.canonical_game_id = %s AND c.market = %s
              AND c.participant_raw = %s AND c.median_line IS NOT NULL
            ORDER BY CASE s.checkpoint_name {order} ELSE 99 END
            LIMIT 1
            """,
            (game_id, market, participant_raw),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return float(row[0]), f"{row[1]}_consensus"


def insert_settlement(conn, row):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.nfl_prop_settlements
                (canonical_game_id, market, participant_raw, participant_key,
                 actual, line, residual, status, line_source)
            VALUES (%(canonical_game_id)s, %(market)s, %(participant_raw)s,
                    %(participant_key)s, %(actual)s, %(line)s,
                    %(residual)s, %(status)s, %(line_source)s)
            ON CONFLICT DO NOTHING
            """,
            row,
        )
        return cur.rowcount


def run(live: bool) -> int:
    import psycopg  # noqa: E402

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    counts = {"settled": 0, "skipped": 0}
    with psycopg.connect(dsn) as conn:
        pending = pending_positions(conn)
        print(f"pending positions: {len(pending)} (dry_run={not live})", flush=True)
        if not pending:
            return 0
        aliases = load_aliases(conn)
        seasons = {}
        for game_id, market, participant_raw in pending:
            try:
                season, week, away, home = parse_canonical_game_id(game_id)
            except ValueError:
                print(f"  SKIP {game_id}: unparseable canonical id")
                counts["skipped"] += 1
                continue
            column = MARKET_COLUMNS.get(market)
            if not column:
                print(f"  SKIP {game_id} {market}: unknown market")
                counts["skipped"] += 1
                continue
            if season not in seasons:
                rows = fetch_season_stats(season)
                seasons[season] = build_lookup(rows) if rows else None
            lookup = seasons[season]
            if lookup is None:
                print(f"  SKIP {game_id}: no stats file for season {season}")
                counts["skipped"] += 1
                continue
            norm = normalize_name(aliases.get(participant_raw, participant_raw))
            prow = match_player(lookup, season, week, {away, home}, norm)
            if prow is None:
                print(f"  SKIP {game_id} {market} {participant_raw}: "
                      f"no nflverse player row")
                counts["skipped"] += 1
                continue
            try:
                actual = float(prow[column])
            except (KeyError, TypeError, ValueError):
                print(f"  SKIP {game_id} {market} {participant_raw}: "
                      f"no stat column {column}")
                counts["skipped"] += 1
                continue
            line, line_source = consensus_line(conn, game_id, market,
                                              participant_raw)
            if line is None:
                print(f"  SKIP {game_id} {market} {participant_raw}: "
                      f"no consensus line")
                counts["skipped"] += 1
                continue
            status = settle_status(actual, line)
            key = normalize_name(prow.get("player_display_name")
                                 or prow.get("player_name") or participant_raw)
            srow = {
                "canonical_game_id": game_id,
                "market": market,
                "participant_raw": participant_raw,
                "participant_key": key,
                "actual": actual,
                "line": line,
                "residual": actual - line,
                "status": status,
                "line_source": line_source,
            }
            if live:
                n = insert_settlement(conn, srow)
                conn.commit()
                counts["settled"] += n
                if n:
                    print(f"  SETTLED {game_id} {market} {participant_raw}: "
                          f"actual={actual} line={line} -> {status}")
            else:
                counts["settled"] += 1
                print(f"  WOULD SETTLE {game_id} {market} {participant_raw}: "
                      f"actual={actual} line={line} -> {status}")
    print(f"done: settled={counts['settled']} skipped={counts['skipped']}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--live", action="store_true",
                    help="perform inserts (default is dry-run)")
    args = ap.parse_args(argv)
    return run(live=args.live)


if __name__ == "__main__":
    sys.exit(main())
