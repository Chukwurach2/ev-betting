"""Daily shadow settlement: scores, results, and closing-line value.

Fetches completed NFL games from The Odds API scores endpoint, settles every
open shadow pick on those games (win/loss/push, with explicit push handling
on integer lines), and records CLV in probability points against the closing
consensus (median no-vig fair probability from the latest pre-kickoff quotes
at the pick's line).

Settlement is pure arithmetic on provider data: no model output, no wagers.
Runs once daily; a single scores request covers the recent slate.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import sys
import urllib.request

ENGINE_VERSION = "v1-settle"
SPORT = "americanfootball_nfl"
EPS = 1e-9


def settle_spread(selection_side: str, home_score: int, away_score: int,
                  line: float) -> str:
    """line is the selected side's own spread (negative = favorite)."""
    diff = (home_score - away_score) if selection_side == "home" else (away_score - home_score)
    adj = diff + line
    if abs(adj) < EPS:
        return "push"
    return "win" if adj > 0 else "loss"


def settle_total(selection: str, home_score: int, away_score: int,
                 line: float) -> str:
    total = home_score + away_score
    diff = total - line
    if abs(diff) < EPS:
        return "push"
    if selection == "Over":
        return "win" if diff > 0 else "loss"
    return "win" if diff < 0 else "loss"


def settle_pick(market: str, selection: str, line: float,
                home_name: str, away_name: str,
                home_score: int, away_score: int) -> str | None:
    """Returns win/loss/push, or None when the selection cannot be resolved."""
    if market == "FULL_GAME_SPREAD":
        if selection == home_name:
            side = "home"
        elif selection == away_name:
            side = "away"
        else:
            return None
        return settle_spread(side, home_score, away_score, line)
    if market == "FULL_GAME_TOTAL":
        if selection not in ("Over", "Under"):
            return None
        return settle_total(selection, home_score, away_score, line)
    return None


def fetch_scores(api_key: str, days_from: int = 3) -> list[dict]:
    url = (f"https://api.the-odds-api.com/v4/sports/{SPORT}/scores/"
           f"?daysFrom={days_from}&apiKey={api_key}")
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge-settle/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def closing_consensus(conn, provider_event_id: str, market: str,
                      selection: str, line: float) -> float | None:
    """Median no-vig fair prob across books from the latest pre-kickoff
    quotes AT THE PICK'S LINE. Line-aware: CLV is only meaningful when the
    closing consensus is measured on the identical line the pick was made at.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (book_key) fair_probability
        FROM public.nfl_edge_odds_quotes
        WHERE provider_event_id = %s AND market = %s AND selection = %s
          AND line = %s
          AND observed_at <= kickoff
        ORDER BY book_key, observed_at DESC
        """,
        (provider_event_id, market, selection, line),
    )
    probs = [float(r[0]) for r in cur.fetchall()]
    if len(probs) < 2:
        return None
    return statistics.median(probs)


def settle(conn, api_key: str) -> dict:
    events = fetch_scores(api_key)
    completed = {}
    for e in events:
        if not e.get("completed"):
            continue
        scores = {s.get("name"): s.get("score") for s in e.get("scores") or []}
        try:
            hs = int(scores[e["home_team"]])
            aws = int(scores[e["away_team"]])
        except (KeyError, TypeError, ValueError):
            continue
        completed[e["id"]] = {
            "home_name": e["home_team"], "away_name": e["away_team"],
            "home_score": hs, "away_score": aws,
        }

    cur = conn.cursor()
    cur.execute(
        """
        SELECT pick_id, provider_event_id, market, selection, line,
               consensus_fair_prob
        FROM public.nfl_edge_picks
        WHERE result IS NULL
        """)
    cols = [d[0] for d in cur.description]
    open_picks = [dict(zip(cols, r)) for r in cur.fetchall()]

    settled = skipped = 0
    for p in open_picks:
        ev = completed.get(p["provider_event_id"])
        if ev is None:
            continue
        result = settle_pick(p["market"], p["selection"], float(p["line"]),
                             ev["home_name"], ev["away_name"],
                             ev["home_score"], ev["away_score"])
        if result is None:
            skipped += 1
            continue
        close = closing_consensus(conn, p["provider_event_id"], p["market"],
                                  p["selection"], float(p["line"]))
        clv = (float(p["consensus_fair_prob"]) - close) if close is not None else None
        cur.execute(
            """
            UPDATE public.nfl_edge_picks
            SET result = %s, settled_at = now(),
                final_home_score = %s, final_away_score = %s,
                clv_prob_points = %s, settled_by = %s
            WHERE pick_id = %s AND result IS NULL
            """,
            (result, ev["home_score"], ev["away_score"], clv,
             ENGINE_VERSION, p["pick_id"]),
        )
        if cur.rowcount:
            settled += 1
    conn.commit()
    return {"engine": ENGINE_VERSION, "completed_games": len(completed),
            "settled": settled, "skipped_unresolved": skipped}


def main() -> int:
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not database or not api_key:
        raise ValueError("NFL_EDGE_DATABASE_URL and THE_ODDS_API_KEY are required")
    import psycopg
    conn = psycopg.connect(database)
    try:
        summary = settle(conn, api_key)
    finally:
        conn.close()
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
