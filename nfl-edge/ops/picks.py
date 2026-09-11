"""Shadow picks engine v1: cross-book consensus edge detection.

For every upcoming game/market/selection/LINE with fresh quotes from at least
two books, the engine compares each book's offered odds against the consensus
no-vig fair probability (median across books at that identical line).
Grouping is line-aware: different lines are different bets and never share a
consensus calculation, even for the same selection. A positive edge above
MIN_EDGE emits a SHADOW pick with fractional-Kelly staking. An empty pick set
is a valid output: the engine never forces picks.

Nothing here is a production wager. mode is hard-wired to 'shadow'; any
attempt to write another mode raises. Promotion to challenger/production
requires the validation evidence described in model/production_gate.py.

The challenger model (model/challenger) annotates each pick with its own
fair probability and predicted margin/total for research. It never emits
picks itself: the gate has not promoted it, so annotation is informational
only and must never fail the engine.
"""
from __future__ import annotations

import hashlib
import os
import statistics
import sys

ENGINE_VERSION = "v1-consensus"
MODE = "shadow"

# Full team name (Odds API selection) -> canonical abbreviation.
FULL_TO_ABBR = {
    'Arizona Cardinals': 'AZ', 'Atlanta Falcons': 'ATL', 'Baltimore Ravens': 'BAL',
    'Buffalo Bills': 'BUF', 'Carolina Panthers': 'CAR', 'Chicago Bears': 'CHI',
    'Cincinnati Bengals': 'CIN', 'Cleveland Browns': 'CLE', 'Dallas Cowboys': 'DAL',
    'Denver Broncos': 'DEN', 'Detroit Lions': 'DET', 'Green Bay Packers': 'GB',
    'Houston Texans': 'HOU', 'Indianapolis Colts': 'IND', 'Jacksonville Jaguars': 'JAX',
    'Kansas City Chiefs': 'KC', 'Las Vegas Raiders': 'LV', 'Los Angeles Chargers': 'LAC',
    'Los Angeles Rams': 'LAR', 'Miami Dolphins': 'MIA', 'Minnesota Vikings': 'MIN',
    'New England Patriots': 'NE', 'New Orleans Saints': 'NO', 'New York Giants': 'NYG',
    'New York Jets': 'NYJ', 'Philadelphia Eagles': 'PHI', 'Pittsburgh Steelers': 'PIT',
    'San Francisco 49ers': 'SF', 'Seattle Seahawks': 'SEA', 'Tampa Bay Buccaneers': 'TB',
    'Tennessee Titans': 'TEN', 'Washington Commanders': 'WAS',
}


def load_challenger():
    """Load the challenger artifact, or return None (annotation skipped)."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "..", "model"))
        from challenger.infer import load_challenger as _load
        return _load()
    except Exception as exc:  # never break the engine for a research signal
        print(f"challenger unavailable ({exc}); annotating picks without it")
        return None


def annotate_challenger(challenger, cache, pick: dict) -> dict:
    """Fill challenger_* fields. Pure annotation; never raises."""
    fields = {"challenger_version": None, "challenger_fair_prob": None,
              "challenger_pred_margin": None, "challenger_pred_total": None}
    if challenger is None:
        return fields
    try:
        home, away = pick["home_team"], pick["away_team"]
        key = (home, away)
        if key not in cache:
            cache[key] = challenger.predict(home, away)
        pred = cache[key]
        if pred is None:
            return fields
        fields["challenger_version"] = challenger.version
        fields["challenger_pred_margin"] = round(pred["pred_margin_home"], 2)
        fields["challenger_pred_total"] = round(pred["pred_total"], 2)
        market, selection = pick["market"], pick["selection"]
        line = float(pick["line"])
        if market == "FULL_GAME_TOTAL" and selection in ("Over", "Under"):
            p = challenger.cover_probability(home, away, "totals", selection, line)
        elif market == "FULL_GAME_SPREAD":
            abbr = FULL_TO_ABBR.get(selection)
            if abbr is None:
                return fields
            side = "home" if abbr == home else "away"
            # DB line is Odds API convention (negative => selection favored);
            # convert to nflverse home-perspective (positive => home favored).
            home_line = -line if side == "home" else line
            p = challenger.cover_probability(home, away, "spreads", side,
                                             home_line)
        else:
            return fields
        fields["challenger_fair_prob"] = round(p, 6) if p is not None else None
    except Exception:
        pass
    return fields

ENGINE_VERSION = "v1-consensus"
MODE = "shadow"

MIN_EDGE = 0.02            # minimum expected value to emit a pick
KELLY_DIVISOR = 4          # quarter-Kelly
MAX_STAKE_UNITS = 1.0      # cap per pick, bankroll = 100 units
FRESHNESS_MINUTES = 30     # quotes older than this are ignored
MIN_CONSENSUS_BOOKS = 2    # books required for a consensus


def american_to_decimal(odds: int) -> float:
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def consensus_prob(fair_probs: list[float]) -> float:
    return statistics.median(fair_probs)


def edge_for_quote(decimal_odds: float, consensus: float) -> float:
    return consensus * decimal_odds - 1.0


def kelly_fraction(edge: float, decimal_odds: float) -> float:
    if edge <= 0 or decimal_odds <= 1:
        return 0.0
    return max(0.0, (edge / (decimal_odds - 1.0)) / KELLY_DIVISOR)


def stake_units(kelly_frac: float) -> float:
    return round(min(kelly_frac * 100.0, MAX_STAKE_UNITS), 4)


def pick_id_for(engine: str, event_id: str, market: str, selection: str,
                line: str, book_key: str, observed_at: str) -> str:
    raw = "|".join([engine, event_id, market, selection, line, book_key,
                    observed_at])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def line_key(line) -> str:
    """Canonical line string so grouping and pick IDs are line-aware.

    Consensus is only meaningful over identical lines: a -3.5 and a -4.5
    are different bets and must never share a consensus calculation.
    """
    return "%g" % float(line)


def assert_shadow(mode: str) -> None:
    if mode != "shadow":
        raise ValueError(
            f"Refusing to write picks with mode={mode!r}: no model has passed "
            "the production gate. See model/production_gate.py.")


LATEST_QUOTES_SQL = """
SELECT DISTINCT ON (q.provider_event_id, q.market, q.selection, q.book_key)
  q.provider_event_id, q.home_team, q.away_team, q.kickoff, q.market,
  q.selection, q.line, q.sportsbook, q.book_key, q.american_odds,
  q.fair_probability, q.observed_at, g.game_id
FROM public.nfl_edge_odds_quotes q
LEFT JOIN public.games g
  ON g.home_team = q.home_team AND g.away_team = q.away_team
 AND g.kickoff = q.kickoff
WHERE q.observed_at > now() - make_interval(mins => %s)
  AND q.kickoff > now()
ORDER BY q.provider_event_id, q.market, q.selection, q.book_key, q.observed_at DESC
"""


def build_picks(conn) -> list[dict]:
    assert_shadow(MODE)
    cur = conn.cursor()
    cur.execute(LATEST_QUOTES_SQL, (FRESHNESS_MINUTES,))
    cols = [d[0] for d in cur.description]
    quotes = [dict(zip(cols, row)) for row in cur.fetchall()]

    challenger = load_challenger()
    cache: dict = {}

    groups: dict[tuple, list[dict]] = {}
    for q in quotes:
        # Line-aware grouping: consensus is only valid over identical lines.
        key = (q["provider_event_id"], q["market"], q["selection"],
               line_key(q["line"]))
        groups.setdefault(key, []).append(q)

    picks: list[dict] = []
    for (event_id, market, selection, linek), qs in groups.items():
        if len({q["book_key"] for q in qs}) < MIN_CONSENSUS_BOOKS:
            continue
        consensus = consensus_prob([float(q["fair_probability"]) for q in qs])
        for q in qs:
            dec = american_to_decimal(int(q["american_odds"]))
            edge = edge_for_quote(dec, consensus)
            if edge < MIN_EDGE:
                continue
            kf = kelly_fraction(edge, dec)
            pick = {
                "pick_id": pick_id_for(ENGINE_VERSION, event_id, market,
                                       selection, linek, q["book_key"],
                                       q["observed_at"].isoformat()),
                "engine_version": ENGINE_VERSION,
                "mode": MODE,
                "game_id": q["game_id"],
                "provider_event_id": event_id,
                "home_team": q["home_team"],
                "away_team": q["away_team"],
                "kickoff": q["kickoff"],
                "market": market,
                "selection": selection,
                "line": q["line"],
                "sportsbook": q["sportsbook"],
                "book_key": q["book_key"],
                "american_odds": int(q["american_odds"]),
                "decimal_odds": round(dec, 4),
                "consensus_fair_prob": round(consensus, 6),
                "edge": round(edge, 6),
                "kelly_fraction": round(kf, 6),
                "stake_units": stake_units(kf),
                "consensus_books": len({x["book_key"] for x in qs}),
                "observed_at": q["observed_at"],
            }
            pick.update(annotate_challenger(challenger, cache, pick))
            picks.append(pick)
    picks.sort(key=lambda p: p["edge"], reverse=True)
    return picks


INSERT_PICK_SQL = """
INSERT INTO public.nfl_edge_picks (
  pick_id, engine_version, mode, game_id, provider_event_id, home_team,
  away_team, kickoff, market, selection, line, sportsbook, book_key,
  american_odds, decimal_odds, consensus_fair_prob, edge, kelly_fraction,
  stake_units, consensus_books, observed_at,
  challenger_version, challenger_fair_prob, challenger_pred_margin,
  challenger_pred_total
) VALUES (
  %(pick_id)s, %(engine_version)s, %(mode)s, %(game_id)s, %(provider_event_id)s,
  %(home_team)s, %(away_team)s, %(kickoff)s, %(market)s, %(selection)s,
  %(line)s, %(sportsbook)s, %(book_key)s, %(american_odds)s, %(decimal_odds)s,
  %(consensus_fair_prob)s, %(edge)s, %(kelly_fraction)s, %(stake_units)s,
  %(consensus_books)s, %(observed_at)s,
  %(challenger_version)s, %(challenger_fair_prob)s, %(challenger_pred_margin)s,
  %(challenger_pred_total)s
) ON CONFLICT (pick_id) DO NOTHING
"""


def write_picks(conn, picks: list[dict]) -> int:
    assert_shadow(MODE)
    cur = conn.cursor()
    written = 0
    for p in picks:
        cur.execute(INSERT_PICK_SQL, p)
        written += cur.rowcount
    conn.commit()
    return written


def main() -> int:
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        raise ValueError("NFL_EDGE_DATABASE_URL is required")
    import psycopg
    conn = psycopg.connect(database)
    try:
        picks = build_picks(conn)
        written = write_picks(conn, picks)
    finally:
        conn.close()
    print(f"engine={ENGINE_VERSION} mode={MODE} picks={len(picks)} written={written}")
    for p in picks[:10]:
        print(f"  {p['away_team']} @ {p['home_team']} {p['market']} {p['selection']} "
              f"{p['line']} {p['book_key']} {p['american_odds']:+d} edge={p['edge']:.3f} "
              f"stake={p['stake_units']}u")
    return 0


if __name__ == "__main__":
    sys.exit(main())
