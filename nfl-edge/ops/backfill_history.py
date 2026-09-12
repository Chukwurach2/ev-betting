"""Historical odds backfill: paid-tier training data for market-only-v1.

Fetches The Odds API historical snapshots (5-minute granularity since Sep
2022) for past NFL regular-season weeks and stores:
  * raw snapshot envelopes -> public.nfl_edge_market_history
  * normalized two-sided quotes -> public.nfl_edge_historical_quotes

Snapshot plan per week: Wednesday 12:00 UTC (early week), Saturday 12:00
UTC (late week), Sunday 15:30 UTC (~90 min before the 1pm ET slate; the
"close"). One bulk call covers the whole slate.

Credit math: historical calls cost 10 x markets x regions. The default
(spreads,totals x us,eu) = 40 credits/snapshot; 3 snapshots x 18 weeks x
3 seasons = 162 calls ~= 6,500 credits, well inside the 20K tier.

Safety:
  * Requires a PAID tier: the historical endpoints 403 on free plans with
    HISTORICAL_UNAVAILABLE_ON_FREE_USAGE_PLAN. Fails fast with a clear
    message instead of burning the run.
  * 429 (rate limit) is a hard stop: the script exits immediately and does
    not retry in a loop.
  * Idempotent: INSERT ... ON CONFLICT DO NOTHING on both tables; a
    snapshot within 30 minutes of a requested time is skipped.
  * Timestamp integrity: the provider's returned timestamp must be present
    and within 1 hour of the requested time, else the run fails loudly
    instead of silently relabeling mis-timed data.
  * Raw immutability: the FULL provider response envelope (timestamp,
    previous_timestamp, next_timestamp, data) is persisted in payload;
    requested_at and provider_snapshot_id record the canonical requested
    time (the provider exposes no separate snapshot id).
  * Stops early if remaining credits drop below --min-remaining (default
    1000) or total spend exceeds --max-credits.

Usage (GitHub Actions workflow backfill-history.yml, or locally):
  python ops/backfill_history.py --seasons 2022,2023,2024
  python ops/backfill_history.py --seasons 2024 --weeks 1-4 --dry-run

Env: NFL_EDGE_DATABASE_URL, THE_ODDS_API_KEY.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from research.devig import devig_multiplicative  # tournament-selected de-vig

BASE = "https://api.the-odds-api.com/v4"
SPORT = "americanfootball_nfl"

# Week-1 Sunday (regular season) per season. 2022+ only: American odds in
# historical snapshots are reliable from 2022-09-18.
WEEK1_SUNDAY = {
    2022: dt.date(2022, 9, 11),
    2023: dt.date(2023, 9, 10),
    2024: dt.date(2024, 9, 8),
}

NY_BOOK_KEYS = {'draftkings', 'fanduel', 'betmgm', 'betrivers', 'espnbet',
                'williamhill_us', 'fanatics'}

# Maximum allowed |provider timestamp - requested timestamp|. Historical
# snapshots are ~5-minute granularity; anything beyond an hour means the
# provider returned data for the wrong time and we must not label it with
# the requested time. Fail loudly instead of silently relabeling.
TIMESTAMP_TOLERANCE_SECONDS = 3600


def _parse_ts(value):
    try:
        t = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc)


def validated_snapshot_time(envelope, requested):
    """Return the provider's snapshot timestamp, or raise.

    Refuses to store a snapshot when the provider timestamp is missing or
    materially differs from the requested historical time: silently
    relabeling mis-timed data would corrupt point-in-time integrity.
    """
    returned = _parse_ts(envelope.get("timestamp"))
    if returned is None:
        raise RuntimeError(
            "refusing to store snapshot: provider timestamp missing for "
            "requested %s" % requested.isoformat())
    drift = abs((returned - requested).total_seconds())
    if drift > TIMESTAMP_TOLERANCE_SECONDS:
        raise RuntimeError(
            "refusing to store snapshot: provider timestamp %s differs from "
            "requested %s by %.0fs (> %ds tolerance)"
            % (returned.isoformat(), requested.isoformat(), drift,
               TIMESTAMP_TOLERANCE_SECONDS))
    return returned


class TierError(RuntimeError):
    """Historical endpoints need a paid Odds API tier."""


class RateLimitError(RuntimeError):
    """Provider rate limit hit: stop, do not retry in a loop."""


def snapshot_times(season, week):
    """The three weekly snapshot instants (aware UTC datetimes).

    Wednesday 12:00 UTC (early-week price), Saturday 12:00 UTC
    (late-week price), Sunday 15:30 UTC (~90 min before the 1pm ET slate:
    the close proxy).
    """
    sunday = WEEK1_SUNDAY[season] + dt.timedelta(days=7 * (week - 1))
    wed = sunday - dt.timedelta(days=4)
    sat = sunday - dt.timedelta(days=1)
    utc = dt.timezone.utc
    return [
        dt.datetime(wed.year, wed.month, wed.day, 12, 0, tzinfo=utc),
        dt.datetime(sat.year, sat.month, sat.day, 12, 0, tzinfo=utc),
        dt.datetime(sunday.year, sunday.month, sunday.day, 15, 30,
                    tzinfo=utc),
    ]


def parse_weeks(spec):
    """'1-18' -> [1..18]; '5' -> [5]."""
    spec = spec.strip()
    if "-" in spec:
        lo, hi = spec.split("-", 1)
        return list(range(int(lo), int(hi) + 1))
    return [int(spec)]


def implied(odds):
    odds = float(odds)
    return 100.0 / (100.0 + odds) if odds > 0 else abs(odds) / (100.0 + abs(odds))


# API market key -> stored FULL_GAME_* market value(s), mirroring
# normalize_snapshot. Used by the skip-incomplete check.
_MARKET_VALUES = {
    "spreads": ("FULL_GAME_SPREAD",),
    "totals": ("FULL_GAME_TOTAL",),
    "h2h": ("FULL_GAME_MONEYLINE",),
}


def _req(path, params, api_key, timeout=30):
    import requests
    try:
        r = requests.get(BASE + path, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError("Odds provider request failed: %s" % type(e).__name__)
    if r.status_code == 429:
        raise RateLimitError("HTTP 429: provider rate limit hit; stopping.")
    if r.status_code in (401, 403):
        try:
            body = r.json()
        except ValueError:
            body = {}
        msg = str(body.get("message", ""))
        if "HISTORICAL_UNAVAILABLE_ON_FREE_USAGE_PLAN" in msg:
            raise TierError(
                "Historical odds need a PAID Odds API tier "
                "(free plan returns 403 HISTORICAL_UNAVAILABLE_ON_FREE_USAGE_PLAN). "
                "Upgrade at the-odds-api.com, then re-run.")
        raise RuntimeError("Odds provider HTTP %d: %s" % (r.status_code, msg[:120]))
    if not r.ok:
        raise RuntimeError("Odds provider HTTP %d" % r.status_code)
    return r.json(), {k: r.headers.get(k) for k in
                       ("x-requests-remaining", "x-requests-used",
                        "x-requests-last")}


def fetch_snapshot(when, regions, markets, api_key):
    """Fetch one historical snapshot envelope. Returns (envelope, headers).

    The historical odds endpoint returns a timestamped envelope:
    {"timestamp", "previous_timestamp", "next_timestamp", "data": [events]}.

    NOTE: this must be the /historical/ path. The live /sports/{sport}/odds
    endpoint ignores the `date` param and returns a bare list of CURRENT
    events -- calling that here would silently mislabel live odds as
    historical snapshots (2026-09-12: caught in the smoke test before any
    bad rows were stored).
    """
    payload, headers = _req(
        "/historical/sports/%s/odds" % SPORT,
        {"apiKey": api_key,
         "date": when.isoformat().replace("+00:00", "Z"),
         "regions": regions, "markets": markets,
         "oddsFormat": "american", "dateFormat": "iso"},
        api_key)
    return payload, headers


def _num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v


def snapshot_books(envelope):
    """Sorted unique book keys in a snapshot (audit trail: books present)."""
    data = envelope.get("data") if isinstance(envelope, dict) else envelope
    books = set()
    for game in data or []:
        for b in game.get("bookmakers") or []:
            if b.get("key"):
                books.add(b["key"])
    return sorted(books)


def normalize_snapshot(envelope, regions, markets):
    """Flatten a snapshot envelope into historical-quote dicts.

    Two-sided pairing per (event, book, market, line) with no-vig fair
    probabilities, mirroring the live pipeline's pair_quotes. observed_at
    is the snapshot's actual timestamp (point-in-time safe: the API never
    returns a snapshot later than requested).
    """
    snap_at = envelope.get("timestamp")
    quotes = []
    for game in envelope.get("data") or []:
        event_id = game.get("id")
        home, away = game.get("home_team"), game.get("away_team")
        kickoff = game.get("commence_time")
        if not (event_id and home and away and kickoff and snap_at):
            continue
        for book in game.get("bookmakers") or []:
            book_key = book.get("key")
            title = book.get("title") or book_key
            if not book_key:
                continue
            for offered in book.get("markets") or []:
                key = offered.get("key")
                if key == "spreads":
                    market = "FULL_GAME_SPREAD"
                elif key == "totals":
                    market = "FULL_GAME_TOTAL"
                elif key == "h2h":
                    market = "FULL_GAME_MONEYLINE"
                else:
                    continue
                sides = []
                for o in offered.get("outcomes") or []:
                    price = _num(o.get("price"))
                    line = _num(o.get("point"))
                    name = o.get("name")
                    if market == "FULL_GAME_MONEYLINE":
                        # Moneyline outcomes carry no point; line stored as 0.
                        if price is None or not name:
                            continue
                        line = 0.0
                    elif price is None or line is None or not name:
                        continue
                    if abs(price) < 100:
                        continue
                    sides.append((name, line, int(price)))
                # Pair by line: spreads pair team lines that are opposites,
                # totals pair Over/Under on the same line, moneyline pairs
                # the two team outcomes at line 0.
                if market == "FULL_GAME_MONEYLINE":
                    if {s[0] for s in sides} == {home, away} and len(sides) == 2:
                        pairs = [sides]
                    else:
                        pairs = []
                    pair_lines = [(0.0, v) for v in pairs]
                elif market == "FULL_GAME_SPREAD":
                    by_line = {}
                    for name, line, price in sides:
                        by_line.setdefault(round(abs(line), 3), []).append(
                            (name, line, price))
                    pairs = [v for v in by_line.values() if len(v) == 2]
                    pair_lines = [(abs(v[0][1]), v) for v in pairs]
                else:
                    by_line = {}
                    for name, line, price in sides:
                        if name not in ("Over", "Under"):
                            continue
                        by_line.setdefault(round(line, 3), []).append(
                            (name, line, price))
                    pairs = [v for v in by_line.values()
                             if {s[0] for s in v} == {"Over", "Under"}]
                    pair_lines = [(v[0][1], v) for v in pairs]
                for line, pair in pair_lines:
                    # Tournament-selected de-vig (devig-tournament-v1):
                    # multiplicative, q_i = p_i / R.
                    fair = devig_multiplicative(implied(pair[0][2]),
                                                implied(pair[1][2]))
                    if fair is None:
                        continue
                    for (name, _, price), fq in zip(pair, fair):
                        qid = hashlib.sha256(
                            ("%s|%s|%s|%s|%s|%s|%s" % (
                                snap_at, event_id, book_key, market, name,
                                line, price)).encode()).hexdigest()
                        quotes.append({
                            "quote_id": qid,
                            "provider_event_id": event_id,
                            "home_team": home, "away_team": away,
                            "kickoff": kickoff,
                            "sportsbook": title, "book_key": book_key,
                            "market": market, "selection": name,
                            "line": line, "american_odds": price,
                            "fair_probability": fq,
                            "observed_at": snap_at,
                            "ny_licensed": book_key in NY_BOOK_KEYS,
                        })
    return quotes


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--weeks", default="1-18")
    ap.add_argument("--regions", default="us,eu")
    ap.add_argument("--markets", default="spreads,totals")
    ap.add_argument("--min-remaining", type=int, default=1000)
    ap.add_argument("--max-credits", type=int, default=15000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--recapture", action="store_true",
                    help="delete legacy-format snapshots (payload without a "
                         "full provider envelope) within 30 min of each "
                         "planned requested time, with their quotes, before "
                         "fetching. Used once to replace pre-change smoke "
                         "rows; logged explicitly.")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",") if s.strip()]
    for s in seasons:
        if s not in WEEK1_SUNDAY:
            raise SystemExit("unsupported season %d (known: %s)" %
                             (s, sorted(WEEK1_SUNDAY)))
    weeks = parse_weeks(args.weeks)

    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not api_key and not args.dry_run:
        raise SystemExit("THE_ODDS_API_KEY is required")

    plan = [(s, w, t) for s in seasons for w in weeks
            for t in snapshot_times(s, w)]
    print("plan: %d snapshots (%d seasons x %d weeks x 3)" %
          (len(plan), len(seasons), len(weeks)))
    est = len(plan) * 10 * len(args.markets.split(",")) * len(args.regions.split(","))
    print("estimated max credits: %d (10 x markets x regions per snapshot)" % est)
    if args.dry_run:
        for s, w, t in plan[:6]:
            print("  dry-run season=%d week=%d at=%s" % (s, w, t.isoformat()))
        if len(plan) > 6:
            print("  ... +%d more" % (len(plan) - 6))
        return 0

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NFL_EDGE_DATABASE_URL is required")
    conn = psycopg.connect(dsn, connect_timeout=10)
    conn.autocommit = True

    spent = 0
    stored_snaps = stored_quotes = skipped = 0
    try:
        for season, week, when in plan:
            if args.recapture:
                # Replace legacy rows (data-only payload, stored before the
                # full-envelope change) so the frozen dataset has one format.
                doomed = conn.execute(
                    """SELECT snapshot_at FROM public.nfl_edge_market_history
                       WHERE regions=%s AND markets=%s
                         AND abs(extract(epoch from (snapshot_at - %s))) < 1800
                         AND payload->>'timestamp' IS NULL""",
                    (args.regions, args.markets, when)).fetchall()
                for (old_snap,) in doomed:
                    qn = conn.execute(
                        """DELETE FROM public.nfl_edge_historical_quotes
                           WHERE observed_at = %s""", (old_snap,)).rowcount
                    conn.execute(
                        """DELETE FROM public.nfl_edge_market_history
                           WHERE snapshot_at = %s AND regions=%s AND markets=%s""",
                        (old_snap, args.regions, args.markets))
                    print("recaptured legacy snapshot %s (deleted %d quotes)"
                          % (old_snap, qn), flush=True)
            # Skip snapshots already captured (within 30 min of request).
            # A receipt WITHOUT any stored quotes for the requested markets
            # is an incomplete capture (e.g. the 2026-09-12 moneyline pilot
            # hit a CHECK violation mid-insert; the frozen spread/total
            # quotes at the same snapshot_at must NOT satisfy this) and is
            # redone rather than skipped.
            expected_markets = []
            for _m in args.markets.split(","):
                expected_markets.extend(_MARKET_VALUES.get(_m.strip(), ()))
            row = conn.execute(
                """SELECT mh.snapshot_at FROM public.nfl_edge_market_history mh
                   WHERE mh.regions=%s AND mh.markets=%s
                     AND abs(extract(epoch from (mh.snapshot_at - %s))) < 1800
                     AND EXISTS (SELECT 1 FROM public.nfl_edge_historical_quotes q
                                 WHERE q.observed_at = mh.snapshot_at
                                   AND q.market = ANY(%s))""",
                (args.regions, args.markets, when, expected_markets)).fetchone()
            if row:
                skipped += 1
                continue
            print("fetching season=%d week=%d at=%s" %
                  (season, week, when.isoformat()), flush=True)
            envelope, headers = fetch_snapshot(when, args.regions,
                                               args.markets, api_key)
            last = headers.get("x-requests-last")
            remaining = headers.get("x-requests-remaining")
            try:
                spent += int(last) if last else 0
            except (TypeError, ValueError):
                pass
            snap_at = validated_snapshot_time(envelope, when)
            books = snapshot_books(envelope)
            conn.execute(
                """INSERT INTO public.nfl_edge_market_history
                   (snapshot_at, regions, markets, requested_at,
                    provider_snapshot_id, books_present, payload, credits_used)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                (snap_at, args.regions, args.markets, when,
                 when.isoformat(), books,
                 json.dumps(envelope),
                 int(last) if last and str(last).isdigit() else None))
            stored_snaps += 1
            quotes = normalize_snapshot(envelope, args.regions, args.markets)
            for q in quotes:
                conn.execute(
                    """INSERT INTO public.nfl_edge_historical_quotes
                       (quote_id, provider_event_id, home_team, away_team, kickoff,
                        sportsbook, book_key, market, selection, line, american_odds,
                        fair_probability, observed_at, ny_licensed)
                       VALUES (%(quote_id)s,%(provider_event_id)s,%(home_team)s,%(away_team)s,
                               %(kickoff)s,%(sportsbook)s,%(book_key)s,%(market)s,%(selection)s,
                               %(line)s,%(american_odds)s,%(fair_probability)s,%(observed_at)s,
                               %(ny_licensed)s)
                       ON CONFLICT (quote_id) DO NOTHING""", q)
                stored_quotes += 1
            print("stored season=%d week=%d snapshot=%s quotes=%d books=%d credits_last=%s remaining=%s" %
                  (season, week, snap_at, len(quotes), len(books), last, remaining))
            try:
                rem = int(remaining) if remaining else None
            except (TypeError, ValueError):
                rem = None
            if rem is not None and rem < args.min_remaining:
                print("stopping: remaining credits %d below --min-remaining %d" %
                      (rem, args.min_remaining))
                break
            if spent >= args.max_credits:
                print("stopping: spent %d credits reached --max-credits" % spent)
                break
    finally:
        conn.close()
    print("done: snapshots=%d quotes=%d skipped=%d credits_spent~%d" %
          (stored_snaps, stored_quotes, skipped, spent))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
