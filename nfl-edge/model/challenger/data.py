"""nflverse game data loader. Cached locally; no API key needed."""
import csv
import datetime as dt
import gzip
import os
import urllib.request

GAMES_URL = ("https://github.com/nflverse/nflverse-data/releases"
             "/download/schedules/games.csv.gz")
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
CACHE_PATH = os.path.join(CACHE_DIR, "games.csv.gz")


def fetch(force=False):
    """Download games.csv.gz to the local cache. Returns cache path."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    if not force and os.path.exists(CACHE_PATH):
        return CACHE_PATH
    req = urllib.request.Request(GAMES_URL, headers={"User-Agent": "nfl-edge/1.0"})
    with urllib.request.urlopen(req, timeout=180) as r, open(CACHE_PATH, "wb") as f:
        f.write(r.read())
    return CACHE_PATH


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_games(cache_path=None):
    """Parse cached games into canonical dicts, chronological order.

    Only games with final scores are returned. spread_line convention:
    positive => home favored. total_line is the closing total.
    """
    path = cache_path or CACHE_PATH
    games = []
    with gzip.open(path, "rt") as f:
        for r in csv.DictReader(f):
            hs, aws = _num(r.get("home_score")), _num(r.get("away_score"))
            if hs is None or aws is None:
                continue
            try:
                day = dt.date.fromisoformat(r["gameday"][:10])
            except (TypeError, ValueError):
                continue
            games.append({
                "season": int(r["season"]),
                "date": day,
                "home": r["home_team"],
                "away": r["away_team"],
                "home_score": hs,
                "away_score": aws,
                "spread_line": _num(r.get("spread_line")),
                "total_line": _num(r.get("total_line")),
            })
    games.sort(key=lambda g: (g["season"], g["date"]))
    return games
