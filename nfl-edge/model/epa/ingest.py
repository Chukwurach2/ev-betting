"""Fetch nflverse play-by-play into the local cache.

Idempotent: seasons already on disk are skipped. CSV-gzip is preferred here
because this environment has no parquet engine (no pyarrow/fastparquet);
parquet assets are byte-identical upstream and can be used where an engine
exists. Data files are never committed (see .cache/).
"""
import os
import urllib.request

PBP_RELEASE = "https://github.com/nflverse/nflverse-data/releases/download/pbp"
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")


def _has_parquet_engine():
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import fastparquet  # noqa: F401
        return True
    except ImportError:
        return False


def asset_name(season, fmt):
    return "play_by_play_%d.%s" % (season, fmt)


def fetch_pbp(seasons, cache_dir=None):
    """Download play-by-play for each season. Returns list of local paths.

    Skips seasons already cached. Raises URLError on download failure;
    already-downloaded seasons are still returned.
    """
    cache_dir = cache_dir or CACHE_DIR
    os.makedirs(cache_dir, exist_ok=True)
    fmt = "parquet" if _has_parquet_engine() else "csv.gz"
    paths = []
    for season in seasons:
        name = asset_name(season, fmt)
        path = os.path.join(cache_dir, name)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            paths.append(path)
            continue
        url = "%s/%s" % (PBP_RELEASE, name)
        req = urllib.request.Request(url, headers={"User-Agent": "nfl-edge/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=300) as r, \
                    open(path, "wb") as f:
                f.write(r.read())
        except Exception:
            if os.path.exists(path):
                os.remove(path)
            raise
        paths.append(path)
    return paths


def cache_size_mb(cache_dir=None):
    """Total MB currently cached."""
    cache_dir = cache_dir or CACHE_DIR
    total = 0
    if os.path.isdir(cache_dir):
        for f in os.listdir(cache_dir):
            p = os.path.join(cache_dir, f)
            if os.path.isfile(p):
                total += os.path.getsize(p)
    return round(total / 1e6, 1)
