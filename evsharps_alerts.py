#!/usr/bin/env python3
"""
EVSharps +EV Alert Bot (NBA)

Key behavior:
- Poll EVSharps NBA endpoint
- Skip placeholders
- Require sharp confirmation + 3+ books
- Optional weighted pricing (fallback to API EV/fair)
- Zone thresholds
- Optional fair-vs-market gap filter
- NY-only bet placement alerts
- Telegram delivery (optional) with console fallback
- Daily de-dupe cache and per-scan heartbeat
"""

import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from storage import append_alert_candidate
except Exception:
    append_alert_candidate = None

API_URL = "https://api-production-3a3b.up.railway.app/api/nba"
TOKEN_CACHE_FILE = Path(__file__).resolve().parent / ".evsharps_tokens.json"

# Rule thresholds
PRIMARY_MIN_ODDS = 105
PRIMARY_MAX_ODDS = 165
PRIMARY_MIN_EV = 0.06

EXT_MIN_ODDS = 165
EXT_MAX_ODDS = 250
EXT_MIN_EV = 0.10

AVOID_ODDS = 250
AVOID_MIN_EV = 0.12

EV_THRESHOLD_FLOOR = float(os.getenv("EV_THRESHOLD", "0.06"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"

USE_WEIGHTED_DEVIG = os.getenv("USE_WEIGHTED_DEVIG", "0") == "1"
DEBUG_SCAN = os.getenv("DEBUG_SCAN", "0") == "1"
SHOW_NEAR_MISS = os.getenv("SHOW_NEAR_MISS", "1") == "1"

ENABLE_GAP_FILTER = os.getenv("ENABLE_GAP_FILTER", "1") == "1"
GAP_PRIMARY = int(os.getenv("GAP_PRIMARY", "5"))
GAP_EXTENDED = int(os.getenv("GAP_EXTENDED", "12"))
GAP_HIGH = int(os.getenv("GAP_HIGH", "20"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_DISABLE = os.getenv("TELEGRAM_DISABLE", "0") == "1"

ENV_ACCESS_TOKEN = os.getenv("EVSHARPS_ACCESS_TOKEN", "").strip() or os.getenv("EVSHARPS_BEARER", "").strip()
ENV_REFRESH_TOKEN = os.getenv("EVSHARPS_REFRESH_TOKEN", "").strip()
ENV_EXPIRES_AT = os.getenv("EVSHARPS_EXPIRES_AT", "").strip()
ENV_SUPABASE_TOKEN_URL = os.getenv("EVSHARPS_SUPABASE_TOKEN_URL", "").strip()

DEVIG_DESC = os.getenv(
    "DEVIG_DESC",
    "Additive devig | Sharp-weighted pricing: pn 0.50, circa 0.25, bol 0.20, dk 0.03, fd 0.02",
)
AUTH_REFRESH_FAIL_MSG = "EV bot auth refresh failed — manual intervention may be required"

# Weighted pricing inputs
DEVIG_WEIGHTS: Dict[str, float] = {
    "pn": 0.50,
    "circa": 0.25,
    "bol": 0.20,
    "dk": 0.03,
    "fd": 0.02,
}

NY_ALLOWED_BOOKS = {"dk", "fd", "mgm", "cz", "espn", "br", "fn"}

BOOK_DISPLAY_MAP = {
    "dk": "DraftKings",
    "fd": "FanDuel",
    "mgm": "BetMGM",
    "cz": "Caesars",
    "czr": "Caesars",
    "espn": "ESPN BET (theScore)",
    "br": "BetRivers",
    "fn": "Fanatics",
    "hr": "Hard Rock (NOT_NY)",
    "bv": "Bovada (NOT_NY)",
    "bol": "BetOnline (NOT_NY)",
    "pn": "Pinnacle (NOT_NY)",
    "circa": "Circa (NOT_NY)",
    "b365": "Bet365 (NOT_NY)",
    "kambi": "Kambi (NOT_NY)",
    "fl": "Fliff (NOT_NY)",
}

PROP_MAP = {
    "pts": "Points",
    "reb": "Rebounds",
    "ast": "Assists",
    "pra": "PRA",
    "pr": "P+R",
    "pa": "P+A",
    "ra": "R+A",
    "3pm": "3PT Made",
    "fg3m": "3PT Made",
    "blk": "Blocks",
    "stl": "Steals",
    "to": "Turnovers",
}

CACHE_FILE = Path(__file__).resolve().parent / "alerted_cache.json"


def _parse_expires_at(value: Any) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    try:
        return int(float(raw))
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


def load_tokens() -> Dict[str, Any]:
    if TOKEN_CACHE_FILE.exists():
        try:
            data = json.loads(TOKEN_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                tokens = {
                    "access_token": str(data.get("access_token") or "").strip(),
                    "refresh_token": str(data.get("refresh_token") or "").strip(),
                    "expires_at": _parse_expires_at(data.get("expires_at")),
                    "token_type": str(data.get("token_type") or "bearer").strip() or "bearer",
                    "supabase_token_url": str(data.get("supabase_token_url") or "").strip(),
                }
                # Allow env to fill gaps without overriding valid cache values.
                if not tokens["supabase_token_url"] and ENV_SUPABASE_TOKEN_URL:
                    tokens["supabase_token_url"] = ENV_SUPABASE_TOKEN_URL
                return tokens
        except Exception:
            pass

    tokens = {
        "access_token": ENV_ACCESS_TOKEN,
        "refresh_token": ENV_REFRESH_TOKEN,
        "expires_at": _parse_expires_at(ENV_EXPIRES_AT),
        "token_type": "bearer",
        "supabase_token_url": ENV_SUPABASE_TOKEN_URL,
    }
    save_tokens(tokens)
    return tokens


def save_tokens(tokens: Dict[str, Any]) -> None:
    payload = {
        "access_token": str(tokens.get("access_token") or "").strip(),
        "refresh_token": str(tokens.get("refresh_token") or "").strip(),
        "expires_at": _parse_expires_at(tokens.get("expires_at")),
        "token_type": str(tokens.get("token_type") or "bearer").strip() or "bearer",
        "supabase_token_url": str(tokens.get("supabase_token_url") or "").strip(),
    }
    try:
        TOKEN_CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # Never crash service if token cache write fails.
        pass


def access_token_expired(tokens: Dict[str, Any], skew_seconds: int = 60) -> bool:
    access = str(tokens.get("access_token") or "").strip()
    if not access:
        return True
    expires_at = _parse_expires_at(tokens.get("expires_at"))
    if expires_at is None:
        return False
    now_ts = int(time.time())
    return now_ts >= (int(expires_at) - int(skew_seconds))


def refresh_access_token(tokens: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    refresh_token = str(tokens.get("refresh_token") or "").strip()
    token_url = str(tokens.get("supabase_token_url") or ENV_SUPABASE_TOKEN_URL).strip()
    if not refresh_token or not token_url:
        return None

    try:
        resp = requests.post(token_url, json={"refresh_token": refresh_token}, timeout=20)
        if not resp.ok:
            return None
        data = resp.json() if resp.content else {}
    except Exception:
        return None

    new_access = str(data.get("access_token") or "").strip()
    if not new_access:
        return None

    new_refresh = str(data.get("refresh_token") or refresh_token).strip()
    expires_in_raw = data.get("expires_in")
    try:
        expires_in = int(float(expires_in_raw))
    except Exception:
        expires_in = None
    expires_at = int(time.time()) + expires_in if expires_in is not None else None

    refreshed = {
        "access_token": new_access,
        "refresh_token": new_refresh,
        "expires_at": expires_at,
        "token_type": str(data.get("token_type") or "bearer").strip() or "bearer",
        "supabase_token_url": token_url,
    }
    save_tokens(refreshed)
    return refreshed


def book_code(code: Any) -> str:
    return str(code or "").strip().lower()


def normalize_book(code: Any) -> str:
    c = book_code(code)
    return BOOK_DISPLAY_MAP.get(c, c or "Unknown")


def format_ny_book_name(code: Any) -> str:
    c = book_code(code)
    return BOOK_DISPLAY_MAP.get(c, c or "Unknown")


def today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def load_alert_cache() -> Dict[str, Any]:
    try:
        if not CACHE_FILE.exists():
            return {"date": today_iso(), "keys": []}
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"date": today_iso(), "keys": []}
        if data.get("date") != today_iso():
            return {"date": today_iso(), "keys": []}
        keys = data.get("keys", [])
        if not isinstance(keys, list):
            keys = []
        return {"date": today_iso(), "keys": [str(k) for k in keys]}
    except Exception:
        return {"date": today_iso(), "keys": []}


def save_alert_cache(cache: Dict[str, Any]) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # do not crash alert loop on cache-write failures
        pass


def americanize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    if s.startswith(("+", "-")):
        return s
    try:
        v = int(float(s))
        return f"+{v}" if v > 0 else str(v)
    except Exception:
        return s


def to_int_odds(x: Any) -> Optional[int]:
    s = str(x or "").strip()
    if not s:
        return None
    if s.startswith("+"):
        s = s[1:]
    try:
        return int(float(s))
    except Exception:
        return None


def to_ev_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        return v / 100.0 if v > 1 else v
    s = str(x).strip()
    if not s:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1].strip()) / 100.0
        except Exception:
            return None
    try:
        v = float(s)
        return v / 100.0 if v > 1 else v
    except Exception:
        return None


def implied_prob_from_american(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return (-odds) / ((-odds) + 100.0)


def true_prob_from_american_fair(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return (-odds) / ((-odds) + 100.0)


def american_from_prob(p: float) -> Optional[int]:
    if p <= 0 or p >= 1:
        return None
    if p < 0.5:
        return int(round((100.0 / p) - 100.0))
    return int(round(-(100.0 * p) / (1.0 - p)))


def parse_two_sided_odds(v: Any) -> Optional[Tuple[int, int]]:
    if v is None:
        return None
    s = str(v).strip()
    if "/" not in s:
        return None
    left, right = s.split("/", 1)
    try:
        over = int(float(left.replace("+", "").strip()))
        under = int(float(right.strip()))
        return over, under
    except Exception:
        return None


def side_market_odds(v: Any, want_under: bool) -> Optional[int]:
    parsed = parse_two_sided_odds(v)
    if parsed:
        over_odds, under_odds = parsed
        return under_odds if want_under else over_odds
    return to_int_odds(v)


def ev_from_prob_and_american(p_true: float, odds: int) -> float:
    if odds == 0:
        return 0.0
    if odds > 0:
        profit = odds / 100.0
    else:
        profit = 100.0 / abs(odds)
    return p_true * profit - (1.0 - p_true)


def zone_for_play(odds_int: int, ev: float) -> Optional[str]:
    if PRIMARY_MIN_ODDS <= odds_int <= PRIMARY_MAX_ODDS and ev >= PRIMARY_MIN_EV:
        return "PRIMARY"
    if EXT_MIN_ODDS <= odds_int <= EXT_MAX_ODDS and ev >= EXT_MIN_EV:
        return "EXTENDED"
    if odds_int > AVOID_ODDS and ev >= AVOID_MIN_EV:
        return "HIGH_ODDS_OK"
    return None


def zone_label(zone: str) -> str:
    return "HIGH" if zone == "HIGH_ODDS_OK" else zone


def is_placeholder_pick(p: Dict[str, Any]) -> bool:
    ev_val = to_ev_float(p.get("ev"))
    ev_is_zero = (ev_val is not None) and (abs(ev_val) < 1e-12)

    fair_raw = p.get("fairVal")
    line_raw = p.get("line")
    implied_raw = p.get("implied")

    fair_str = str(fair_raw).strip()
    line_str = str(line_raw).strip()
    implied_str = str(implied_raw).strip()

    fair_is_100 = fair_str in {"100", "+100"}
    line_is_100 = line_str in {"100", "+100"}
    implied_is_50pct = implied_str == "50%"

    try:
        if float(fair_raw) == 100.0:
            fair_is_100 = True
    except Exception:
        pass
    try:
        if float(line_raw) == 100.0:
            line_is_100 = True
    except Exception:
        pass

    return ev_is_zero and (fair_is_100 or line_is_100 or implied_is_50pct)


def sharp_confirmation_ok(p: Dict[str, Any]) -> Tuple[bool, str]:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict):
        return False, "books"

    books_present = {book_code(k) for k in book_odds.keys() if str(k).strip()}
    if len(books_present) < 3:
        return False, "books"

    # Tier 1: pn or circa
    if "pn" in books_present or "circa" in books_present:
        return True, ""

    # Tier 2: bol + (dk or fd)
    if "bol" in books_present and ("dk" in books_present or "fd" in books_present):
        return True, ""

    return False, "sharp_confirmation"


def weighted_devig_fair_prob(pick: Dict[str, Any]) -> Optional[float]:
    """
    Robust one-sided weighted probability:
    - Uses whatever side odds are available from weighted books.
    - Converts each side odd to implied probability.
    - Weighted average across available books with configured weights.
    """
    book_odds = pick.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return None

    want_under = bool(pick.get("under", False))
    weighted_probs: List[Tuple[float, float]] = []

    for bk, raw in book_odds.items():
        bk_norm = book_code(bk)
        w = DEVIG_WEIGHTS.get(bk_norm, 0.0)
        if w <= 0:
            continue

        side_odds = side_market_odds(raw, want_under)
        if side_odds is None:
            continue

        p_imp = implied_prob_from_american(side_odds)
        weighted_probs.append((p_imp, w))

    if not weighted_probs:
        return None

    wsum = sum(w for _, w in weighted_probs)
    if wsum <= 0:
        return None

    return sum(p * (w / wsum) for p, w in weighted_probs)


def market_consensus_odds(p: Dict[str, Any]) -> Optional[int]:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return None

    want_under = bool(p.get("under", False))
    vals: List[int] = []
    for v in book_odds.values():
        side = side_market_odds(v, want_under)
        if side is not None:
            vals.append(side)
    if not vals:
        return None
    return int(round(median(vals)))


def fair_market_gap_ok(zone: str, fair_odds: int, market_odds: int) -> Tuple[bool, int, int]:
    gap_cents = market_odds - fair_odds
    if zone == "PRIMARY":
        min_gap = GAP_PRIMARY
    elif zone == "EXTENDED":
        min_gap = GAP_EXTENDED
    else:
        min_gap = GAP_HIGH
    return gap_cents >= min_gap, gap_cents, min_gap


def fetch_payload(tokens: Dict[str, Any]) -> Tuple[Any, Dict[str, Any]]:
    access_token = str(tokens.get("access_token") or "").strip()
    if not access_token:
        raise requests.HTTPError("Missing access token")

    headers = {
        "accept": "*/*",
        "origin": "https://www.evsharps.com",
        "referer": "https://www.evsharps.com/",
        "authorization": f"Bearer {access_token}",
        "user-agent": "Mozilla/5.0",
    }
    r = requests.get(API_URL, headers=headers, timeout=30)
    if r.status_code in {401, 403}:
        refreshed = refresh_access_token(tokens)
        if not refreshed:
            r.raise_for_status()
        retry_headers = dict(headers)
        retry_headers["authorization"] = f"Bearer {refreshed['access_token']}"
        r2 = requests.get(API_URL, headers=retry_headers, timeout=30)
        r2.raise_for_status()
        return r2.json(), refreshed

    r.raise_for_status()
    return r.json(), tokens


def extract_picks(payload: Any) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            has_core = all(k in x for k in ("player", "prop", "book")) and ("ev" in x or "fairVal" in x)
            if has_core:
                picks.append(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(payload)
    return picks


def build_market_string(p: Dict[str, Any]) -> str:
    prop = str(p.get("prop") or "").strip().lower()
    prop_name = PROP_MAP.get(prop, prop.upper() if prop else "")
    handicap = str(p.get("handicap") or "").strip()
    side = "u" if bool(p.get("under", False)) else "o"
    return f"{prop_name} {side}{handicap}" if handicap else prop_name


def devig_against_string(p: Dict[str, Any]) -> str:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return ""
    parts = []
    for bk, odd in book_odds.items():
        parts.append(f"{normalize_book(bk)} {americanize(odd)}")
    return " | ".join(parts)


def stable_key(p: Dict[str, Any]) -> str:
    player = str(p.get("player") or "").strip().lower()
    prop = str(p.get("prop") or "").strip().lower()
    side = "u" if bool(p.get("under", False)) else "o"
    handicap = str(p.get("handicap") or "").strip()
    book = book_code(p.get("book"))
    odds = str(p.get("line") or "").strip()
    sig = "|".join([player, prop, side, handicap, book, odds])
    return hashlib.sha1(sig.encode("utf-8")).hexdigest()


def persist_alert_candidate(
    p: Dict[str, Any],
    alert_id: str,
    zone: str,
    ev_used: float,
    ev_source: str,
    fair_used: str,
    market_odds: Optional[int],
    gap_cents: Optional[int],
) -> None:
    if append_alert_candidate is None:
        return

    row = {
        "alert_id": alert_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "player": p.get("player"),
        "prop": p.get("prop"),
        "handicap": p.get("handicap"),
        "under": bool(p.get("under", False)),
        "market_display": build_market_string(p),
        "game": p.get("game"),
        "dt": p.get("dt"),
        "recommended_book_code": book_code(p.get("book")),
        "recommended_book_name": format_ny_book_name(p.get("book")),
        "recommended_odds": to_int_odds(p.get("line")),
        "fair_odds": to_int_odds(fair_used) if fair_used is not None else None,
        "market_odds": market_odds,
        "ev_pct": round(ev_used * 100.0, 4),
        "gap_cents": gap_cents,
        "zone": zone,
        "ev_source": ev_source,
        "devig_summary": DEVIG_DESC,
        "sharp_confirmation_summary": "3+ books and [pn/circa or bol+dk/fd]",
        "is_logged": False,
        "logged_at": "",
    }

    try:
        append_alert_candidate(row)
    except Exception:
        # Never break alerting loop on storage failures.
        pass


def format_pick(
    p: Dict[str, Any],
    zone: str,
    ev_used: float,
    fair_used: str,
    ev_source: str,
    market_odds: Optional[int],
    gap_cents: Optional[int],
    place_on: str,
) -> str:
    player = str(p.get("player") or "").strip()
    market_str = build_market_string(p)
    odds = americanize(p.get("line"))
    book_name = normalize_book(p.get("book"))
    dt_raw = str(p.get("dt") or "").strip() or "N/A"
    game_raw = str(p.get("game") or "").strip() or "N/A"

    ev_str = f"{ev_used * 100:.1f}%"
    devig_str = devig_against_string(p)

    msg_lines = [
        f"+EV ALERT ({zone_label(zone)})",
        f"{player} | {market_str}",
        f"DATE: {dt_raw}",
        f"GAME: {game_raw}",
        f"PLACE ON: {place_on}",
        f"Book: {book_name} {odds} | Fair: {fair_used}",
    ]

    if market_odds is not None:
        msg_lines.append(f"Market: {americanize(market_odds)}")
    if gap_cents is not None:
        msg_lines.append(f"Gap: {gap_cents} cents")

    msg_lines.append(f"EV: {ev_str}")
    msg_lines.append(f"EV Source: {ev_source}")

    if devig_str:
        msg_lines.append(f"Devig vs: {devig_str}")

    msg_lines.append(f"Model: {DEVIG_DESC}")
    return "\n".join(msg_lines)


def send_telegram(msg: str) -> None:
    if TELEGRAM_DISABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(msg)
        print("-" * 50)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=20,
        )
        if not resp.ok:
            print(f"Telegram send failed: {resp.status_code}")
            print(msg)
            print("-" * 50)
    except Exception as e:
        print(f"Telegram error: {e}")
        print(msg)
        print("-" * 50)


def main() -> None:
    tokens = load_tokens()
    if not str(tokens.get("access_token") or "").strip():
        raise SystemExit("Missing EVSharps access token. Set EVSHARPS_ACCESS_TOKEN or initialize .evsharps_tokens.json.")

    while True:
        started = datetime.now()
        cache = load_alert_cache()
        alerted_today = set(cache.get("keys", []))

        reasons = {
            "total": 0,
            "skip_placeholder": 0,
            "fail_books": 0,
            "fail_sharp_confirmation": 0,
            "fail_ny_book": 0,
            "fail_odds_parse": 0,
            "fail_ev_api": 0,
            "fail_weighted_missing": 0,
            "fail_ev_floor": 0,
            "fail_zone": 0,
            "fail_market_consensus": 0,
            "fail_gap": 0,
            "dupe": 0,
            "alerted": 0,
        }

        fail_examples: List[Dict[str, Any]] = []
        fail_examples_by_reason: Dict[str, int] = {}
        near_miss: List[Dict[str, Any]] = []
        candidates = 0

        def add_fail_example(reason: str, p: Dict[str, Any]) -> None:
            if not DEBUG_SCAN:
                return
            if len(fail_examples) >= 10:
                return
            if fail_examples_by_reason.get(reason, 0) >= 3:
                return
            book_odds = p.get("bookOdds")
            books_present = list(book_odds.keys()) if isinstance(book_odds, dict) else None
            fail_examples.append(
                {
                    "reason": reason,
                    "player": p.get("player"),
                    "prop": p.get("prop"),
                    "handicap": p.get("handicap"),
                    "under": p.get("under"),
                    "book": p.get("book"),
                    "line": p.get("line"),
                    "books_present": books_present,
                }
            )
            fail_examples_by_reason[reason] = fail_examples_by_reason.get(reason, 0) + 1

        try:
            if access_token_expired(tokens):
                refreshed = refresh_access_token(tokens)
                if refreshed:
                    tokens = refreshed
                else:
                    send_telegram(AUTH_REFRESH_FAIL_MSG)

            payload, tokens = fetch_payload(tokens)
            save_tokens(tokens)
            picks = extract_picks(payload)

            if DEBUG_SCAN:
                print("RAW SAMPLE COUNT:", len(picks))
                for p in picks[:10]:
                    book_odds = p.get("bookOdds")
                    keys = list(book_odds.keys()) if isinstance(book_odds, dict) else None
                    vals = list(book_odds.values())[:3] if isinstance(book_odds, dict) else None
                    raw_sample = {
                        "player": p.get("player"),
                        "prop": p.get("prop"),
                        "handicap": p.get("handicap"),
                        "under": p.get("under"),
                        "book": p.get("book"),
                        "line": p.get("line"),
                        "ev_raw": p.get("ev"),
                        "fairVal_raw": p.get("fairVal"),
                        "implied_raw": p.get("implied"),
                        "kelly_raw": p.get("kelly"),
                        "bookOdds_keys": keys,
                        "bookOdds_values_sample": vals,
                        "dt": p.get("dt"),
                        "game": p.get("game"),
                    }
                    print("RAW SAMPLE:", json.dumps(raw_sample, ensure_ascii=False))

            for p in picks:
                reasons["total"] += 1

                if is_placeholder_pick(p):
                    reasons["skip_placeholder"] += 1
                    add_fail_example("skip_placeholder", p)
                    continue

                books_ok, books_fail = sharp_confirmation_ok(p)
                if not books_ok:
                    if books_fail == "books":
                        reasons["fail_books"] += 1
                        add_fail_example("fail_books", p)
                    else:
                        reasons["fail_sharp_confirmation"] += 1
                        add_fail_example("fail_sharp_confirmation", p)
                        if SHOW_NEAR_MISS:
                            near_miss.append(
                                {
                                    "_ev_score": to_ev_float(p.get("ev")) or float("-inf"),
                                    "reason": "fail_sharp_confirmation",
                                    "player": p.get("player"),
                                    "prop": p.get("prop"),
                                    "handicap": p.get("handicap"),
                                    "under": p.get("under"),
                                    "book": p.get("book"),
                                    "line": p.get("line"),
                                    "ev_raw": p.get("ev"),
                                    "fairVal_raw": p.get("fairVal"),
                                    "game": p.get("game"),
                                    "dt": p.get("dt"),
                                    "books_present": list(p.get("bookOdds", {}).keys()) if isinstance(p.get("bookOdds"), dict) else None,
                                }
                            )
                    continue

                pick_book_code = book_code(p.get("book"))
                if pick_book_code not in NY_ALLOWED_BOOKS:
                    reasons["fail_ny_book"] += 1
                    add_fail_example("fail_ny_book", p)
                    if SHOW_NEAR_MISS:
                        near_miss.append(
                            {
                                "_ev_score": to_ev_float(p.get("ev")) or float("-inf"),
                                "reason": "fail_ny_book",
                                "player": p.get("player"),
                                "prop": p.get("prop"),
                                "book": p.get("book"),
                                "line": p.get("line"),
                                "ev_raw": p.get("ev"),
                            }
                        )
                    continue

                odds_int = to_int_odds(p.get("line"))
                if odds_int is None:
                    reasons["fail_odds_parse"] += 1
                    add_fail_example("fail_odds_parse", p)
                    continue

                fair_int = to_int_odds(p.get("fairVal"))
                raw_ev = p.get("ev")
                ev_api = to_ev_float(raw_ev)
                if ev_api is None or ev_api == 0:
                    if fair_int is not None:
                        p_true_api = true_prob_from_american_fair(fair_int)
                        ev_api = ev_from_prob_and_american(p_true_api, odds_int)
                    else:
                        reasons["fail_ev_api"] += 1
                        add_fail_example("fail_ev_api", p)
                        continue

                fair_api = americanize(p.get("fairVal"))

                ev_used = ev_api
                fair_used = fair_api
                ev_source = "API"

                if USE_WEIGHTED_DEVIG:
                    p_true = weighted_devig_fair_prob(p)
                    if p_true is None:
                        reasons["fail_weighted_missing"] += 1
                    else:
                        fair_odds = american_from_prob(p_true)
                        if fair_odds is not None:
                            ev_used = ev_from_prob_and_american(p_true, odds_int)
                            fair_used = americanize(fair_odds)
                            ev_source = "WEIGHTED"
                        else:
                            reasons["fail_weighted_missing"] += 1

                if DEBUG_SCAN and reasons["total"] <= 10:
                    print(
                        "DEBUG:",
                        p.get("player"),
                        "ev_raw=", raw_ev,
                        "fairVal=", p.get("fairVal"),
                        "line=", p.get("line"),
                        "ev_api=", ev_api,
                        "ev_used=", ev_used,
                        "source=", ev_source,
                    )

                if ev_used < EV_THRESHOLD_FLOOR:
                    reasons["fail_ev_floor"] += 1
                    add_fail_example("fail_ev_floor", p)
                    continue

                zone = zone_for_play(odds_int, ev_used)
                if zone is None:
                    reasons["fail_zone"] += 1
                    add_fail_example("fail_zone", p)
                    continue

                market_odds: Optional[int] = None
                gap_cents: Optional[int] = None

                if ENABLE_GAP_FILTER:
                    fair_used_int = to_int_odds(fair_used)
                    market_odds = market_consensus_odds(p)
                    if fair_used_int is None or market_odds is None:
                        reasons["fail_market_consensus"] += 1
                        add_fail_example("fail_market_consensus", p)
                        continue

                    gap_ok, gap_cents, min_gap = fair_market_gap_ok(zone, fair_used_int, market_odds)
                    if not gap_ok:
                        reasons["fail_gap"] += 1
                        add_fail_example("fail_gap", p)
                        if SHOW_NEAR_MISS:
                            near_miss.append(
                                {
                                    "_ev_score": ev_used,
                                    "reason": "fail_gap",
                                    "player": p.get("player"),
                                    "prop": p.get("prop"),
                                    "book": p.get("book"),
                                    "line": p.get("line"),
                                    "fair_used": fair_used,
                                    "market_odds": market_odds,
                                    "gap_cents": gap_cents,
                                    "min_gap": min_gap,
                                }
                            )
                        continue

                candidates += 1

                k = stable_key(p)
                if k in alerted_today:
                    reasons["dupe"] += 1
                    add_fail_example("dupe", p)
                    continue

                msg = format_pick(
                    p=p,
                    zone=zone,
                    ev_used=ev_used,
                    fair_used=fair_used,
                    ev_source=ev_source,
                    market_odds=market_odds,
                    gap_cents=gap_cents,
                    place_on=format_ny_book_name(p.get("book")),
                )
                send_telegram(msg)
                persist_alert_candidate(
                    p=p,
                    alert_id=k,
                    zone=zone,
                    ev_used=ev_used,
                    ev_source=ev_source,
                    fair_used=fair_used,
                    market_odds=market_odds,
                    gap_cents=gap_cents,
                )

                alerted_today.add(k)
                reasons["alerted"] += 1

            cache = {"date": today_iso(), "keys": sorted(alerted_today)}
            save_alert_cache(cache)

            if DEBUG_SCAN:
                print("SCAN SUMMARY:", reasons)
                if fail_examples:
                    print("FAIL EXAMPLES:", json.dumps(fail_examples[:10], ensure_ascii=False))

            if DEBUG_SCAN or SHOW_NEAR_MISS:
                near_miss_sorted = sorted(
                    near_miss,
                    key=lambda x: x.get("_ev_score", float("-inf")),
                    reverse=True,
                )[:10]
                print("TOP NEAR-MISSES:")
                for nm in near_miss_sorted:
                    row = {k: v for k, v in nm.items() if k != "_ev_score"}
                    print(json.dumps(row, ensure_ascii=False))

        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in {401, 403}:
                send_telegram(AUTH_REFRESH_FAIL_MSG)
            print("HTTP error:", e)
        except Exception as e:
            print("Error:", e)

        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Scan complete: total={reasons['total']} placeholders={reasons['skip_placeholder']} "
            f"candidates={candidates} alerted={reasons['alerted']} time={completed_at}"
        )

        if RUN_ONCE:
            break

        elapsed = (datetime.now() - started).total_seconds()
        sleep_for = max(1, POLL_SECONDS - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
