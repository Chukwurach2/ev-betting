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
try:
    from storage import load_ledger_payload
except Exception:
    load_ledger_payload = None

API_URL = "https://api-production-3a3b.up.railway.app/api/nba"
TOKEN_CACHE_FILE = Path(__file__).resolve().parent / ".evsharps_tokens.json"


def _safe_float(value: Any, default: float) -> float:
    try:
        v = float(value)
    except Exception:
        return default
    if v != v:  # NaN check
        return default
    return v

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
DEBUG_PLAYER = os.getenv("DEBUG_PLAYER", "").strip()
DEBUG_PROP = os.getenv("DEBUG_PROP", "").strip()
DEBUG_HANDICAP = os.getenv("DEBUG_HANDICAP", "").strip()
DEBUG_UNDER_RAW = os.getenv("DEBUG_UNDER", "").strip()

ENABLE_GAP_FILTER = os.getenv("ENABLE_GAP_FILTER", "1") == "1"
GAP_PRIMARY = int(os.getenv("GAP_PRIMARY", "5"))
GAP_EXTENDED = int(os.getenv("GAP_EXTENDED", "12"))
GAP_HIGH = int(os.getenv("GAP_HIGH", "20"))

BANKROLL = _safe_float(os.getenv("BANKROLL", "500"), 500.0)
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))
MIN_STAKE = float(os.getenv("MIN_STAKE", "2"))
MAX_STAKE_PCT = float(os.getenv("MAX_STAKE_PCT", "0.03"))
ROUND_TO = float(os.getenv("ROUND_TO", "0.50"))
BANKROLL_MODE = os.getenv("BANKROLL_MODE", "realized").strip().lower() or "realized"

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


def _parse_debug_under(raw: str) -> Optional[bool]:
    v = str(raw or "").strip().lower()
    if v == "":
        return None
    if v in {"1", "true", "t", "yes", "y"}:
        return True
    if v in {"0", "false", "f", "no", "n"}:
        return False
    return None


DEBUG_UNDER = _parse_debug_under(DEBUG_UNDER_RAW)


def _norm_str(value: Any) -> str:
    return str(value or "").strip().lower()


def is_target_debug_pick(p: Dict[str, Any]) -> bool:
    if not DEBUG_PLAYER:
        return False
    if _norm_str(p.get("player")) != _norm_str(DEBUG_PLAYER):
        return False
    if DEBUG_PROP and _norm_str(p.get("prop")) != _norm_str(DEBUG_PROP):
        return False
    if DEBUG_HANDICAP and str(p.get("handicap") or "").strip() != DEBUG_HANDICAP.strip():
        return False
    if DEBUG_UNDER is not None and bool(p.get("under", False)) != DEBUG_UNDER:
        return False
    return True


def emit_target_debug(diag: Dict[str, Any]) -> None:
    print("TARGET DEBUG:", json.dumps(diag, ensure_ascii=False))


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


def american_profit_multiple(odds: int) -> float:
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


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


def best_ny_price_from_bookodds(p: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return None, None

    want_under = bool(p.get("under", False))
    best_code: Optional[str] = None
    best_odds: Optional[int] = None
    best_score: Optional[float] = None

    for bk_raw, raw in book_odds.items():
        bk = book_code(bk_raw)
        if bk not in NY_ALLOWED_BOOKS:
            continue
        side_odds = side_market_odds(raw, want_under)
        if side_odds is None or side_odds == 0:
            continue
        score = american_profit_multiple(side_odds)
        if best_score is None or score > best_score:
            best_score = score
            best_code = bk
            best_odds = side_odds

    return best_code, best_odds


def _bookodds_map_lower(book_odds: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in book_odds.items():
        kk = book_code(k)
        if kk and kk not in out:
            out[kk] = v
    return out


def _tiered_sharp_books(available_books: set[str]) -> List[str]:
    if "pn" in available_books and "circa" in available_books:
        return ["pn", "circa"]
    if "pn" in available_books and "bol" in available_books:
        return ["pn", "bol"]
    if "circa" in available_books and "bol" in available_books:
        return ["circa", "bol"]
    if "bol" in available_books and ("dk" in available_books or "fd" in available_books):
        out = ["bol"]
        if "dk" in available_books:
            out.append("dk")
        if "fd" in available_books:
            out.append("fd")
        return out
    return []


def sharp_fair_prob_details(p: Dict[str, Any]) -> Dict[str, Any]:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return {"p_true": None, "books_used": [], "method": "NO_BOOKODDS"}

    want_under = bool(p.get("under", False))
    odds_map = _bookodds_map_lower(book_odds)
    selected = _tiered_sharp_books(set(odds_map.keys()))
    if not selected:
        return {"p_true": None, "books_used": [], "method": "NO_TIER_MATCH"}

    weighted: List[Tuple[float, float]] = []
    books_used: List[str] = []

    for bk in selected:
        raw = odds_map.get(bk)
        if raw is None:
            continue

        parsed = parse_two_sided_odds(raw)
        if parsed:
            over_odds, under_odds = parsed
            p_over_raw = implied_prob_from_american(over_odds)
            p_under_raw = implied_prob_from_american(under_odds)
            denom = p_over_raw + p_under_raw
            if denom <= 0:
                continue
            p_over_true = p_over_raw / denom
            p_side = (1.0 - p_over_true) if want_under else p_over_true
            confidence = 1.0
        else:
            side = side_market_odds(raw, want_under)
            if side is None:
                continue
            p_side = implied_prob_from_american(side)
            # one-sided quote gets lower confidence weight
            confidence = 0.70

        base_w = DEVIG_WEIGHTS.get(bk, 0.0)
        eff_w = base_w * confidence
        if eff_w <= 0:
            continue

        weighted.append((p_side, eff_w))
        books_used.append(bk)

    if not weighted:
        return {"p_true": None, "books_used": books_used, "method": "NO_VALID_SHARP_ROWS"}

    wsum = sum(w for _, w in weighted)
    if wsum <= 0:
        return {"p_true": None, "books_used": books_used, "method": "ZERO_WEIGHT"}
    p_true = sum(p * (w / wsum) for p, w in weighted)
    return {"p_true": p_true, "books_used": books_used, "method": "SHARP_TIERED"}


def sharp_fair_prob_from_bookodds(p: Dict[str, Any]) -> Optional[float]:
    return sharp_fair_prob_details(p).get("p_true")


def kelly_fraction_from_prob_and_odds(p_true: float, odds: int) -> float:
    b = american_profit_multiple(odds)
    q = 1.0 - p_true
    f = (b * p_true - q) / b
    return max(0.0, f)


def round_to_increment(value: float, increment: float) -> float:
    if increment <= 0:
        return value
    return round(value / increment) * increment


def recommended_stake(bankroll: float, p_true: float, odds: int) -> float:
    full_kelly = kelly_fraction_from_prob_and_odds(p_true, odds)
    if full_kelly <= 0:
        return 0.0

    raw_stake = bankroll * full_kelly * KELLY_FRACTION
    cap_amount = bankroll * MAX_STAKE_PCT
    stake = min(raw_stake, cap_amount)

    if stake > 0 and stake < MIN_STAKE:
        stake = MIN_STAKE

    stake = round_to_increment(stake, ROUND_TO)
    return max(0.0, stake)


def resolve_runtime_bankroll(fallback_bankroll: float) -> Tuple[float, str]:
    fallback = _safe_float(fallback_bankroll, 0.0)
    fallback_valid = fallback > 0

    if load_ledger_payload is None:
        if fallback_valid:
            return fallback, "env_fallback_no_storage"
        return 0.0, "none"

    try:
        payload = load_ledger_payload()
        start_raw = payload.get("starting_bankroll", fallback)
        start = _safe_float(start_raw if start_raw not in (None, "") else fallback, fallback)
        bets = payload.get("bets", [])
        if not isinstance(bets, list):
            bets = []

        settled_statuses = {"WON", "LOST", "VOID"}
        settled_pnl = 0.0
        open_exposure = 0.0
        for b in bets:
            if not isinstance(b, dict):
                continue
            status = str(b.get("status", "")).upper()
            if status in settled_statuses:
                settled_pnl += _safe_float(b.get("pnl", 0.0), 0.0)
            else:
                open_exposure += _safe_float(b.get("stake", 0.0), 0.0)

        realized = start + settled_pnl
        dynamic = realized - open_exposure if BANKROLL_MODE == "effective" else realized
        if dynamic > 0:
            source = "dynamic_effective" if BANKROLL_MODE == "effective" else "dynamic_realized"
            return dynamic, source
        if fallback_valid:
            return fallback, "env_fallback_dynamic_nonpositive"
        return 0.0, "none"
    except Exception:
        if fallback_valid:
            return fallback, "env_fallback_dynamic_error"
        return 0.0, "none"


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


def is_reconstructible_placeholder(p: Dict[str, Any]) -> bool:
    if not is_placeholder_pick(p):
        return False

    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict) or not book_odds:
        return False

    books_present = {book_code(k) for k in book_odds.keys() if str(k).strip()}
    if len(books_present) < 3:
        return False

    sharp_ok, _ = sharp_confirmation_ok(p)
    if not sharp_ok:
        return False

    if not any(b in NY_ALLOWED_BOOKS for b in books_present):
        return False

    want_under = bool(p.get("under", False))
    for raw in book_odds.values():
        side = side_market_odds(raw, want_under)
        if side is not None and side != 0:
            return True
    return False


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


def market_identity_key(p: Dict[str, Any]) -> str:
    parts = [
        _norm_str(p.get("dt")),
        _norm_str(p.get("game")),
        _norm_str(p.get("player")),
        _norm_str(p.get("prop")),
        str(p.get("handicap") or "").strip(),
        "u" if bool(p.get("under", False)) else "o",
    ]
    return "|".join(parts)


def _books_present_count(p: Dict[str, Any]) -> int:
    book_odds = p.get("bookOdds")
    if not isinstance(book_odds, dict):
        return 0
    return len([k for k in book_odds.keys() if str(k).strip()])


def row_quality_score(p: Dict[str, Any]) -> Tuple[int, int, int, int, int, int, int, int, str, str]:
    placeholder = is_placeholder_pick(p)
    ev_val = to_ev_float(p.get("ev"))
    ev_nonzero = 1 if (ev_val is not None and abs(ev_val) > 1e-12) else 0

    line_int = to_int_odds(p.get("line"))
    has_line = 1 if (line_int is not None and line_int != 0) else 0
    has_book = 1 if bool(book_code(p.get("book"))) else 0

    books_count = _books_present_count(p)
    has_bookodds = 1 if books_count > 0 else 0

    fair_int = to_int_odds(p.get("fairVal"))
    fair_nondefault = 1 if (fair_int is not None and fair_int not in {0, 100}) else 0

    ny_book, ny_line = best_ny_price_from_bookodds(p)
    ny_placeable = 1 if (ny_book and ny_line is not None and ny_line != 0) else 0

    # Higher tuple wins.
    return (
        1 if not placeholder else 0,  # 1) non-placeholder
        ev_nonzero,                   # 2) valid nonzero ev
        has_line,                     # 3) non-empty/nonzero line
        has_book,                     # 4) non-empty book
        has_bookodds,                 # 5) populated bookOdds
        books_count,                  # 6) more books
        fair_nondefault,              # 7) fairVal not default
        ny_placeable,                 # 8) NY-placeable derived line exists
        _norm_str(p.get("book")),     # deterministic tie-breaker
        str(p.get("line") or "").strip(),
    )


def select_representative_rows(raw_picks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for p in raw_picks:
        grouped.setdefault(market_identity_key(p), []).append(p)

    reps: List[Dict[str, Any]] = []
    rep_by_key: Dict[str, Dict[str, Any]] = {}
    for key, rows in grouped.items():
        best = max(rows, key=row_quality_score)
        reps.append(best)
        rep_by_key[key] = best
    return reps, grouped, rep_by_key


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
    place_book_code: str,
    place_book_name: str,
    place_odds_int: int,
    zone: str,
    ev_used: float,
    ev_source: str,
    fair_source: str,
    fair_used: str,
    market_odds: Optional[int],
    gap_cents: Optional[int],
    recommended_stake_amount: float,
    bankroll_used: float,
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
        "recommended_book_code": place_book_code,
        "recommended_book_name": place_book_name,
        "recommended_odds": place_odds_int,
        "fair_odds": to_int_odds(fair_used) if fair_used is not None else None,
        "market_odds": market_odds,
        "ev_pct": round(ev_used * 100.0, 4),
        "gap_cents": gap_cents,
        "zone": zone,
        "ev_source": ev_source,
        "fair_source": fair_source,
        "recommended_stake": round(float(recommended_stake_amount), 2),
        "bankroll_snapshot": round(float(bankroll_used), 2),
        "kelly_fraction_used": KELLY_FRACTION,
        "max_stake_pct": MAX_STAKE_PCT,
        "min_stake": MIN_STAKE,
        "round_to": ROUND_TO,
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
    place_book_name: str,
    place_odds_int: int,
    ev_used: float,
    fair_used: str,
    ev_source: str,
    fair_source: str,
    market_odds: Optional[int],
    gap_cents: Optional[int],
    recommended_stake_amount: float,
    bankroll_used: float,
) -> str:
    player = str(p.get("player") or "").strip()
    market_str = build_market_string(p)
    odds = americanize(place_odds_int)
    book_name = place_book_name
    dt_raw = str(p.get("dt") or "").strip() or "N/A"
    game_raw = str(p.get("game") or "").strip() or "N/A"

    ev_str = f"{ev_used * 100:.1f}%"
    devig_str = devig_against_string(p)

    msg_lines = [
        f"+EV ALERT ({zone_label(zone)})",
        f"{player} | {market_str}",
        f"DATE: {dt_raw}",
        f"GAME: {game_raw}",
        f"PLACE ON: {place_book_name}",
        f"Book: {book_name} {odds} | Fair: {fair_used}",
    ]

    if market_odds is not None:
        msg_lines.append(f"Market: {americanize(market_odds)}")
    if gap_cents is not None:
        msg_lines.append(f"Gap: {gap_cents} cents")

    msg_lines.append(f"EV: {ev_str}")
    msg_lines.append(f"Recommended Stake: ${recommended_stake_amount:.2f}")
    msg_lines.append(f"Bankroll Used: ${bankroll_used:.2f}")
    msg_lines.append(
        f"Stake Rule: {KELLY_FRACTION:.2f} Kelly, max {MAX_STAKE_PCT*100:.0f}% bankroll"
    )
    msg_lines.append(f"EV Source: {ev_source}")
    msg_lines.append(f"Fair Source: {fair_source}")

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
            raw_picks = extract_picks(payload)
            picks, grouped_raw_rows, rep_by_market = select_representative_rows(raw_picks)
            target_market_keys: set[str] = set()
            bankroll_used, bankroll_source = resolve_runtime_bankroll(BANKROLL)

            if DEBUG_SCAN:
                print(f"BANKROLL DEBUG: source={bankroll_source} used={bankroll_used:.2f}")
                print("RAW SAMPLE COUNT:", len(raw_picks))
                print("REPRESENTATIVE COUNT:", len(picks))
                for p in raw_picks[:10]:
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

            if DEBUG_PLAYER:
                target_raw_rows = [r for r in raw_picks if is_target_debug_pick(r)]
                target_market_keys = {market_identity_key(r) for r in target_raw_rows}
                if target_raw_rows:
                    for mkey in sorted(target_market_keys):
                        rows = grouped_raw_rows.get(mkey, [])
                        selected = rep_by_market.get(mkey)
                        print(f"TARGET DUPES: key={mkey} count={len(rows)}")
                        for row in rows:
                            books_count = _books_present_count(row)
                            dupe_summary = {
                                "book": row.get("book"),
                                "line": row.get("line"),
                                "ev": row.get("ev"),
                                "fairVal": row.get("fairVal"),
                                "placeholder": is_placeholder_pick(row),
                                "reconstructible_placeholder": is_reconstructible_placeholder(row),
                                "books_present_count": books_count,
                                "ranking": row_quality_score(row),
                            }
                            print("TARGET DUPE ROW:", json.dumps(dupe_summary, ensure_ascii=False))
                        if selected is not None:
                            sel_summary = {
                                "book": selected.get("book"),
                                "line": selected.get("line"),
                                "ev": selected.get("ev"),
                                "fairVal": selected.get("fairVal"),
                                "placeholder": is_placeholder_pick(selected),
                                "reconstructible_placeholder": is_reconstructible_placeholder(selected),
                                "ranking": row_quality_score(selected),
                            }
                            print("TARGET DUPE SELECTED:", json.dumps(sel_summary, ensure_ascii=False))
                else:
                    print("TARGET DUPES: no raw rows matched DEBUG_* filters")

            for p in picks:
                reasons["total"] += 1
                debug_target = market_identity_key(p) in target_market_keys if DEBUG_PLAYER else False
                debug_diag = {
                    "player": p.get("player"),
                    "prop": p.get("prop"),
                    "handicap": p.get("handicap"),
                    "under": bool(p.get("under", False)),
                    "book": p.get("book"),
                    "line": p.get("line"),
                    "ev_raw": p.get("ev"),
                    "fairVal_raw": p.get("fairVal"),
                    "placeholder": None,
                    "reconstructible_placeholder": None,
                    "books_present": list(p.get("bookOdds", {}).keys()) if isinstance(p.get("bookOdds"), dict) else None,
                    "sharp_confirmation": "NOT_CHECKED",
                    "ny_book": "NOT_CHECKED",
                    "derived_ny_book": None,
                    "derived_ny_line": None,
                    "derived_sharp_fair": None,
                    "odds_int": None,
                    "ev_used": None,
                    "zone": None,
                    "market_odds": None,
                    "gap": "NOT_CHECKED",
                    "dupe": "NOT_CHECKED",
                    "final_decision": None,
                    "reject_reason": None,
                }

                placeholder = is_placeholder_pick(p)
                reconstructible_placeholder = is_reconstructible_placeholder(p) if placeholder else False
                if debug_target:
                    debug_diag["placeholder"] = placeholder
                    debug_diag["reconstructible_placeholder"] = reconstructible_placeholder

                if placeholder and not reconstructible_placeholder:
                    reasons["skip_placeholder"] += 1
                    add_fail_example("skip_placeholder", p)
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "skip_placeholder"
                        emit_target_debug(debug_diag)
                    continue

                books_ok, books_fail = sharp_confirmation_ok(p)
                if not books_ok:
                    if books_fail == "books":
                        reasons["fail_books"] += 1
                        add_fail_example("fail_books", p)
                        if debug_target:
                            debug_diag["sharp_confirmation"] = "FAIL_BOOKS"
                    else:
                        reasons["fail_sharp_confirmation"] += 1
                        add_fail_example("fail_sharp_confirmation", p)
                        if debug_target:
                            debug_diag["sharp_confirmation"] = "FAIL_SHARP_CONFIRMATION"
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
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_books" if books_fail == "books" else "fail_sharp_confirmation"
                        emit_target_debug(debug_diag)
                    continue
                elif debug_target:
                    debug_diag["sharp_confirmation"] = "PASS"

                place_book_code, place_odds_int = best_ny_price_from_bookodds(p)
                if debug_target:
                    debug_diag["derived_ny_book"] = place_book_code
                    debug_diag["derived_ny_line"] = place_odds_int
                if not place_book_code or place_odds_int is None or place_odds_int == 0:
                    reasons["fail_ny_book"] += 1
                    add_fail_example("fail_ny_book", p)
                    if debug_target:
                        debug_diag["ny_book"] = "FAIL"
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
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_ny_book"
                        emit_target_debug(debug_diag)
                    continue
                elif debug_target:
                    debug_diag["ny_book"] = "PASS"

                odds_int = int(place_odds_int)
                if debug_target:
                    debug_diag["odds_int"] = odds_int
                if odds_int is None:
                    reasons["fail_odds_parse"] += 1
                    add_fail_example("fail_odds_parse", p)
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_odds_parse"
                        emit_target_debug(debug_diag)
                    continue

                raw_ev = p.get("ev")
                fair_int = to_int_odds(p.get("fairVal"))

                ev_used: Optional[float] = None
                fair_used: Optional[str] = None
                ev_source = "API_FALLBACK"
                fair_source = "API_FALLBACK"

                sharp_details = sharp_fair_prob_details(p)
                p_true_sharp = sharp_details.get("p_true")
                if p_true_sharp is not None:
                    sharp_fair_odds = american_from_prob(p_true_sharp)
                    if sharp_fair_odds is not None:
                        fair_used = americanize(sharp_fair_odds)
                        ev_used = ev_from_prob_and_american(p_true_sharp, odds_int)
                        ev_source = "SHARP_BOOKS"
                        fair_source = "SHARP_BOOKS"
                        if debug_target:
                            debug_diag["derived_sharp_fair"] = fair_used

                if ev_used is None:
                    if USE_WEIGHTED_DEVIG:
                        p_true_weighted = weighted_devig_fair_prob(p)
                        if p_true_weighted is None:
                            reasons["fail_weighted_missing"] += 1
                        else:
                            fair_weighted_odds = american_from_prob(p_true_weighted)
                            if fair_weighted_odds is not None:
                                fair_used = americanize(fair_weighted_odds)
                                ev_used = ev_from_prob_and_american(p_true_weighted, odds_int)
                                ev_source = "WEIGHTED_FALLBACK"
                                fair_source = "WEIGHTED_FALLBACK"
                            else:
                                reasons["fail_weighted_missing"] += 1

                if ev_used is None:
                    ev_api = to_ev_float(raw_ev)
                    fair_api = americanize(p.get("fairVal"))
                    if ev_api is None or ev_api == 0:
                        if fair_int is not None:
                            p_true_api = true_prob_from_american_fair(fair_int)
                            ev_api = ev_from_prob_and_american(p_true_api, odds_int)
                        else:
                            reasons["fail_ev_api"] += 1
                            add_fail_example("fail_ev_api", p)
                            if debug_target:
                                debug_diag["final_decision"] = "REJECT"
                                debug_diag["reject_reason"] = "fail_ev_api"
                                emit_target_debug(debug_diag)
                            continue
                    ev_used = ev_api
                    fair_used = fair_api
                    ev_source = "API_FALLBACK"
                    fair_source = "API_FALLBACK"

                # type narrowing
                if ev_used is None:
                    reasons["fail_ev_api"] += 1
                    add_fail_example("fail_ev_api", p)
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_ev_api"
                        emit_target_debug(debug_diag)
                    continue
                if fair_used is None:
                    fair_used = ""
                if debug_target:
                    debug_diag["ev_used"] = ev_used

                if DEBUG_SCAN and reasons["total"] <= 10:
                    print(
                        "DEBUG:",
                        p.get("player"),
                        "ev_raw=", raw_ev,
                        "fairVal=", p.get("fairVal"),
                        "line=", p.get("line"),
                        "ev_api=", to_ev_float(raw_ev),
                        "ev_used=", ev_used,
                        "source=", ev_source,
                    )

                if ev_used < EV_THRESHOLD_FLOOR:
                    reasons["fail_ev_floor"] += 1
                    add_fail_example("fail_ev_floor", p)
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_ev_floor"
                        emit_target_debug(debug_diag)
                    continue

                zone = zone_for_play(odds_int, ev_used)
                if debug_target:
                    debug_diag["zone"] = zone
                if zone is None:
                    reasons["fail_zone"] += 1
                    add_fail_example("fail_zone", p)
                    if debug_target:
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "fail_zone"
                        emit_target_debug(debug_diag)
                    continue

                market_odds: Optional[int] = None
                gap_cents: Optional[int] = None

                if ENABLE_GAP_FILTER:
                    fair_used_int = to_int_odds(fair_used)
                    market_odds = market_consensus_odds(p)
                    if debug_target:
                        debug_diag["market_odds"] = market_odds
                    if fair_used_int is None or market_odds is None:
                        reasons["fail_market_consensus"] += 1
                        add_fail_example("fail_market_consensus", p)
                        if debug_target:
                            debug_diag["gap"] = "FAIL_MARKET_CONSENSUS"
                            debug_diag["final_decision"] = "REJECT"
                            debug_diag["reject_reason"] = "fail_market_consensus"
                            emit_target_debug(debug_diag)
                        continue

                    gap_ok, gap_cents, min_gap = fair_market_gap_ok(zone, fair_used_int, market_odds)
                    if not gap_ok:
                        reasons["fail_gap"] += 1
                        add_fail_example("fail_gap", p)
                        if debug_target:
                            debug_diag["gap"] = f"FAIL(min={min_gap}, actual={gap_cents})"
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
                        if debug_target:
                            debug_diag["final_decision"] = "REJECT"
                            debug_diag["reject_reason"] = "fail_gap"
                            emit_target_debug(debug_diag)
                        continue
                    elif debug_target:
                        debug_diag["gap"] = f"PASS({gap_cents})"
                elif debug_target:
                    debug_diag["gap"] = "SKIPPED"

                fair_for_stake = to_int_odds(fair_used)
                if fair_for_stake is not None:
                    p_true_for_stake = true_prob_from_american_fair(fair_for_stake)
                    recommended_stake_amount = recommended_stake(bankroll_used, p_true_for_stake, odds_int)
                else:
                    recommended_stake_amount = 0.0

                candidates += 1

                k = stable_key(p)
                if k in alerted_today:
                    reasons["dupe"] += 1
                    add_fail_example("dupe", p)
                    if debug_target:
                        debug_diag["dupe"] = "FAIL"
                        debug_diag["final_decision"] = "REJECT"
                        debug_diag["reject_reason"] = "dupe"
                        emit_target_debug(debug_diag)
                    continue
                elif debug_target:
                    debug_diag["dupe"] = "PASS"

                msg = format_pick(
                    p=p,
                    zone=zone,
                    place_book_name=format_ny_book_name(place_book_code),
                    place_odds_int=odds_int,
                    ev_used=ev_used,
                    fair_used=fair_used,
                    ev_source=ev_source,
                    fair_source=fair_source,
                    market_odds=market_odds,
                    gap_cents=gap_cents,
                    recommended_stake_amount=recommended_stake_amount,
                    bankroll_used=bankroll_used,
                )
                send_telegram(msg)
                persist_alert_candidate(
                    p=p,
                    alert_id=k,
                    place_book_code=place_book_code,
                    place_book_name=format_ny_book_name(place_book_code),
                    place_odds_int=odds_int,
                    zone=zone,
                    ev_used=ev_used,
                    ev_source=ev_source,
                    fair_source=fair_source,
                    fair_used=fair_used,
                    market_odds=market_odds,
                    gap_cents=gap_cents,
                    recommended_stake_amount=recommended_stake_amount,
                    bankroll_used=bankroll_used,
                )

                alerted_today.add(k)
                reasons["alerted"] += 1
                if debug_target:
                    debug_diag["final_decision"] = "ALERT"
                    emit_target_debug(debug_diag)

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
