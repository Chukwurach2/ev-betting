"""
Storage backend for EV ledger.

Streamlit Cloud secrets required for Google Sheets mode:
- [gcp_service_account]  # full service-account JSON fields
- spreadsheet_name = "Your Google Sheet Name"
- worksheet_name = "ledger"  # optional (defaults to "ledger")

If secrets are missing/unavailable, storage falls back to local JSON:
`data/ev_ledger.json`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections.abc import Mapping


import streamlit as st

LOCAL_DATA_DIR = Path(__file__).resolve().parent / "data"
LOCAL_LEDGER_PATH = LOCAL_DATA_DIR / "ev_ledger.json"
DEFAULT_WORKSHEET = "ledger"

REQUIRED_LEDGER_COLUMNS = [
    "timestamp",
    "league",
    "market",
    "player",
    "book",
    "book_odds",
    "fair_odds",
    "stake",
    "kelly_frac",
    "notes",
    "result",
]

NUMERIC_FIELDS = {
    "starting_bankroll",
    "unit_size",
    "odds_american",
    "stake",
    "fair_odds_american",
    "true_prob",
    "ev_pct",
    "kelly_fraction_used",
    "kelly_units_from_tool",
    "boost_pct",
    "unboosted_odds_american",
    "closing_odds_american",
    "recommended_stake_snapshot",
    "parlay_leg_count",
    "parlay_boost_pct",
    "parlay_unboosted_odds_american",
    "parlay_boosted_odds_american",
    "parlay_true_prob",
    "pnl",
    "book_odds",
    "fair_odds",
    "kelly_frac",
}

BOOL_FIELDS = {"is_live", "is_parlay"}


def _warn_once(message: str) -> None:
    key = f"storage_warn_{abs(hash(message))}"
    try:
        if not st.session_state.get(key):
            st.warning(message)
            st.session_state[key] = True
    except Exception:
        pass


def _secrets_get(key: str, default: Any = None) -> Any:
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def _gsheets_config() -> Dict[str, Any]:
    return {
        "credentials": _secrets_get("gcp_service_account"),
        "spreadsheet_id": _secrets_get("spreadsheet_id"),
        "spreadsheet_name": _secrets_get("spreadsheet_name"),
        "worksheet_name": _secrets_get("worksheet_name", DEFAULT_WORKSHEET) or DEFAULT_WORKSHEET,
    }

def _google_backend_enabled() -> bool:
    cfg = _gsheets_config()
    creds = cfg.get("credentials")
    has_creds = isinstance(creds, Mapping) and bool(creds)
    has_sheet = bool(cfg.get("spreadsheet_id") or cfg.get("spreadsheet_name"))
    return has_creds and has_sheet


def get_storage_backend_label() -> str:
    return "Google Sheets" if _google_backend_enabled() else "Local JSON"


@st.cache_resource(show_spinner=False)
def _get_gspread_worksheet():
    import gspread
    from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

    cfg = _gsheets_config()
    client = gspread.service_account_from_dict(dict(cfg["credentials"]))
    worksheet_name = str(cfg.get("worksheet_name") or DEFAULT_WORKSHEET)

    spreadsheet_id = cfg.get("spreadsheet_id")
    spreadsheet_name = cfg.get("spreadsheet_name")

    if spreadsheet_id:
        sh = client.open_by_key(str(spreadsheet_id))
    else:
        # fallback to name/url behavior
        try:
            sh = client.open(str(spreadsheet_name))
        except SpreadsheetNotFound:
            if str(spreadsheet_name).startswith("http"):
                sh = client.open_by_url(str(spreadsheet_name))
            else:
                sh = client.open_by_key(str(spreadsheet_name))

    try:
        ws = sh.worksheet(worksheet_name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=80)

    _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS)
    return ws


def _ensure_local_ledger_file() -> None:
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LOCAL_LEDGER_PATH.exists():
        LOCAL_LEDGER_PATH.write_text("[]", encoding="utf-8")


def _coerce_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool, list, dict)):
        return value

    txt = str(value).strip()
    if txt == "":
        return None

    if key in BOOL_FIELDS:
        low = txt.lower()
        if low in {"true", "1", "yes"}:
            return True
        if low in {"false", "0", "no"}:
            return False

    if key in NUMERIC_FIELDS:
        try:
            if any(ch in txt for ch in [".", "e", "E"]):
                return float(txt)
            return int(txt)
        except ValueError:
            return txt

    if txt.startswith("{") or txt.startswith("["):
        try:
            return json.loads(txt)
        except json.JSONDecodeError:
            return txt

    return txt


def _serialize_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _normalize_row_for_schema(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    normalized.setdefault("timestamp", normalized.get("placed_at") or datetime.now().isoformat(timespec="seconds"))
    normalized.setdefault("league", normalized.get("sport") or "")
    normalized.setdefault("player", normalized.get("selection") or "")
    normalized.setdefault("book_odds", normalized.get("odds_american") or "")
    normalized.setdefault("fair_odds", normalized.get("fair_odds_american") or "")
    normalized.setdefault("kelly_frac", normalized.get("kelly_fraction_used") or "")
    normalized.setdefault("notes", normalized.get("notes") or "")
    normalized.setdefault("result", normalized.get("status") or "")
    return normalized


def _sheet_row_to_app_bet(row: Dict[str, Any], default_unit_size: float) -> Dict[str, Any]:
    normalized = dict(row)
    placed_at = normalized.get("placed_at") or normalized.get("timestamp") or datetime.now().isoformat(timespec="seconds")
    status = str(normalized.get("status") or normalized.get("result") or "OPEN").upper()
    if status == "W":
        status = "WON"
    elif status == "L":
        status = "LOST"
    elif status not in {"OPEN", "WON", "LOST", "VOID"}:
        status = "OPEN"

    normalized.setdefault("bet_id", str(uuid.uuid4())[:8])
    normalized.setdefault("placed_at", placed_at)
    normalized.setdefault("sport", normalized.get("league") or "")
    normalized.setdefault("market", normalized.get("market") or "")
    normalized.setdefault("selection", normalized.get("selection") or normalized.get("player") or "")
    normalized.setdefault("book", normalized.get("book") or "")
    normalized.setdefault("odds_american", normalized.get("odds_american", normalized.get("book_odds")))
    normalized.setdefault("stake", normalized.get("stake", 0.0))
    normalized.setdefault("unit_size", normalized.get("unit_size", default_unit_size))
    normalized.setdefault("market_type", normalized.get("market_type", "Game"))
    normalized.setdefault("devig_method", normalized.get("devig_method", "Market Avg"))
    normalized.setdefault("status", status)
    normalized.setdefault("pnl", normalized.get("pnl", 0.0))

    for key, value in list(normalized.items()):
        normalized[key] = _coerce_value(key, value)

    return normalized


def _ensure_sheet_headers(ws, required_headers: List[str], row_keys: Optional[List[str]] = None) -> List[str]:
    headers = ws.row_values(1)
    if not headers:
        headers = list(required_headers)

    for header in required_headers:
        if header not in headers:
            headers.append(header)

    if row_keys:
        for key in row_keys:
            if key not in headers:
                headers.append(key)

    ws.update("1:1", [headers])
    return headers


def _load_google_rows() -> List[Dict[str, Any]]:
    ws = _get_gspread_worksheet()
    values = ws.get_all_values()
    if not values:
        _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS)
        return []

    headers = values[0]
    rows: List[Dict[str, Any]] = []
    for raw in values[1:]:
        if not raw or all(str(v).strip() == "" for v in raw):
            continue
        row = {}
        for idx, key in enumerate(headers):
            row[key] = _coerce_value(key, raw[idx] if idx < len(raw) else "")
        rows.append(row)
    return rows


def _append_google_row(row: Dict[str, Any]) -> None:
    ws = _get_gspread_worksheet()
    normalized = _normalize_row_for_schema(row)
    headers = _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS, list(normalized.keys()))
    values = [_serialize_value(normalized.get(h)) for h in headers]
    ws.append_row(values, value_input_option="USER_ENTERED")


def _rewrite_google_rows(rows: List[Dict[str, Any]]) -> None:
    ws = _get_gspread_worksheet()
    normalized_rows = [_normalize_row_for_schema(r) for r in rows]

    headers = list(REQUIRED_LEDGER_COLUMNS)
    for row in normalized_rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)

    ws.clear()
    ws.update("1:1", [headers])
    if normalized_rows:
        matrix = [[_serialize_value(row.get(h)) for h in headers] for row in normalized_rows]
        ws.append_rows(matrix, value_input_option="USER_ENTERED")


def _read_local_raw() -> Any:
    _ensure_local_ledger_file()
    try:
        with LOCAL_LEDGER_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        _warn_once("Local ledger file is missing/corrupt. Continuing with an empty ledger.")
        return []


def _write_local_raw(raw: Any) -> None:
    _ensure_local_ledger_file()
    with LOCAL_LEDGER_PATH.open("w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)


def load_ledger() -> List[Dict[str, Any]]:
    """Load ledger rows as a list of dicts from Sheets (primary) or local JSON fallback."""
    if _google_backend_enabled():
        try:
            return _load_google_rows()
        except Exception as exc:
            _warn_once(f"Google Sheets unavailable ({exc}). Falling back to local ledger.")

    raw = _read_local_raw()
    if isinstance(raw, dict):
        bets = raw.get("bets", [])
        return bets if isinstance(bets, list) else []
    if isinstance(raw, list):
        return raw
    return []


def append_ledger_row(row: Dict[str, Any]) -> None:
    """Append one row to Sheets (primary) or local JSON fallback."""
    if _google_backend_enabled():
        try:
            _append_google_row(row)
            return
        except Exception as exc:
            _warn_once(f"Google Sheets append failed ({exc}). Writing to local ledger fallback.")

    raw = _read_local_raw()
    if isinstance(raw, dict):
        raw.setdefault("bets", [])
        if not isinstance(raw["bets"], list):
            raw["bets"] = []
        raw["bets"].append(row)
    elif isinstance(raw, list):
        raw.append(row)
    else:
        raw = [row]
    _write_local_raw(raw)


def load_ledger_payload() -> Dict[str, Any]:
    """Compatibility payload for existing app behavior."""
    rows = load_ledger()

    starting_bankroll = 0.0
    unit_size = 1.0
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        sb = row.get("starting_bankroll")
        us = row.get("unit_size")
        if sb not in (None, ""):
            try:
                starting_bankroll = float(sb)
            except (TypeError, ValueError):
                pass
        if us not in (None, ""):
            try:
                unit_size = float(us)
            except (TypeError, ValueError):
                pass
        if sb not in (None, "") and us not in (None, ""):
            break

    bets = [_sheet_row_to_app_bet(r, unit_size) for r in rows if isinstance(r, dict)]
    return {
        "starting_bankroll": starting_bankroll,
        "unit_size": unit_size,
        "bets": bets,
    }


def save_ledger_payload(payload: Dict[str, Any]) -> None:
    """Persist full payload; used by existing dashboard flows that edit existing rows."""
    bets = payload.get("bets", []) if isinstance(payload, dict) else []
    if not isinstance(bets, list):
        bets = []

    enriched_rows: List[Dict[str, Any]] = []
    for bet in bets:
        if not isinstance(bet, dict):
            continue
        row = dict(bet)
        row["starting_bankroll"] = payload.get("starting_bankroll", 0.0)
        row["unit_size"] = payload.get("unit_size", 1.0)
        enriched_rows.append(row)

    if _google_backend_enabled():
        try:
            _rewrite_google_rows(enriched_rows)
            return
        except Exception as exc:
            _warn_once(f"Google Sheets save failed ({exc}). Saving to local fallback.")

    local_payload = {
        "starting_bankroll": float(payload.get("starting_bankroll", 0.0) or 0.0),
        "unit_size": float(payload.get("unit_size", 1.0) or 1.0),
        "bets": bets,
    }
    _write_local_raw(local_payload)
