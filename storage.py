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
LOCAL_ALERTS_PATH = LOCAL_DATA_DIR / "ev_alerts.json"
DEFAULT_WORKSHEET = "ledger"
DEFAULT_ALERTS_WORKSHEET = "alerts"

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

REQUIRED_ALERT_COLUMNS = [
    "alert_id",
    "timestamp",
    "player",
    "prop",
    "handicap",
    "under",
    "market_display",
    "game",
    "dt",
    "recommended_book_code",
    "recommended_book_name",
    "recommended_odds",
    "fair_odds",
    "market_odds",
    "ev_pct",
    "gap_cents",
    "zone",
    "ev_source",
    "devig_summary",
    "sharp_confirmation_summary",
    "is_logged",
    "logged_at",
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

BOOL_FIELDS = {"is_live", "is_parlay", "is_logged"}


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
        "alerts_worksheet_name": _secrets_get("alerts_worksheet_name", DEFAULT_ALERTS_WORKSHEET) or DEFAULT_ALERTS_WORKSHEET,
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


@st.cache_resource(show_spinner=False)
def _get_gspread_alerts_worksheet():
    import gspread
    from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

    cfg = _gsheets_config()
    client = gspread.service_account_from_dict(dict(cfg["credentials"]))
    worksheet_name = str(cfg.get("alerts_worksheet_name") or DEFAULT_ALERTS_WORKSHEET)

    spreadsheet_id = cfg.get("spreadsheet_id")
    spreadsheet_name = cfg.get("spreadsheet_name")

    if spreadsheet_id:
        sh = client.open_by_key(str(spreadsheet_id))
    else:
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

    _ensure_sheet_headers(ws, REQUIRED_ALERT_COLUMNS)
    return ws


def _ensure_local_ledger_file() -> None:
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LOCAL_LEDGER_PATH.exists():
        LOCAL_LEDGER_PATH.write_text("[]", encoding="utf-8")


def _ensure_local_alerts_file() -> None:
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not LOCAL_ALERTS_PATH.exists():
        LOCAL_ALERTS_PATH.write_text("[]", encoding="utf-8")


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


def _normalize_alert_row_for_schema(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    normalized.setdefault("alert_id", str(uuid.uuid4())[:8])
    normalized.setdefault("timestamp", datetime.now().isoformat(timespec="seconds"))
    normalized.setdefault("player", "")
    normalized.setdefault("prop", "")
    normalized.setdefault("handicap", "")
    normalized.setdefault("under", False)
    normalized.setdefault("market_display", "")
    normalized.setdefault("game", "")
    normalized.setdefault("dt", "")
    normalized.setdefault("recommended_book_code", "")
    normalized.setdefault("recommended_book_name", "")
    normalized.setdefault("recommended_odds", "")
    normalized.setdefault("fair_odds", "")
    normalized.setdefault("market_odds", "")
    normalized.setdefault("ev_pct", "")
    normalized.setdefault("gap_cents", "")
    normalized.setdefault("zone", "")
    normalized.setdefault("ev_source", "")
    normalized.setdefault("devig_summary", "")
    normalized.setdefault("sharp_confirmation_summary", "")
    normalized.setdefault("is_logged", False)
    normalized.setdefault("logged_at", "")
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


def _load_google_alert_rows() -> List[Dict[str, Any]]:
    ws = _get_gspread_alerts_worksheet()
    values = ws.get_all_values()
    if not values:
        _ensure_sheet_headers(ws, REQUIRED_ALERT_COLUMNS)
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


def _append_google_alert_row(row: Dict[str, Any]) -> None:
    ws = _get_gspread_alerts_worksheet()
    normalized = _normalize_alert_row_for_schema(row)
    headers = _ensure_sheet_headers(ws, REQUIRED_ALERT_COLUMNS, list(normalized.keys()))
    values = [_serialize_value(normalized.get(h)) for h in headers]
    ws.append_row(values, value_input_option="USER_ENTERED")


def _rewrite_google_alert_rows(rows: List[Dict[str, Any]]) -> None:
    ws = _get_gspread_alerts_worksheet()
    normalized_rows = [_normalize_alert_row_for_schema(r) for r in rows]

    headers = list(REQUIRED_ALERT_COLUMNS)
    for row in normalized_rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)

    ws.clear()
    ws.update("1:1", [headers])
    if normalized_rows:
        matrix = [[_serialize_value(row.get(h)) for h in headers] for row in normalized_rows]
        ws.append_rows(matrix, value_input_option="USER_ENTERED")


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


def _read_local_alerts_raw() -> Any:
    _ensure_local_alerts_file()
    try:
        with LOCAL_ALERTS_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        _warn_once("Local alerts file is missing/corrupt. Continuing with an empty alerts queue.")
        return []


def _write_local_raw(raw: Any) -> None:
    _ensure_local_ledger_file()
    with LOCAL_LEDGER_PATH.open("w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)


def _write_local_alerts_raw(raw: Any) -> None:
    _ensure_local_alerts_file()
    with LOCAL_ALERTS_PATH.open("w", encoding="utf-8") as f:
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
    row_to_write = dict(row) if isinstance(row, dict) else {}
    if not str(row_to_write.get("bet_id", "")).strip():
        row_to_write["bet_id"] = str(uuid.uuid4())[:8]

    if _google_backend_enabled():
        try:
            _append_google_row(row_to_write)
            return
        except Exception as exc:
            _warn_once(f"Google Sheets append failed ({exc}). Writing to local ledger fallback.")

    raw = _read_local_raw()
    if isinstance(raw, dict):
        raw.setdefault("bets", [])
        if not isinstance(raw["bets"], list):
            raw["bets"] = []
        raw["bets"].append(row_to_write)
    elif isinstance(raw, list):
        raw.append(row_to_write)
    else:
        raw = [row_to_write]
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


def load_alert_candidates() -> List[Dict[str, Any]]:
    """Load alert candidates from Sheets (primary) or local JSON fallback."""
    if _google_backend_enabled():
        try:
            return _load_google_alert_rows()
        except Exception as exc:
            _warn_once(f"Google Sheets alerts unavailable ({exc}). Falling back to local alerts queue.")

    raw = _read_local_alerts_raw()
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if isinstance(raw, dict):
        val = raw.get("alerts")
        if isinstance(val, list):
            return [r for r in val if isinstance(r, dict)]
    return []


def append_alert_candidate(row: Dict[str, Any]) -> bool:
    """
    Append one alert candidate to Sheets (primary) or local JSON fallback.
    Returns False when alert_id already exists.
    """
    row_to_write = _normalize_alert_row_for_schema(dict(row) if isinstance(row, dict) else {})
    alert_id = str(row_to_write.get("alert_id", "")).strip()
    if not alert_id:
        alert_id = str(uuid.uuid4())[:8]
        row_to_write["alert_id"] = alert_id

    existing = load_alert_candidates()
    if any(str(r.get("alert_id", "")).strip() == alert_id for r in existing if isinstance(r, dict)):
        return False

    if _google_backend_enabled():
        try:
            _append_google_alert_row(row_to_write)
            return True
        except Exception as exc:
            _warn_once(f"Google Sheets alert append failed ({exc}). Writing to local alerts fallback.")

    raw = _read_local_alerts_raw()
    if isinstance(raw, list):
        raw.append(row_to_write)
    elif isinstance(raw, dict):
        raw.setdefault("alerts", [])
        if not isinstance(raw["alerts"], list):
            raw["alerts"] = []
        raw["alerts"].append(row_to_write)
    else:
        raw = [row_to_write]
    _write_local_alerts_raw(raw)
    return True


def mark_alert_logged(alert_id: str, logged_at: Optional[str] = None) -> bool:
    """Mark alert candidate as logged; returns True if an alert row was updated."""
    target = str(alert_id or "").strip()
    if not target:
        return False

    rows = load_alert_candidates()
    updated = False
    stamp = logged_at or datetime.now().isoformat(timespec="seconds")
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("alert_id", "")).strip() == target:
            row["is_logged"] = True
            row["logged_at"] = stamp
            updated = True
            break

    if not updated:
        return False

    if _google_backend_enabled():
        try:
            _rewrite_google_alert_rows(rows)
            return True
        except Exception as exc:
            _warn_once(f"Google Sheets alert update failed ({exc}). Writing to local alerts fallback.")

    _write_local_alerts_raw(rows)
    return True
