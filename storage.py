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
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections.abc import Mapping


import streamlit as st

LOCAL_DATA_DIR = Path(__file__).resolve().parent / "data"
LOCAL_LEDGER_PATH = LOCAL_DATA_DIR / "ev_ledger.json"
LOCAL_ALERTS_PATH = LOCAL_DATA_DIR / "ev_alerts.json"
LOCAL_BACKUP_DIR = LOCAL_DATA_DIR / "backups"
DEFAULT_WORKSHEET = "ledger"
DEFAULT_ALERTS_WORKSHEET = "alerts"
DEFAULT_STARTING_BANKROLL = 500.0

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
    "recommended_stake",
    "bankroll_snapshot",
    "kelly_fraction_used",
    "max_stake_pct",
    "min_stake",
    "round_to",
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
    "recommended_stake",
    "bankroll_snapshot",
    "max_stake_pct",
    "min_stake",
    "round_to",
}

BOOL_FIELDS = {"is_live", "is_parlay", "is_logged"}
_LAST_STORAGE_ERROR: Optional[str] = None


@dataclass
class StorageReadResult:
    ok: bool
    state: str
    rows: List[Dict[str, Any]]
    source: str
    row_count: int
    error: Optional[str] = None
    worksheet_name: Optional[str] = None
    spreadsheet_target: Optional[str] = None
    used_fallback: bool = False


@dataclass
class StorageWriteResult:
    ok: bool
    source: str
    row_count_before: int
    row_count_after: int
    mode: str
    blocked: bool = False
    error: Optional[str] = None
    backup_path: Optional[str] = None


def _storage_debug_enabled() -> bool:
    secret_flag = _secrets_get("storage_debug", None)
    if secret_flag is not None:
        return str(secret_flag).strip().lower() in {"1", "true", "yes", "on"}
    return os.getenv("STORAGE_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}


def _storage_log(message: str, *, always: bool = False) -> None:
    if not always and not _storage_debug_enabled():
        return
    print(f"[storage] {message}")


def _set_last_storage_error(message: str) -> None:
    global _LAST_STORAGE_ERROR
    _LAST_STORAGE_ERROR = message
    _storage_log(message, always=True)


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


def _env_json_or_mapping(key: str) -> Optional[Mapping[str, Any]]:
    raw = os.getenv(key, "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, Mapping):
            return parsed
    except Exception:
        return None
    return None


def _gsheets_config() -> Dict[str, Any]:
    creds = _secrets_get("gcp_service_account")
    if not isinstance(creds, Mapping) or not creds:
        creds = _env_json_or_mapping("GCP_SERVICE_ACCOUNT_JSON")

    spreadsheet_id = _secrets_get("spreadsheet_id") or os.getenv("SPREADSHEET_ID", "").strip()
    spreadsheet_name = _secrets_get("spreadsheet_name") or os.getenv("SPREADSHEET_NAME", "").strip()
    worksheet_name = (
        _secrets_get("worksheet_name", DEFAULT_WORKSHEET)
        or os.getenv("WORKSHEET_NAME", DEFAULT_WORKSHEET).strip()
        or DEFAULT_WORKSHEET
    )
    alerts_worksheet_name = (
        _secrets_get("alerts_worksheet_name", DEFAULT_ALERTS_WORKSHEET)
        or os.getenv("ALERTS_WORKSHEET_NAME", DEFAULT_ALERTS_WORKSHEET).strip()
        or DEFAULT_ALERTS_WORKSHEET
    )
    return {
        "credentials": creds,
        "spreadsheet_id": spreadsheet_id or None,
        "spreadsheet_name": spreadsheet_name or None,
        "worksheet_name": worksheet_name,
        "alerts_worksheet_name": alerts_worksheet_name,
    }

def _google_backend_enabled() -> bool:
    cfg = _gsheets_config()
    creds = cfg.get("credentials")
    has_creds = isinstance(creds, Mapping) and bool(creds)
    has_sheet = bool(cfg.get("spreadsheet_id") or cfg.get("spreadsheet_name"))
    _storage_log(
        f"google_backend_enabled={has_creds and has_sheet} has_creds={has_creds} "
        f"sheet_target={(cfg.get('spreadsheet_id') or cfg.get('spreadsheet_name') or '')} "
        f"worksheet={cfg.get('worksheet_name')} alerts_worksheet={cfg.get('alerts_worksheet_name')}"
    )
    return has_creds and has_sheet


def get_storage_backend_label() -> str:
    return "Google Sheets" if _google_backend_enabled() else "Local JSON"


@st.cache_resource(show_spinner=False)
def _get_gspread_worksheet(create_if_missing: bool = False):
    import gspread
    from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

    cfg = _gsheets_config()
    _storage_log(
        f"init_ledger_ws target={(cfg.get('spreadsheet_id') or cfg.get('spreadsheet_name') or '')} "
        f"worksheet={cfg.get('worksheet_name')}"
    )
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
        if not create_if_missing:
            raise
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=80)

    _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS)
    return ws


@st.cache_resource(show_spinner=False)
def _get_gspread_alerts_worksheet(create_if_missing: bool = False):
    import gspread
    from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

    cfg = _gsheets_config()
    _storage_log(
        f"init_alerts_ws target={(cfg.get('spreadsheet_id') or cfg.get('spreadsheet_name') or '')} "
        f"worksheet={cfg.get('alerts_worksheet_name')}"
    )
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
        if not create_if_missing:
            raise
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


def _ensure_local_backup_dir() -> None:
    LOCAL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def _write_backup_snapshot(name: str, payload: Any) -> str:
    _ensure_local_backup_dir()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = LOCAL_BACKUP_DIR / f"{name}_{stamp}.json"
    with backup_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    _storage_log(f"backup_snapshot_written path={backup_path}", always=True)
    return str(backup_path)


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
    normalized.setdefault("recommended_stake", "")
    normalized.setdefault("bankroll_snapshot", "")
    normalized.setdefault("kelly_fraction_used", "")
    normalized.setdefault("max_stake_pct", "")
    normalized.setdefault("min_stake", "")
    normalized.setdefault("round_to", "")
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


def _rows_from_sheet_values(values: List[List[Any]]) -> List[Dict[str, Any]]:
    if not values:
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


def _load_google_rows_result() -> StorageReadResult:
    cfg = _gsheets_config()
    target = cfg.get("spreadsheet_id") or cfg.get("spreadsheet_name") or ""
    worksheet_name = str(cfg.get("worksheet_name") or DEFAULT_WORKSHEET)
    try:
        ws = _get_gspread_worksheet()
        values = ws.get_all_values()
        if not values:
            _storage_log(
                f"load_ledger_google_success target_ws={worksheet_name} row_count=0 state=empty",
                always=True,
            )
            return StorageReadResult(
                ok=True,
                state="empty",
                rows=[],
                source="google_sheets",
                row_count=0,
                worksheet_name=worksheet_name,
                spreadsheet_target=str(target),
            )

        rows = _rows_from_sheet_values(values)
        state = "data" if rows else "empty"
        _storage_log(
            f"load_ledger_google_success target_ws={worksheet_name} raw_rows={max(len(values) - 1, 0)} row_count={len(rows)} state={state}",
            always=True,
        )
        return StorageReadResult(
            ok=True,
            state=state,
            rows=rows,
            source="google_sheets",
            row_count=len(rows),
            worksheet_name=worksheet_name,
            spreadsheet_target=str(target),
        )
    except Exception as exc:
        message = f"Google Sheets ledger read failed from worksheet '{worksheet_name}': {exc}"
        _set_last_storage_error(message)
        _storage_log(
            f"load_ledger_google_failed target_ws={worksheet_name} error={exc}",
            always=True,
        )
        return StorageReadResult(
            ok=False,
            state="failed",
            rows=[],
            source="google_sheets",
            row_count=0,
            error=str(exc),
            worksheet_name=worksheet_name,
            spreadsheet_target=str(target),
        )


def _append_google_row(row: Dict[str, Any]) -> None:
    ws = _get_gspread_worksheet()
    normalized = _normalize_row_for_schema(row)
    headers = _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS, list(normalized.keys()))
    values = [_serialize_value(normalized.get(h)) for h in headers]
    _storage_log(
        f"append_ledger_row target_ws={ws.title} headers={len(headers)} payload={json.dumps(normalized, default=str)[:2000]}"
    )
    ws.append_row(values, value_input_option="USER_ENTERED")
    _storage_log(f"append_ledger_row_success target_ws={ws.title}", always=True)


def _load_google_alert_rows() -> List[Dict[str, Any]]:
    ws = _get_gspread_alerts_worksheet()
    values = ws.get_all_values()
    return _rows_from_sheet_values(values)


def _append_google_alert_row(row: Dict[str, Any]) -> None:
    ws = _get_gspread_alerts_worksheet()
    normalized = _normalize_alert_row_for_schema(row)
    headers = _ensure_sheet_headers(ws, REQUIRED_ALERT_COLUMNS, list(normalized.keys()))
    values = [_serialize_value(normalized.get(h)) for h in headers]
    _storage_log(
        f"append_alert_row target_ws={ws.title} headers={len(headers)} payload={json.dumps(normalized, default=str)[:2000]}"
    )
    ws.append_row(values, value_input_option="USER_ENTERED")
    _storage_log(f"append_alert_row_success target_ws={ws.title}", always=True)


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
    except (OSError, json.JSONDecodeError) as exc:
        message = f"Local ledger file is missing/corrupt: {exc}"
        _set_last_storage_error(message)
        _warn_once(f"{message}. Preserving remote state and treating local fallback as unavailable.")
        raise


def _read_local_alerts_raw() -> Any:
    _ensure_local_alerts_file()
    try:
        with LOCAL_ALERTS_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        message = f"Local alerts file is missing/corrupt: {exc}"
        _set_last_storage_error(message)
        _warn_once(f"{message}. Treating local alerts fallback as unavailable.")
        raise


def _write_local_raw(raw: Any) -> None:
    _ensure_local_ledger_file()
    with LOCAL_LEDGER_PATH.open("w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)


def _write_local_alerts_raw(raw: Any) -> None:
    _ensure_local_alerts_file()
    with LOCAL_ALERTS_PATH.open("w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)


def _parse_local_ledger_rows(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, dict):
        bets = raw.get("bets", [])
        return [b for b in bets if isinstance(b, dict)] if isinstance(bets, list) else []
    if isinstance(raw, list):
        return [b for b in raw if isinstance(b, dict)]
    return []


def _extract_bankroll_and_unit(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    starting_bankroll = DEFAULT_STARTING_BANKROLL
    unit_size = 1.0
    found_bankroll = False
    found_unit = False
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        sb = row.get("starting_bankroll")
        us = row.get("unit_size")
        if not found_bankroll and sb not in (None, ""):
            try:
                starting_bankroll = float(sb)
                found_bankroll = True
            except (TypeError, ValueError):
                pass
        if not found_unit and us not in (None, ""):
            try:
                unit_size = float(us)
                found_unit = True
            except (TypeError, ValueError):
                pass
        if found_bankroll and found_unit:
            break
    return {
        "starting_bankroll": starting_bankroll,
        "unit_size": unit_size,
        "found_bankroll": float(1 if found_bankroll else 0),
        "found_unit": float(1 if found_unit else 0),
    }


def _load_local_ledger_result() -> StorageReadResult:
    raw = _read_local_raw()
    rows = _parse_local_ledger_rows(raw)
    state = "data" if rows else "empty"
    _storage_log(f"load_ledger_local_success path={LOCAL_LEDGER_PATH} row_count={len(rows)} state={state}", always=True)
    return StorageReadResult(
        ok=True,
        state=state,
        rows=rows,
        source="local_json",
        row_count=len(rows),
        worksheet_name=None,
        spreadsheet_target=str(LOCAL_LEDGER_PATH),
    )


def load_ledger_read_result() -> StorageReadResult:
    """Return an explicit read result so callers can distinguish empty vs failed loads."""
    if _google_backend_enabled():
        google_result = _load_google_rows_result()
        if google_result.ok:
            return google_result
        _warn_once(
            f"Google Sheets ledger read failed ({google_result.error}). Using local ledger fallback without modifying remote data."
        )
        local_result = _load_local_ledger_result()
        local_result.used_fallback = True
        return local_result

    return _load_local_ledger_result()


def load_ledger() -> List[Dict[str, Any]]:
    """Load ledger rows as a list of dicts from Sheets (primary) or local JSON fallback."""
    return load_ledger_read_result().rows


def append_ledger_row(row: Dict[str, Any]) -> bool:
    """Append one row to Sheets (primary) or local JSON fallback. Returns True if Sheets write succeeded."""
    row_to_write = dict(row) if isinstance(row, dict) else {}
    if not str(row_to_write.get("bet_id", "")).strip():
        row_to_write["bet_id"] = str(uuid.uuid4())[:8]

    if _google_backend_enabled():
        try:
            before = _load_google_rows_result()
            if not before.ok:
                raise RuntimeError(
                    f"Pre-append ledger read failed for worksheet '{before.worksheet_name}': {before.error}"
                )
            _append_google_row(row_to_write)
            after = _load_google_rows_result()
            _storage_log(
                f"append_ledger_row_counts target_ws={before.worksheet_name} row_count_before={before.row_count} "
                f"row_count_after={after.row_count if after.ok else 'unknown'}",
                always=True,
            )
            return True
        except Exception as exc:
            _set_last_storage_error(f"Google Sheets append failed: {exc}")
            _warn_once(f"Google Sheets append failed ({exc}). Writing to local ledger fallback.")
    else:
        _set_last_storage_error("Google Sheets backend disabled. Writing to local ledger fallback.")

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
    _storage_log("append_ledger_row_local_fallback", always=True)
    return False


def load_ledger_payload() -> Dict[str, Any]:
    """Compatibility payload for existing app behavior."""
    result = load_ledger_read_result()
    rows = result.rows

    metadata = _extract_bankroll_and_unit(rows)
    starting_bankroll = metadata["starting_bankroll"] if result.ok else 0.0
    unit_size = metadata["unit_size"]

    bets = [_sheet_row_to_app_bet(r, unit_size) for r in rows if isinstance(r, dict)]
    return {
        "starting_bankroll": starting_bankroll,
        "unit_size": unit_size,
        "bets": bets,
        "_storage": asdict(result),
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
        remote_before = _load_google_rows_result()
        previous_count = int(payload.get("_previous_remote_row_count") or 0)
        effective_remote_count = max(remote_before.row_count, previous_count)
        if not remote_before.ok:
            message = (
                "Blocked Google Sheets ledger overwrite because the pre-write read failed. "
                f"Worksheet '{remote_before.worksheet_name}' error: {remote_before.error}"
            )
            _set_last_storage_error(message)
            _warn_once(message)
            raise RuntimeError(message)

        if remote_before.row_count == 0 and previous_count > 0:
            message = (
                "Blocked Google Sheets ledger overwrite because the latest verified remote row count dropped to zero "
                f"after previously loading data. previous_rows={previous_count} current_rows=0"
            )
            _set_last_storage_error(message)
            _storage_log(message, always=True)
            _warn_once(message)
            raise RuntimeError(message)

        looks_empty_payload = len(enriched_rows) == 0
        remote_meta = _extract_bankroll_and_unit(remote_before.rows)
        bankroll_reset = (
            float(payload.get("starting_bankroll", DEFAULT_STARTING_BANKROLL) or DEFAULT_STARTING_BANKROLL) == DEFAULT_STARTING_BANKROLL
            and effective_remote_count > 0
            and remote_meta["starting_bankroll"] != DEFAULT_STARTING_BANKROLL
        )
        if effective_remote_count > 0 and (looks_empty_payload or bankroll_reset):
            message = (
                "Blocked destructive Google Sheets ledger overwrite because the new payload is empty or resets bankroll "
                f"to the default while existing rows are present. existing_rows={effective_remote_count} "
                f"new_rows={len(enriched_rows)} starting_bankroll={payload.get('starting_bankroll')}"
            )
            _set_last_storage_error(message)
            _storage_log(message, always=True)
            _warn_once(message)
            raise RuntimeError(message)

        if remote_before.row_count > 0 and len(enriched_rows) == 0:
            message = (
                "Blocked destructive Google Sheets ledger overwrite because loaded row count dropped from a non-zero "
                f"value to zero. existing_rows={remote_before.row_count}"
            )
            _set_last_storage_error(message)
            _storage_log(message, always=True)
            _warn_once(message)
            raise RuntimeError(message)

        backup_path = _write_backup_snapshot(
            "ledger_pre_full_write",
            {
                "captured_at": datetime.now().isoformat(timespec="seconds"),
                "worksheet_name": remote_before.worksheet_name,
                "spreadsheet_target": remote_before.spreadsheet_target,
                "row_count": remote_before.row_count,
                "rows": remote_before.rows,
            },
        )

        try:
            _rewrite_google_rows(enriched_rows)
            _storage_log(
                f"rewrite_ledger_success target_ws={remote_before.worksheet_name} row_count_before={remote_before.row_count} "
                f"row_count_after={len(enriched_rows)} backup={backup_path}",
                always=True,
            )
            return
        except Exception as exc:
            message = f"Google Sheets save failed after backup {backup_path}: {exc}"
            _set_last_storage_error(message)
            _warn_once(message)
            raise

    local_payload = {
        "starting_bankroll": float(payload.get("starting_bankroll", DEFAULT_STARTING_BANKROLL) or DEFAULT_STARTING_BANKROLL),
        "unit_size": float(payload.get("unit_size", 1.0) or 1.0),
        "bets": bets,
    }
    _storage_log(f"save_ledger_payload_local row_count={len(bets)}", always=True)
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
            _set_last_storage_error(f"Google Sheets alert append failed: {exc}")
            _warn_once(f"Google Sheets alert append failed ({exc}). Writing to local alerts fallback.")
    else:
        _set_last_storage_error("Google Sheets backend disabled for alerts. Writing to local alerts fallback.")

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
    _storage_log("append_alert_candidate_local_fallback", always=True)
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
            _set_last_storage_error(f"Google Sheets alert update failed: {exc}")
            _warn_once(f"Google Sheets alert update failed ({exc}). Writing to local alerts fallback.")

    _write_local_alerts_raw(rows)
    return True


def get_storage_diagnostics() -> Dict[str, Any]:
    cfg = _gsheets_config()
    creds = cfg.get("credentials")
    has_creds = isinstance(creds, Mapping) and bool(creds)
    has_sheet_target = bool(cfg.get("spreadsheet_id") or cfg.get("spreadsheet_name"))
    return {
        "backend_label": get_storage_backend_label(),
        "google_backend_enabled": bool(has_creds and has_sheet_target),
        "has_credentials": has_creds,
        "has_spreadsheet_target": has_sheet_target,
        "spreadsheet_target": cfg.get("spreadsheet_id") or cfg.get("spreadsheet_name") or "",
        "worksheet_name": cfg.get("worksheet_name"),
        "alerts_worksheet_name": cfg.get("alerts_worksheet_name"),
        "storage_debug_enabled": _storage_debug_enabled(),
        "last_storage_error": _LAST_STORAGE_ERROR,
    }


def test_google_sheets_connection(write_test_row: bool = False) -> Dict[str, Any]:
    diag = get_storage_diagnostics()
    if not diag["google_backend_enabled"]:
        return {
            "ok": False,
            "message": "Google backend is disabled (missing credentials or spreadsheet target).",
            **diag,
        }
    try:
        ws = _get_gspread_worksheet()
        _ensure_sheet_headers(ws, REQUIRED_LEDGER_COLUMNS)
        if write_test_row:
            row = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "league": "SYSTEM",
                "market": "CONNECTIVITY_TEST",
                "player": "storage.test",
                "book": "SYSTEM",
                "book_odds": 0,
                "fair_odds": 0,
                "stake": 0,
                "kelly_frac": 0,
                "notes": "Connectivity test row",
                "result": "OPEN",
            }
            _append_google_row(row)
        return {
            "ok": True,
            "message": "Google Sheets connection succeeded.",
            **diag,
        }
    except Exception as exc:
        _set_last_storage_error(f"Google Sheets connectivity test failed: {exc}")
        return {
            "ok": False,
            "message": str(exc),
            **diag,
        }
