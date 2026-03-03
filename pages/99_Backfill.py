from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

from storage import append_ledger_row, get_storage_backend_label, load_ledger


LOCAL_LEDGER_PATH = Path(__file__).resolve().parents[1] / "data" / "ev_ledger.json"


def _normalize_part(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _row_key(row: Dict[str, Any]) -> Optional[str]:
    if not isinstance(row, dict):
        return None

    bet_id = _normalize_part(row.get("bet_id"))
    if bet_id:
        return f"id:{bet_id}"

    placed = _normalize_part(row.get("placed_at") or row.get("timestamp"))
    selection = _normalize_part(row.get("selection") or row.get("player"))
    book = _normalize_part(row.get("book"))
    odds = _normalize_part(row.get("odds_american") or row.get("book_odds"))
    return f"sig:{placed}|{selection}|{book}|{odds}"


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("bets", "ledger"):
            val = payload.get(key)
            if isinstance(val, list):
                return [r for r in val if isinstance(r, dict)]
    return []


def _load_local_rows() -> List[Dict[str, Any]]:
    if not LOCAL_LEDGER_PATH.exists():
        st.error(f"Local ledger not found: {LOCAL_LEDGER_PATH}")
        return []

    try:
        with LOCAL_LEDGER_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        st.error(f"Could not read local ledger: {exc}")
        return []

    return _extract_rows(payload)


st.set_page_config(page_title="Admin Backfill", layout="centered")
st.title("Admin / Maintenance: Backfill to Google Sheets")
st.warning("Use this only if you logged bets locally and need to sync them into Google Sheets.")
st.caption("One-time tool: append missing local bets into the Sheets ledger without duplicates.")

backend = get_storage_backend_label()
st.caption(f"Data source: {backend}")

if backend != "Google Sheets":
    st.error("Google Sheets backend is not active. Add Streamlit secrets and reload before running backfill.")
    st.stop()

local_rows = _load_local_rows()
try:
    sheet_rows = load_ledger()
except Exception as exc:
    st.error(f"Could not read sheet ledger: {exc}")
    st.stop()

sheet_keys = {_row_key(r) for r in sheet_rows if isinstance(r, dict)}
missing_rows = []
for row in local_rows:
    k = _row_key(row)
    if k and k not in sheet_keys:
        missing_rows.append(row)

c1, c2, c3 = st.columns(3)
c1.metric("Local Rows", len(local_rows))
c2.metric("Sheet Rows", len(sheet_rows))
c3.metric("Missing Rows", len(missing_rows))

if missing_rows:
    st.caption("Preview of rows that will be appended to Sheets")
    st.dataframe(missing_rows[:100], use_container_width=True)

if st.button("APPEND MISSING TO SHEETS", type="primary", disabled=not missing_rows):
    appended = 0
    failed = 0
    for row in missing_rows:
        try:
            append_ledger_row(row)
            appended += 1
        except Exception:
            failed += 1

    if appended:
        st.success(f"Appended {appended} row(s) to Google Sheets.")
    if failed:
        st.error(f"Failed to append {failed} row(s).")
