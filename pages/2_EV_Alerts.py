from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from storage import (
    append_ledger_row,
    get_storage_diagnostics,
    get_storage_backend_label,
    load_alert_candidates,
    mark_alert_logged,
    test_google_sheets_connection,
)


st.set_page_config(page_title="EV Alerts", layout="centered")
st.title("EV Alerts")
st.caption("Live +EV alert candidates generated from EVSharps rules")
st.warning("Alerts are candidates, not auto-submitted bets. Review before logging.")
st.caption(f"Data source: {get_storage_backend_label()}")

with st.expander("Storage Debug", expanded=False):
    diag = get_storage_diagnostics()
    st.write(
        {
            "backend_label": diag.get("backend_label"),
            "google_backend_enabled": diag.get("google_backend_enabled"),
            "has_credentials": diag.get("has_credentials"),
            "has_spreadsheet_target": diag.get("has_spreadsheet_target"),
            "spreadsheet_target": diag.get("spreadsheet_target"),
            "worksheet_name": diag.get("worksheet_name"),
            "alerts_worksheet_name": diag.get("alerts_worksheet_name"),
            "last_storage_error": diag.get("last_storage_error"),
        }
    )
    dc1, dc2 = st.columns(2)
    with dc1:
        if st.button("Test Connection", key="alerts_storage_test_connection"):
            res = test_google_sheets_connection(write_test_row=False)
            if res.get("ok"):
                st.success(res.get("message", "Connection OK"))
            else:
                st.error(f"Connection failed: {res.get('message')}")
    with dc2:
        if st.button("Write Test Ledger Row", key="alerts_storage_write_test_row"):
            res = test_google_sheets_connection(write_test_row=True)
            if res.get("ok"):
                st.success("Test row written to ledger worksheet.")
            else:
                st.error(f"Test row write failed: {res.get('message')}")


def _to_ts(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, errors="coerce")


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_selection(row: Dict[str, Any]) -> str:
    player = _safe_str(row.get("player"))
    market = _safe_str(row.get("market_display") or row.get("prop"))
    if player and market:
        return f"{player} | {market}"
    return player or market or ""


try:
    alerts_raw = load_alert_candidates()
except Exception as exc:
    st.error(f"Could not load alert candidates: {exc}")
    st.stop()

alerts: List[Dict[str, Any]] = [r for r in alerts_raw if isinstance(r, dict)]
if not alerts:
    st.info("No current alert candidates")
    st.stop()

for row in alerts:
    row.setdefault("is_logged", False)
    row.setdefault("timestamp", "")
    row.setdefault("sport", _safe_str(row.get("league")))
    row.setdefault("league", _safe_str(row.get("sport")))
    row["timestamp_dt"] = _to_ts(row.get("timestamp"))
    row.setdefault("zone", "")
    row.setdefault("recommended_book_name", "")
    row.setdefault("ev_pct", None)

alerts_df = pd.DataFrame(alerts)
alerts_df = alerts_df.sort_values("timestamp_dt", ascending=False, na_position="last")

zones = sorted([z for z in alerts_df.get("zone", pd.Series(dtype=str)).dropna().astype(str).unique() if z])
books = sorted([
    b for b in alerts_df.get("recommended_book_name", pd.Series(dtype=str)).dropna().astype(str).unique() if b
])
sports = sorted([
    s for s in alerts_df.get("sport", pd.Series(dtype=str)).dropna().astype(str).unique() if s
])

c1, c2, c3 = st.columns(3)
with c1:
    zone_filter = st.multiselect("Zone", zones, default=zones)
with c2:
    book_filter = st.multiselect("Book", books, default=books)
with c3:
    sport_filter = st.multiselect("Sport", sports, default=sports)

c4, c5 = st.columns(2)
with c4:
    status_filter = st.selectbox("Status", ["All", "Unlogged", "Logged"], index=1)
with c5:
    days_filter = st.selectbox("Date Range", ["Last 1 day", "Last 3 days", "Last 7 days", "Last 30 days", "All"], index=2)

filtered = alerts_df.copy()
if zone_filter:
    filtered = filtered[filtered["zone"].astype(str).isin(zone_filter)]
if book_filter:
    filtered = filtered[filtered["recommended_book_name"].astype(str).isin(book_filter)]
if sport_filter:
    filtered = filtered[filtered["sport"].astype(str).isin(sport_filter)]

if status_filter == "Unlogged":
    filtered = filtered[~filtered["is_logged"].astype(bool)]
elif status_filter == "Logged":
    filtered = filtered[filtered["is_logged"].astype(bool)]

if days_filter != "All":
    days = int(days_filter.split()[1])
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=days))
    filtered = filtered[filtered["timestamp_dt"] >= cutoff]

if filtered.empty:
    st.info("No current alert candidates")
    st.stop()

st.subheader("Recent Alerts")
show_cols = [
    "timestamp",
    "sport",
    "player",
    "market_display",
    "zone",
    "recommended_book_name",
    "recommended_odds",
    "fair_odds",
    "market_odds",
    "ev_pct",
    "gap_cents",
    "recommended_stake",
    "ev_source",
    "is_logged",
    "logged_at",
]

for col in show_cols:
    if col not in filtered.columns:
        filtered[col] = ""

st.dataframe(filtered[show_cols], width="stretch", hide_index=True)

unlogged = filtered[~filtered["is_logged"].astype(bool)].copy()
if unlogged.empty:
    st.success("All filtered alerts are already logged.")
    st.stop()

st.subheader("Log to Ledger")

options = []
id_map: Dict[str, Dict[str, Any]] = {}
for _, r in unlogged.iterrows():
    alert_id = _safe_str(r.get("alert_id"))
    label = f"{_safe_str(r.get('timestamp'))} | {_safe_str(r.get('player'))} | {_safe_str(r.get('market_display'))} | {_safe_str(r.get('recommended_book_name'))} {_safe_str(r.get('recommended_odds'))}"
    if not alert_id:
        continue
    options.append(label)
    id_map[label] = r.to_dict()

if not options:
    st.info("No loggable alerts in the current filter.")
    st.stop()

selected_label = st.selectbox("Select alert", options)
selected = id_map[selected_label]

if st.button("Log to Ledger", type="primary"):
    try:
        alert_ts = _safe_str(selected.get("timestamp")) or datetime.now().isoformat(timespec="seconds")
        market_display = _safe_str(selected.get("market_display") or selected.get("prop"))
        player = _safe_str(selected.get("player"))
        alert_sport = _safe_str(selected.get("sport") or selected.get("league") or "NBA")

        ledger_row = {
            "timestamp": alert_ts,
            "placed_at": alert_ts,
            "sport": alert_sport,
            "league": alert_sport,
            "market": market_display,
            "selection": _build_selection(selected),
            "player": player,
            "book": _safe_str(selected.get("recommended_book_name")),
            "odds_american": selected.get("recommended_odds"),
            "book_odds": selected.get("recommended_odds"),
            "fair_odds_american": selected.get("fair_odds"),
            "fair_odds": selected.get("fair_odds"),
            "ev_pct": selected.get("ev_pct"),
            "devig_method": "Split Weights",
            "devig_details": "PN",
            "stake": 0,
            "kelly_frac": 0,
            "notes": "Logged from EV Alerts page",
            "status": "OPEN",
            "result": "OPEN",
        }

        wrote_to_sheets = append_ledger_row(ledger_row)
        ok = mark_alert_logged(_safe_str(selected.get("alert_id")), datetime.now().isoformat(timespec="seconds"))
        if not wrote_to_sheets:
            diag_now = get_storage_diagnostics()
            st.warning(
                "Ledger row was written via local fallback (not Google Sheets). "
                f"Reason: {diag_now.get('last_storage_error') or 'Google backend unavailable'}"
            )
        if not ok:
            st.warning("Bet logged, but alert status update did not persist. Refresh and check the alert queue.")
        else:
            st.success("Alert logged to ledger and marked as logged.")
        st.rerun()
    except Exception as exc:
        st.error(f"Could not log this alert: {exc}")
