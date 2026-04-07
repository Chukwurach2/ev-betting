from datetime import datetime
from html import escape
import math
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from storage import (
    get_storage_backend_label,
    get_storage_diagnostics,
    load_ledger_payload,
    save_ledger_payload,
)


DEFAULT_STARTING_BANKROLL = 500.0
LEDGER_SESSION_PAYLOAD_KEY = "mobile_view_last_good_payload"
RECENT_GRADED_LIMIT = 25
SUPPORTED_STATUSES = {"OPEN", "WON", "LOST", "VOID"}


def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_ts(value: Any) -> pd.Timestamp:
    return pd.to_datetime(value, errors="coerce")


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def profit_on_win(stake: float, odds: float) -> float:
    if odds > 0:
        return stake * (odds / 100.0)
    return stake * (100.0 / abs(odds))


def load_ledger_payload_guarded() -> Dict[str, Any]:
    st.session_state.setdefault(LEDGER_SESSION_PAYLOAD_KEY, None)
    previous_payload = st.session_state.get(LEDGER_SESSION_PAYLOAD_KEY)
    try:
        payload = load_ledger_payload()
    except Exception:
        if previous_payload:
            st.warning("Ledger load failed. Preserving the last known good mobile view state.")
            return dict(previous_payload)
        raise

    storage_meta = payload.get("_storage", {})
    if storage_meta.get("state") == "empty" and previous_payload and previous_payload.get("bets"):
        st.warning("Storage returned an empty ledger after previous data existed. Preserving the last known good mobile view state.")
        return dict(previous_payload)

    payload["starting_bankroll"] = float(
        payload.get("starting_bankroll", DEFAULT_STARTING_BANKROLL) or DEFAULT_STARTING_BANKROLL
    )
    st.session_state[LEDGER_SESSION_PAYLOAD_KEY] = payload
    return payload


def normalize_status(value: Any) -> str:
    status = str(value or "OPEN").upper().strip()
    if status == "W":
        return "WON"
    if status == "L":
        return "LOST"
    if status not in SUPPORTED_STATUSES:
        return "OPEN"
    return status


def display_text(value: Any, fallback: str = "—") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def format_odds(value: Any) -> str:
    odds = parse_float(value)
    if odds is None:
        return "—"
    if odds > 0:
        return f"+{int(odds)}" if float(odds).is_integer() else f"+{odds:g}"
    return f"{int(odds)}" if float(odds).is_integer() else f"{odds:g}"


def format_currency(value: Any) -> str:
    amount = parse_float(value)
    return "—" if amount is None else f"${amount:,.2f}"


def format_date(value: Any) -> str:
    ts = parse_ts(value)
    if pd.isna(ts):
        return "—"
    return ts.strftime("%b %d, %Y %I:%M %p")


def selection_text(row: Dict[str, Any]) -> str:
    return display_text(row.get("selection") or row.get("player") or row.get("team"))


def event_text(row: Dict[str, Any]) -> str:
    team = display_text(row.get("team"), "")
    opponent = display_text(row.get("opponent"), "")
    if team and opponent:
        return f"{team} vs {opponent}"
    if team:
        return team
    if opponent:
        return opponent
    return display_text(row.get("event"))


def player_or_team_text(row: Dict[str, Any]) -> str:
    return display_text(row.get("player") or row.get("team") or row.get("selection"))


def line_text(row: Dict[str, Any]) -> str:
    for key in ("line", "bet_line", "points"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return "—"


def payload_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
    bets = payload.get("bets", []) if isinstance(payload, dict) else []
    rows: List[Dict[str, Any]] = []
    for raw in bets:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["status"] = normalize_status(row.get("status") or row.get("result"))
        row["selection_display"] = selection_text(row)
        row["event_display"] = event_text(row)
        row["player_or_team_display"] = player_or_team_text(row)
        row["league_market_display"] = display_text(row.get("league") or row.get("market"))
        row["prop_display"] = display_text(row.get("market") or row.get("market_type"))
        row["line_display"] = line_text(row)
        row["placed_at_dt"] = parse_ts(row.get("placed_at") or row.get("timestamp"))
        row["settled_at_dt"] = parse_ts(row.get("settled_at"))
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "bet_id",
                "status",
                "sport",
                "league_market_display",
                "event_display",
                "player_or_team_display",
                "prop_display",
                "line_display",
                "selection_display",
                "book",
                "placed_at",
                "placed_at_dt",
                "settled_at",
                "settled_at_dt",
            ]
        )

    if "bet_id" in df.columns:
        df["bet_id"] = df["bet_id"].astype(str)
        df = df.drop_duplicates(subset=["bet_id"], keep="last").copy()

    return df.sort_values(["placed_at_dt", "bet_id"], ascending=[False, False], na_position="last")


def filter_mobile_bets(
    df: pd.DataFrame,
    show_open: bool,
    show_recent: bool,
    sport_filter: str,
    book_filter: str,
) -> Dict[str, pd.DataFrame]:
    filtered = df.copy()
    if sport_filter != "All Sports":
        filtered = filtered[filtered["sport"].fillna("") == sport_filter]
    if book_filter != "All Sportsbooks":
        filtered = filtered[filtered["book"].fillna("") == book_filter]

    open_df = filtered[filtered["status"] == "OPEN"].copy()
    recent_df = (
        filtered[filtered["status"].isin(["WON", "LOST", "VOID"])]
        .sort_values(["settled_at_dt", "placed_at_dt"], ascending=[False, False], na_position="last")
        .head(RECENT_GRADED_LIMIT)
        .copy()
    )

    return {
        "open": open_df if show_open else open_df.iloc[0:0].copy(),
        "recent": recent_df if show_recent else recent_df.iloc[0:0].copy(),
    }


def update_bet_status(payload: Dict[str, Any], bet_id: str, target_status: str) -> None:
    bets = payload.get("bets", []) if isinstance(payload, dict) else []
    target = normalize_status(target_status)
    for row in bets:
        if not isinstance(row, dict):
            continue
        if str(row.get("bet_id", "")).strip() != str(bet_id).strip():
            continue

        stake = parse_float(row.get("stake")) or 0.0
        odds = parse_float(row.get("odds_american"))
        if target == "OPEN":
            row["status"] = "OPEN"
            row["result"] = "OPEN"
            row["settled_at"] = None
            row["pnl"] = 0.0
            row["closing_odds_american"] = None
            return
        if target == "WON":
            if odds is None:
                raise ValueError("Cannot settle WIN without odds.")
            row["status"] = "WON"
            row["result"] = "W"
            row["settled_at"] = now_ts()
            row["pnl"] = profit_on_win(stake, odds)
            return
        if target == "LOST":
            row["status"] = "LOST"
            row["result"] = "L"
            row["settled_at"] = now_ts()
            row["pnl"] = -stake
            return
        row["status"] = "VOID"
        row["result"] = "VOID"
        row["settled_at"] = now_ts()
        row["pnl"] = 0.0
        return

    raise ValueError(f"Bet id not found: {bet_id}")


def persist_payload(payload: Dict[str, Any]) -> None:
    payload_to_save = dict(payload)
    payload_to_save["_previous_remote_row_count"] = int(payload.get("_storage", {}).get("row_count", len(payload.get("bets", []))) or 0)
    save_ledger_payload(payload_to_save)
    refreshed = dict(payload_to_save)
    refreshed["_storage"] = {
        **dict(payload.get("_storage", {})),
        "ok": True,
        "state": "data" if payload.get("bets") else "empty",
        "row_count": len(payload.get("bets", [])),
    }
    st.session_state[LEDGER_SESSION_PAYLOAD_KEY] = refreshed


def status_badge(status: str) -> str:
    return {
        "OPEN": "badge-open",
        "WON": "badge-won",
        "LOST": "badge-lost",
        "VOID": "badge-void",
    }.get(status, "badge-open")


def render_card_html(row: Dict[str, Any], include_settled_meta: bool = False) -> str:
    status = normalize_status(row.get("status"))
    settled_line = ""
    pnl_line = ""
    if include_settled_meta:
        settled_line = f"""
        <div class="mobile-detail"><span class="mobile-label">Settled</span><span>{escape(format_date(row.get("settled_at")))}</span></div>
        """
        pnl_line = f"""
        <div class="mobile-detail"><span class="mobile-label">P/L</span><span>{escape(format_currency(row.get("pnl")))}</span></div>
        """

    return f"""
    <div class="mobile-card">
      <div class="mobile-topline">
        <span class="mobile-status {status_badge(status)}">{escape(status)}</span>
        <span class="mobile-book">{escape(display_text(row.get("book")))}</span>
      </div>
      <div class="mobile-selection-label">Selection</div>
      <div class="mobile-selection">{escape(selection_text(row))}</div>
      <div class="mobile-detail"><span class="mobile-label">Sport</span><span>{escape(display_text(row.get("sport")))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">League / Market</span><span>{escape(display_text(row.get("league") or row.get("market")))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Event / Game</span><span>{escape(event_text(row))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Player / Team</span><span>{escape(player_or_team_text(row))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Prop / Bet Type</span><span>{escape(display_text(row.get("market") or row.get("market_type")))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Line</span><span>{escape(line_text(row))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Odds</span><span>{escape(format_odds(row.get("odds_american")))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Stake</span><span>{escape(format_currency(row.get("stake")))}</span></div>
      <div class="mobile-detail"><span class="mobile-label">Date Placed</span><span>{escape(format_date(row.get("placed_at")))}</span></div>
      {settled_line}
      {pnl_line}
    </div>
    """


def handle_grade_action(payload: Dict[str, Any], bet_id: str, target_status: str, success_label: str) -> None:
    try:
        update_bet_status(payload, bet_id, target_status)
        persist_payload(payload)
    except Exception as exc:
        st.error(f"Could not update bet {bet_id}: {exc}")
        return
    st.success(success_label)
    st.rerun()


st.set_page_config(page_title="Mobile View", layout="centered")
st.title("Mobile View")
st.caption("Phone-first view for open bets, fast grading, and quick status checks.")

st.markdown(
    """
<style>
.mobile-card {
    border: 1px solid rgba(148, 163, 184, 0.45);
    border-radius: 16px;
    padding: 1rem 0.95rem;
    background: linear-gradient(180deg, rgba(248,250,252,0.98), rgba(241,245,249,0.96));
    margin-bottom: 0.45rem;
}
.mobile-topline {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
    flex-wrap: wrap;
}
.mobile-status {
    font-size: 0.8rem;
    font-weight: 700;
    padding: 0.3rem 0.55rem;
    border-radius: 999px;
    letter-spacing: 0.02em;
}
.badge-open {
    background: rgba(37, 99, 235, 0.12);
    color: #1d4ed8;
}
.badge-won {
    background: rgba(22, 163, 74, 0.12);
    color: #15803d;
}
.badge-lost {
    background: rgba(220, 38, 38, 0.12);
    color: #b91c1c;
}
.badge-void {
    background: rgba(148, 163, 184, 0.22);
    color: #334155;
}
.mobile-book {
    color: #475569;
    font-size: 0.88rem;
    font-weight: 600;
}
.mobile-selection-label {
    color: #475569;
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
.mobile-selection {
    font-size: 1.08rem;
    line-height: 1.35;
    font-weight: 800;
    color: #0f172a;
    margin: 0.2rem 0 0.85rem 0;
    word-break: break-word;
    overflow-wrap: anywhere;
}
.mobile-detail {
    display: flex;
    justify-content: space-between;
    gap: 0.9rem;
    padding: 0.28rem 0;
    font-size: 0.92rem;
    color: #0f172a;
}
.mobile-detail span:last-child {
    text-align: right;
    word-break: break-word;
    overflow-wrap: anywhere;
}
.mobile-label {
    color: #475569;
    font-weight: 600;
    min-width: 6.6rem;
}
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div {
    min-height: 2.75rem;
}
button[kind] {
    min-height: 2.9rem;
    font-size: 1rem;
    font-weight: 700;
}
</style>
""",
    unsafe_allow_html=True,
)

try:
    ledger_payload = load_ledger_payload_guarded()
except Exception as exc:
    st.error(f"Could not load ledger ({get_storage_backend_label()} backend).\n\n{exc}")
    st.stop()

df = payload_to_df(ledger_payload)
storage_diag = get_storage_diagnostics()

st.caption(f"Data source: {get_storage_backend_label()}")
if ledger_payload.get("_storage", {}).get("used_fallback"):
    st.warning(
        "Google Sheets load failed, so this page is using local fallback data. "
        f"Reason: {storage_diag.get('last_storage_error') or 'Google backend unavailable'}"
    )

open_count = int((df["status"] == "OPEN").sum()) if not df.empty else 0
recent_count = int(df["status"].isin(["WON", "LOST", "VOID"]).sum()) if not df.empty else 0
m1, m2 = st.columns(2)
m1.metric("Open Bets", open_count)
m2.metric("Settled Bets", recent_count)

st.markdown("### Filters")
f1, f2 = st.columns(2)
with f1:
    show_open_bets = st.checkbox("Show Open Bets", value=True)
with f2:
    show_recent_bets = st.checkbox("Show Recently Graded Bets", value=False)

sport_options = ["All Sports"] + sorted([x for x in df.get("sport", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x.strip()])
book_options = ["All Sportsbooks"] + sorted([x for x in df.get("book", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x.strip()])
s1, s2 = st.columns(2)
with s1:
    selected_sport = st.selectbox("Sport", options=sport_options, index=0)
with s2:
    selected_book = st.selectbox("Sportsbook", options=book_options, index=0)

views = filter_mobile_bets(df, show_open_bets, show_recent_bets, selected_sport, selected_book)
open_view = views["open"]
recent_view = views["recent"]

if not show_open_bets and not show_recent_bets:
    st.info("Turn on at least one view to show bets.")
    st.stop()

if show_open_bets:
    st.markdown("### Open Bets")
    if open_view.empty:
        st.info("No open bets match the current filters.")
    else:
        st.caption("Push and Void both save as VOID in the current ledger schema.")
        open_rows = open_view.to_dict(orient="records")
        for row in open_rows:
            st.markdown(render_card_html(row), unsafe_allow_html=True)
            g1, g2 = st.columns(2)
            with g1:
                if st.button("Win", key=f"win_{row['bet_id']}", use_container_width=True):
                    handle_grade_action(ledger_payload, row["bet_id"], "WON", f"Bet {row['bet_id']} graded WIN.")
            with g2:
                if st.button("Loss", key=f"loss_{row['bet_id']}", use_container_width=True):
                    handle_grade_action(ledger_payload, row["bet_id"], "LOST", f"Bet {row['bet_id']} graded LOSS.")
            g3, g4 = st.columns(2)
            with g3:
                if st.button("Push", key=f"push_{row['bet_id']}", use_container_width=True):
                    handle_grade_action(ledger_payload, row["bet_id"], "VOID", f"Bet {row['bet_id']} graded PUSH (saved as VOID).")
            with g4:
                if st.button("Void", key=f"void_{row['bet_id']}", use_container_width=True):
                    handle_grade_action(ledger_payload, row["bet_id"], "VOID", f"Bet {row['bet_id']} graded VOID.")
            st.markdown("")

if show_recent_bets:
    st.markdown("### Recently Graded Bets")
    st.caption(f"Showing the latest {RECENT_GRADED_LIMIT} settled bets that match your filters.")
    if recent_view.empty:
        st.info("No recently graded bets match the current filters.")
    else:
        for row in recent_view.to_dict(orient="records"):
            st.markdown(render_card_html(row, include_settled_meta=True), unsafe_allow_html=True)
            if st.button("Reset To Open", key=f"reset_{row['bet_id']}", use_container_width=True):
                handle_grade_action(ledger_payload, row["bet_id"], "OPEN", f"Bet {row['bet_id']} reset to OPEN.")
