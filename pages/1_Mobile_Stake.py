import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import streamlit as st
from storage import load_ledger_payload, append_ledger_row, get_storage_backend_label


# -----------------------------
# Odds + sizing helpers
# -----------------------------
def parse_american_odds(raw: str) -> float:
    txt = str(raw).strip().replace(",", "")
    if not txt:
        raise ValueError("Odds are required.")
    try:
        val = float(txt)
    except Exception as exc:
        raise ValueError("Enter valid American odds (e.g., -110, +125).") from exc
    if val == 0:
        raise ValueError("American odds cannot be 0.")
    return val


def american_to_decimal(odds: float) -> float:
    if odds == 0:
        raise ValueError("Odds cannot be 0.")
    if odds > 0:
        return 1.0 + (odds / 100.0)
    return 1.0 + (100.0 / abs(odds))


def decimal_to_american(dec: float) -> float:
    if dec <= 1.0:
        raise ValueError("Decimal odds must be > 1.0.")
    if dec >= 2.0:
        return (dec - 1.0) * 100.0
    return -100.0 / (dec - 1.0)


def american_implied_prob(odds: float) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def kelly_fraction_from_prob(p: float, odds: float) -> float:
    dec = american_to_decimal(odds)
    b = dec - 1.0
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)


def round_to(x: float, step: float) -> float:
    if step <= 0:
        return x
    return round(x / step) * step


def recommend_stake(
    bankroll: float,
    unit_size: float,
    odds_american: float,
    fair_odds_american: Optional[float] = None,
    true_prob: Optional[float] = None,
    kelly_fraction: float = 0.25,
    max_fraction_of_bankroll: float = 0.03,
    min_stake: float = 1.0,
    round_step: float = 0.25,
) -> Dict[str, Any]:
    if true_prob is None:
        if fair_odds_american is None:
            raise ValueError("Provide Fair Odds or True Probability.")
        p_used = american_implied_prob(float(fair_odds_american))
    else:
        p_used = float(true_prob)

    full_kelly = kelly_fraction_from_prob(p_used, odds_american)
    raw_stake = float(bankroll) * full_kelly * float(kelly_fraction)
    cap_amount = float(bankroll) * float(max_fraction_of_bankroll)
    post_cap = min(raw_stake, cap_amount)
    stake = round_to(max(float(min_stake), post_cap), float(round_step))

    dec = american_to_decimal(float(odds_american))
    b = dec - 1.0
    ev_per_dollar = p_used * b - (1.0 - p_used)

    return {
        "recommended_stake": stake,
        "raw_stake_before_cap": raw_stake,
        "cap_amount": cap_amount,
        "stake_after_cap_before_min_round": post_cap,
        "was_capped": raw_stake > cap_amount,
        "ev_per_dollar": ev_per_dollar,
        "true_prob": p_used,
        "full_kelly_fraction": full_kelly,
        "unit_size": float(unit_size),
    }


# -----------------------------
# Ledger helpers
# -----------------------------
def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def realized_bankroll(payload: Dict[str, Any]) -> float:
    start = float(payload.get("starting_bankroll", 0.0))
    bets = payload.get("bets", [])
    pnl = 0.0
    for b in bets:
        if str(b.get("status", "")).upper() in {"WON", "LOST", "VOID"}:
            pnl += float(b.get("pnl", 0.0) or 0.0)
    return start + pnl


def add_open_bet(payload: Dict[str, Any], bet: Dict[str, Any]) -> str:
    bet_id = str(uuid.uuid4())[:8]
    rec = {
        "bet_id": bet_id,
        "placed_at": now_ts(),
        "sport": bet["sport"],
        "market": bet["market"],
        "selection": bet["selection"],
        "book": bet["book"],
        "odds_american": float(bet["odds_american"]),
        "stake": float(bet["stake"]),
        "unit_size": float(payload.get("unit_size", 1.0)),
        "market_type": bet.get("market_type", "Game"),
        "team": bet.get("team") or None,
        "opponent": bet.get("opponent") or None,
        "devig_method": bet.get("devig_method", "Market Avg"),
        "devig_details": bet.get("devig_details") or None,
        "recommended_stake_snapshot": float(bet.get("recommended_stake_snapshot")) if bet.get("recommended_stake_snapshot") is not None else None,
        "stake_source": bet.get("stake_source") or None,
        "is_live": False,
        "is_parlay": False,
        "parlay_leg_count": None,
        "parlay_boost_pct": None,
        "parlay_unboosted_odds_american": None,
        "parlay_boosted_odds_american": None,
        "parlay_true_prob": None,
        "parlay_legs": None,
        "fair_odds_american": float(bet.get("fair_odds_american")) if bet.get("fair_odds_american") is not None else None,
        "true_prob": float(bet.get("true_prob")) if bet.get("true_prob") is not None else None,
        "ev_pct": float(bet.get("ev_pct")) if bet.get("ev_pct") is not None else None,
        "kelly_fraction_used": float(bet.get("kelly_fraction_used")) if bet.get("kelly_fraction_used") is not None else None,
        "kelly_units_from_tool": None,
        "boost_pct": None,
        "unboosted_odds_american": None,
        "closing_odds_american": None,
        "status": "OPEN",
        "settled_at": None,
        "pnl": 0.0,
        "notes": bet.get("notes") or None,
    }
    payload.setdefault("bets", []).append(rec)
    return bet_id


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Mobile Stake", layout="centered")
st.title("Mobile Stake")
st.caption("Quick stake sizing + one-tap logging for phone use.")

st.markdown(
    """
<style>
div[data-testid="stMetric"] { padding: 0.35rem 0.1rem; }
div[data-baseweb="input"] input { font-size: 1.1rem; }
button[kind="primary"] { min-height: 2.8rem; font-size: 1.05rem; }
</style>
""",
    unsafe_allow_html=True,
)

ledger_payload = load_ledger_payload()
st.caption(f"Ledger backend: `{get_storage_backend_label()}`")

unit_size = float(ledger_payload.get("unit_size", 1.0))
br = realized_bankroll(ledger_payload)

# Uses same presets as New Bet tab sizing settings.
KELLY_FRACTION = 0.25
MAX_FRAC_BR = 0.03
MIN_STAKE = 1.0
ROUND_STEP = 0.25

m1, m2, m3 = st.columns(3)
m1.metric("Realized BR", f"${br:.2f}")
m2.metric("Unit Size", f"${unit_size:.2f}")
m3.metric("Preset", "0.25 Kelly / 3% cap")

st.markdown("### Odds Input")
book_odds_raw = st.text_input("Book Odds (American)", value="-110", placeholder="e.g., -110 or +140")
fair_odds_raw = st.text_input("Fair Odds (American)", value="", placeholder="Optional if True Prob provided")
true_prob_raw = st.text_input("True Probability (0-1)", value="", placeholder="Optional if Fair Odds provided")

reco = None
parse_err = None
try:
    book_odds = parse_american_odds(book_odds_raw)
    fair_odds = parse_american_odds(fair_odds_raw) if fair_odds_raw.strip() else None
    true_prob = float(true_prob_raw) if true_prob_raw.strip() else None
    if true_prob is not None and not (0.0 < true_prob < 1.0):
        raise ValueError("True probability must be between 0 and 1.")

    if fair_odds is None and true_prob is None:
        st.info("Enter Fair Odds or True Probability to compute a recommendation.")
    else:
        reco = recommend_stake(
            bankroll=br,
            unit_size=unit_size,
            odds_american=book_odds,
            fair_odds_american=fair_odds,
            true_prob=true_prob,
            kelly_fraction=KELLY_FRACTION,
            max_fraction_of_bankroll=MAX_FRAC_BR,
            min_stake=MIN_STAKE,
            round_step=ROUND_STEP,
        )
except Exception as e:
    parse_err = str(e)

if parse_err:
    st.error(parse_err)

if reco is not None:
    st.markdown("### Suggested Stake")
    r1, r2 = st.columns(2)
    r3, r4 = st.columns(2)
    r1.metric("Recommended", f"${reco['recommended_stake']:.2f}")
    r2.metric("Cap Status", "CAPPED" if reco["was_capped"] else "Not capped")
    r3.metric("Raw Before Cap", f"${reco['raw_stake_before_cap']:.2f}")
    r4.metric("Cap Amount", f"${reco['cap_amount']:.2f}")

    p_used = reco["true_prob"]
    ev_pct = float(reco["ev_per_dollar"]) * 100.0 if reco.get("ev_per_dollar") is not None else None
    i1, i2, i3 = st.columns(3)
    i1.metric("Book Implied", f"{american_implied_prob(book_odds)*100:.2f}%")
    i2.metric("Fair/True Prob", f"{p_used*100:.2f}%")
    i3.metric("EV per $", "N/A" if ev_pct is None else f"{ev_pct:+.2f}%")

    st.caption(
        f"Path: raw ${reco['raw_stake_before_cap']:.2f} -> cap ${reco['cap_amount']:.2f} -> "
        f"post-cap ${reco['stake_after_cap_before_min_round']:.2f} -> rounded ${reco['recommended_stake']:.2f}"
    )

st.markdown("### Log New Bet")
s1, s2 = st.columns(2)
with s1:
    sport = st.text_input("Sport", value="NBA")
    market = st.text_input("Market", value="Moneyline")
    market_type = st.selectbox("Market Type", ["Game", "Team", "Player", "Period", "Other"], index=0)
    selection = st.text_input("Selection", value="")
with s2:
    team = st.text_input("Team (optional)", value="")
    opponent = st.text_input("Opponent (optional)", value="")
    book = st.selectbox("Book", ["DraftKings", "FanDuel", "BetMGM", "Caesars", "Fanatics", "BetRivers", "theScore", "Pinnacle"], index=0)
    devig_method = st.selectbox("Devig Method", ["Market Avg", "Single Book (100%)", "Split Weights"], index=0)

devig_details = st.text_input("Devig Details (optional)", value="", placeholder="e.g., Pinnacle 100%")
notes = st.text_input("Notes (optional)", value="")

stake_mode = st.radio(
    "Stake Mode",
    options=["Use suggested stake", "Manual stake"],
    horizontal=True,
    index=0 if reco is not None else 1,
)
manual_stake = st.number_input("Manual stake ($)", min_value=0.0, value=1.0, step=0.25)
confirm = st.checkbox("Confirm add as OPEN", value=False)
log_this_bet = st.checkbox("Log this bet", value=True)

if st.button("Add OPEN Bet", type="primary", use_container_width=True, disabled=not confirm):
    try:
        if not selection.strip():
            raise ValueError("Selection is required.")
        if not sport.strip() or not market.strip() or not book.strip():
            raise ValueError("Sport, Market, and Book are required.")

        if devig_method != "Market Avg" and not devig_details.strip():
            raise ValueError("Devig Details required for Single Book (100%) / Split Weights.")

        if stake_mode == "Manual stake":
            stake = float(manual_stake)
            if stake <= 0:
                raise ValueError("Manual stake must be > 0.")
            stake_source = "Manual"
            rec_snapshot = float(reco["recommended_stake"]) if reco is not None else None
        else:
            if reco is None:
                raise ValueError("Suggestion unavailable. Enter Fair Odds or True Probability, or switch to Manual stake.")
            stake = float(reco["recommended_stake"])
            stake_source = "Recommended"
            rec_snapshot = float(reco["recommended_stake"])

        fair_odds = parse_american_odds(fair_odds_raw) if fair_odds_raw.strip() else None
        true_prob = float(true_prob_raw) if true_prob_raw.strip() else None
        ev_pct = (float(reco["ev_per_dollar"]) * 100.0) if reco and reco.get("ev_per_dollar") is not None else None

        row = {
            "sport": sport.strip(),
            "league": sport.strip(),
            "team": team.strip() or None,
            "opponent": opponent.strip() or None,
            "market": market.strip(),
            "market_type": market_type,
            "selection": selection.strip(),
            "player": selection.strip(),
            "book": book,
            "devig_method": devig_method,
            "devig_details": devig_details.strip() or None,
            "recommended_stake_snapshot": rec_snapshot,
            "stake_source": stake_source,
            "odds_american": parse_american_odds(book_odds_raw),
            "book_odds": parse_american_odds(book_odds_raw),
            "stake": stake,
            "fair_odds_american": fair_odds,
            "fair_odds": fair_odds,
            "true_prob": true_prob,
            "ev_pct": ev_pct,
            "kelly_fraction_used": KELLY_FRACTION if stake_source == "Recommended" else None,
            "kelly_frac": KELLY_FRACTION if stake_source == "Recommended" else None,
            "notes": notes.strip() or None,
            "status": "OPEN",
            "result": "OPEN",
            "timestamp": now_ts(),
            "placed_at": now_ts(),
            "starting_bankroll": float(ledger_payload.get("starting_bankroll", 0.0)),
            "unit_size": float(ledger_payload.get("unit_size", 1.0)),
        }
        bet_id = str(uuid.uuid4())[:8]
        row["bet_id"] = bet_id
        if log_this_bet:
            append_ledger_row(row)
            st.success(f"Added OPEN bet: {bet_id}")
        else:
            st.success("Bet not logged (toggle is off).")
    except Exception as e:
        st.error(f"Failed to add bet: {e}")
