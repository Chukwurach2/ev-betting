# app.py
# EV Betting Dashboard - Streamlit UI (Revised v2)
# Fixes:
# - Removes use_container_width (uses width="stretch")
# - Guarantees placed_at_dt / settled_at_dt columns + safe sorting
# - Sort FIRST, then select columns (fixes KeyError: placed_at_dt)
# - 3-button grading that updates KPIs instantly
#
# Run:
#   source venv/bin/activate
#   streamlit run app.py

import json
import math
import calendar
import re
import uuid
import storage
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd
import streamlit as st
import altair as alt
from storage import load_ledger_payload, save_ledger_payload, get_storage_backend_label

# --- DEBUG (temporary) ---
try:
    rows = storage.load_ledger()
    st.sidebar.markdown("### Debug")
    st.sidebar.write("Backend:", storage.get_storage_backend_label())
    st.sidebar.write("Rows:", len(rows))
    st.sidebar.write("worksheet_name:", st.secrets.get("worksheet_name"))
    st.sidebar.write("spreadsheet_id:", st.secrets.get("spreadsheet_id"))
    st.sidebar.write("spreadsheet_name:", st.secrets.get("spreadsheet_name"))
    st.sidebar.write("Has gcp_service_account:", "gcp_service_account" in st.secrets)
    st.sidebar.write("gcp_service_account type:", type(st.secrets.get("gcp_service_account")).__name__)
except Exception as e:
    st.sidebar.error(f"Debug error: {e}")
# --- END DEBUG ---


# -----------------------------
# Odds helpers
# -----------------------------
def american_to_decimal(odds: float) -> float:
    if odds == 0:
        raise ValueError("Odds cannot be 0.")
    if odds > 0:
        return 1 + odds / 100.0
    return 1 + 100.0 / abs(odds)


def decimal_to_american(dec: float) -> float:
    if dec <= 1.0:
        raise ValueError("Decimal odds must be > 1.0")
    if dec >= 2.0:
        return (dec - 1.0) * 100.0
    return -100.0 / (dec - 1.0)

def american_implied_prob(odds: float) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)

def fair_prob_from_fair_american(fair_odds: float) -> float:
    return american_implied_prob(fair_odds)

def profit_on_win(stake: float, odds: float) -> float:
    if odds > 0:
        return stake * (odds / 100.0)
    return stake * (100.0 / abs(odds))


def unboosted_american_from_boosted(boosted_odds: float, boost_pct: float) -> float:
    mult = 1.0 + float(boost_pct) / 100.0
    if mult <= 0:
        raise ValueError("Boost % must be greater than -100.")
    boosted_dec = american_to_decimal(float(boosted_odds))
    unboosted_dec = 1.0 + ((boosted_dec - 1.0) / mult)
    return decimal_to_american(unboosted_dec)

def kelly_fraction_from_prob(p: float, odds: float) -> float:
    dec = american_to_decimal(float(odds))
    b = dec - 1.0
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)

def round_to(x: float, step: float) -> float:
    if step <= 0:
        return x
    return round(x / step) * step

def now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


# -----------------------------
# Odds band helper
# -----------------------------
def odds_band(o: Any) -> str:
    try:
        if o is None or (isinstance(o, float) and math.isnan(o)):
            return "N/A"
        o = float(o)
        if o < 0:
            return "Negative"
        if o < 200:
            return "+100 to +199"
        if o < 400:
            return "+200 to +399"
        if o < 700:
            return "+400 to +699"
        if o < 1200:
            return "+700 to +1199"
        return "+1200+"
    except Exception:
        return "N/A"


def normalize_token(value: Optional[str]) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


MARKET_ALIASES = {
    "reb": "Player Rbs",
    "rbs": "Player Rbs",
    "rebound": "Player Rbs",
    "rebounds": "Player Rbs",
    "rebs": "Player Rbs",
    "pts": "Player Points",
    "point": "Player Points",
    "points": "Player Points",
    "ast": "Player Assists",
    "assists": "Player Assists",
    "ml": "Moneyline",
    "money line": "Moneyline",
    "moneyline": "Moneyline",
    "1h ml": "1H ML",
    "1h moneyline": "1H ML",
    "first half ml": "1H ML",
    "1h spread": "1H Spread",
    "first half spread": "1H Spread",
    "1h total over": "1H Total Over",
    "first half total over": "1H Total Over",
    "1h over": "1H Total Over",
    "1h total under": "1H Total Under",
    "first half total under": "1H Total Under",
    "1h under": "1H Total Under",
    "tt o": "Team Total Over",
    "team total o": "Team Total Over",
    "team total over": "Team Total Over",
    "tt over": "Team Total Over",
    "tt u": "Team Total Under",
    "team total u": "Team Total Under",
    "team total under": "Team Total Under",
    "tt under": "Team Total Under",
    "full game spread": "Spread",
    "full game total over": "Total Over",
    "full game total under": "Total Under",
    "total over": "Total Over",
    "total under": "Total Under",
    "player pts": "Player Points",
    "player points": "Player Points",
    "player rebs": "Player Rbs",
    "player rebounds": "Player Rbs",
    "player ast": "Player Assists",
    "player assists": "Player Assists",
    "stl": "Player Stl",
    "stls": "Player Stl",
    "steal": "Player Stl",
    "steals": "Player Stl",
    "player stl": "Player Stl",
    "player stls": "Player Stl",
    "player steals": "Player Stl",
    "p+a": "Player P+A",
    "pts+ast": "Player P+A",
    "points+assists": "Player P+A",
    "points and assists": "Player P+A",
    "pa": "Player P+A",
    "p+r": "Player P+R",
    "pts+rebs": "Player P+R",
    "points+rebounds": "Player P+R",
    "points and rebounds": "Player P+R",
    "pr": "Player P+R",
    "player pr": "Player P+R",
    "player p+r": "Player P+R",
    "r+a": "Player R+A",
    "rebs+ast": "Player R+A",
    "rebounds+assists": "Player R+A",
    "rebounds and assists": "Player R+A",
    "ra": "Player R+A",
    "pra": "Player PRA",
    "p+r+a": "Player PRA",
    "points+rebounds+assists": "Player PRA",
    "points rebounds assists": "Player PRA",
    "3pt made": "Player 3PM",
    "3pm": "Player 3PM",
    "3pt": "Player 3PM",
    "3ptm": "Player 3PM",
    "player 3pm": "Player 3PM",
    "player 3pt": "Player 3PM",
    "dd": "Player DD",
    "double double": "Player DD",
    "player dd": "Player DD",
    "player double double": "Player DD",
    "td": "Player TD",
    "triple double": "Player TD",
    "player td": "Player TD",
    "player triple double": "Player TD",
    "goal": "Player Goals",
    "goals": "Player Goals",
    "player goal": "Player Goals",
    "player goals": "Player Goals",
    "player point": "Player Points",
    "player points": "Player Points",
    "player sog": "Shots on Goal",
    "player shots on goal": "Shots on Goal",
    "first basket": "First Basket",
    "1st basket": "First Basket",
    "first bucket": "First Basket",
    "1st bucket": "First Basket",
    "atg": "ATG",
    "atgs": "ATG",
    "anytime goal scorer": "ATG",
    "sog": "Shots on Goal",
    "shots on goal": "Shots on Goal",
    "btts": "BTTS",
    "both teams to score": "BTTS",
    "both teams score": "BTTS",
    "yes btts": "BTTS",
    "corners": "Corners",
    "corner": "Corners",
    "total corners over": "Corners Over",
    "total corners under": "Corners Under",
    "team corners over": "Team Corners Over",
    "team corners under": "Team Corners Under",
}

BOOK_ALIASES = {
    "dk": "DraftKings",
    "draft kings": "DraftKings",
    "draftkings": "DraftKings",
    "fd": "FanDuel",
    "fan duel": "FanDuel",
    "fanduel": "FanDuel",
    "as placed": "FanDuel",
    "asplaced": "FanDuel",
    "fd as placed": "FanDuel",
    "fanduel as placed": "FanDuel",
    "fanduel (as placed)": "FanDuel",
    "fan duel (as placed)": "FanDuel",
    "mgm": "BetMGM",
    "bet mgm": "BetMGM",
    "betmgm": "BetMGM",
    "bet mom": "BetMGM",
    "betmgn": "BetMGM",
    "czr": "Caesars",
    "caesars": "Caesars",
    "fanatics": "Fanatics",
    "fanatic": "Fanatics",
    "br": "BetRivers",
    "bet rivers": "BetRivers",
    "betrivers": "BetRivers",
    "rivers": "BetRivers",
    "thescore": "theScore",
    "the score": "theScore",
    "scorebet": "theScore",
    "score bet": "theScore",
    "pn": "Pinnacle",
    "pinnacle": "Pinnacle",
}

SPORT_ALIASES = {
    "nba": "NBA",
    "nfl": "NFL",
    "nhl": "NHL",
    "national hockey league": "NHL",
    "international hockey": "Intl Hockey",
    "intl hockey": "Intl Hockey",
    "mlb": "MLB",
    "ufc": "UFC",
    "mma": "UFC",
    "cbb": "CBB",
    "ncaab": "CBB",
    "college basketball": "CBB",
    "ncaaf": "NCAAF",
    "wnba": "WNBA",
    "epl": "EPL",
    "soccer": "EPL",
}

MARKET_TYPE_ALIASES = {
    "game": "Game",
    "team": "Team",
    "player": "Player",
    "period": "Period",
    "other": "Other",
}

DEVIG_METHOD_OPTIONS = ["Market Avg", "Single Book (100%)", "Split Weights"]
DEVIG_METHOD_ALIASES = {
    "market avg": "Market Avg",
    "single book": "Single Book (100%)",
    "single book 100%": "Single Book (100%)",
    "single book (100%)": "Single Book (100%)",
    "split weights": "Split Weights",
    "spilt weights": "Split Weights",
}

DEFAULT_MARKET_OPTIONS = [
    "Moneyline",
    "Spread",
    "Total Over",
    "Total Under",
    "1H ML",
    "1H Spread",
    "1H Total Over",
    "1H Total Under",
    "Team Total Over",
    "Team Total Under",
    "Player Points",
    "Player Rbs",
    "Player Assists",
    "Player Stl",
    "Player P+A",
    "Player P+R",
    "Player R+A",
    "Player PRA",
    "Player 3PM",
    "Player Goals",
    "Player DD",
    "Player TD",
    "First Basket",
    "BTTS",
    "Corners",
    "Corners Over",
    "Corners Under",
    "Team Corners Over",
    "Team Corners Under",
    "ATG",
    "Shots on Goal",
]

MARKET_PRESETS_BY_SPORT = {
    "NBA": [
        "Moneyline", "Spread", "Total Over", "Total Under",
        "1H ML", "1H Spread", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
        "Player Points", "Player Rbs", "Player Assists", "Player Stl", "Player P+A", "Player P+R", "Player R+A", "Player PRA", "Player 3PM", "Player DD", "Player TD", "First Basket",
    ],
    "Intl Hockey": [
        "Moneyline", "Puckline", "Total Over", "Total Under",
        "1H ML", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
        "Player Points", "Player Goals", "Player Assists", "ATG", "Shots on Goal",
    ],
    "NHL": [
        "Moneyline", "Puckline", "Total Over", "Total Under",
        "1H ML", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
        "Player Points", "Player Goals", "Player Assists", "ATG", "Shots on Goal",
    ],
    "CBB": [
        "Moneyline", "Spread", "Total Over", "Total Under",
        "1H ML", "1H Spread", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
        "Player Points", "Player Rbs", "Player Assists", "Player Stl", "Player P+A", "Player P+R", "Player R+A", "Player PRA",
    ],
    "NFL": [
        "Moneyline", "Spread", "Total Over", "Total Under",
        "1H ML", "1H Spread", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
    ],
    "MLB": [
        "Moneyline", "Runline", "Total Over", "Total Under",
        "1H ML", "1H Total Over", "1H Total Under",
        "Team Total Over", "Team Total Under",
    ],
    "UFC": [
        "Moneyline", "Method of Victory", "Total Rounds Over", "Total Rounds Under",
    ],
    "EPL": [
        "Moneyline", "Draw No Bet", "Total Over", "Total Under",
        "Team Total Over", "Team Total Under",
        "BTTS", "Corners", "Corners Over", "Corners Under", "Team Corners Over", "Team Corners Under",
    ],
}

TEAM_OPTIONS_BY_SPORT: Dict[str, List[str]] = {
    "NBA": [
        "ATL - Atlanta Hawks", "BOS - Boston Celtics", "BKN - Brooklyn Nets", "CHA - Charlotte Hornets",
        "CHI - Chicago Bulls", "CLE - Cleveland Cavaliers", "DAL - Dallas Mavericks", "DEN - Denver Nuggets",
        "DET - Detroit Pistons", "GSW - Golden State Warriors", "HOU - Houston Rockets", "IND - Indiana Pacers",
        "LAC - LA Clippers", "LAL - Los Angeles Lakers", "MEM - Memphis Grizzlies", "MIA - Miami Heat",
        "MIL - Milwaukee Bucks", "MIN - Minnesota Timberwolves", "NOP - New Orleans Pelicans", "NYK - New York Knicks",
        "OKC - Oklahoma City Thunder", "ORL - Orlando Magic", "PHI - Philadelphia 76ers", "PHX - Phoenix Suns",
        "POR - Portland Trail Blazers", "SAC - Sacramento Kings", "SAS - San Antonio Spurs", "TOR - Toronto Raptors",
        "UTA - Utah Jazz", "WAS - Washington Wizards",
    ],
    "NHL": [
        "ANA - Anaheim Ducks", "BOS - Boston Bruins", "BUF - Buffalo Sabres", "CGY - Calgary Flames",
        "CAR - Carolina Hurricanes", "CHI - Chicago Blackhawks", "COL - Colorado Avalanche", "CBJ - Columbus Blue Jackets",
        "DAL - Dallas Stars", "DET - Detroit Red Wings", "EDM - Edmonton Oilers", "FLA - Florida Panthers",
        "LAK - Los Angeles Kings", "MIN - Minnesota Wild", "MTL - Montreal Canadiens", "NSH - Nashville Predators",
        "NJD - New Jersey Devils", "NYI - New York Islanders", "NYR - New York Rangers", "OTT - Ottawa Senators",
        "PHI - Philadelphia Flyers", "PIT - Pittsburgh Penguins", "SJS - San Jose Sharks", "SEA - Seattle Kraken",
        "STL - St. Louis Blues", "TBL - Tampa Bay Lightning", "TOR - Toronto Maple Leafs", "UTA - Utah Hockey Club",
        "VAN - Vancouver Canucks", "VGK - Vegas Golden Knights", "WSH - Washington Capitals", "WPG - Winnipeg Jets",
    ],
    "CBB": [
        "DUKE - Duke", "UNC - North Carolina", "UK - Kentucky", "KU - Kansas", "UCONN - UConn", "NOVA - Villanova",
        "GONZ - Gonzaga", "UCLA - UCLA", "ARIZ - Arizona", "BAY - Baylor", "HOU - Houston", "PUR - Purdue",
        "TENN - Tennessee", "AUB - Auburn", "BAMA - Alabama", "MSU - Michigan State", "MICH - Michigan",
        "IU - Indiana", "ILL - Illinois", "WIS - Wisconsin", "OSU - Ohio State", "MD - Maryland",
        "TTU - Texas Tech", "UT - Texas", "TA&M - Texas A&M", "OU - Oklahoma", "UVA - Virginia",
        "VT - Virginia Tech", "MIA - Miami", "FSU - Florida State", "CLEM - Clemson", "SDSU - San Diego State",
        "CREI - Creighton", "XAV - Xavier", "MARQ - Marquette", "PROV - Providence", "SETON - Seton Hall",
        "ARK - Arkansas", "LSU - LSU", "FLA - Florida",
    ],
    "NFL": [
        "ARI - Arizona Cardinals", "ATL - Atlanta Falcons", "BAL - Baltimore Ravens", "BUF - Buffalo Bills",
        "CAR - Carolina Panthers", "CHI - Chicago Bears", "CIN - Cincinnati Bengals", "CLE - Cleveland Browns",
        "DAL - Dallas Cowboys", "DEN - Denver Broncos", "DET - Detroit Lions", "GB - Green Bay Packers",
        "HOU - Houston Texans", "IND - Indianapolis Colts", "JAX - Jacksonville Jaguars", "KC - Kansas City Chiefs",
        "LV - Las Vegas Raiders", "LAC - Los Angeles Chargers", "LAR - Los Angeles Rams", "MIA - Miami Dolphins",
        "MIN - Minnesota Vikings", "NE - New England Patriots", "NO - New Orleans Saints", "NYG - New York Giants",
        "NYJ - New York Jets", "PHI - Philadelphia Eagles", "PIT - Pittsburgh Steelers", "SF - San Francisco 49ers",
        "SEA - Seattle Seahawks", "TB - Tampa Bay Buccaneers", "TEN - Tennessee Titans", "WSH - Washington Commanders",
    ],
    "MLB": [
        "ARI - Arizona Diamondbacks", "ATL - Atlanta Braves", "BAL - Baltimore Orioles", "BOS - Boston Red Sox",
        "CHC - Chicago Cubs", "CHW - Chicago White Sox", "CIN - Cincinnati Reds", "CLE - Cleveland Guardians",
        "COL - Colorado Rockies", "DET - Detroit Tigers", "HOU - Houston Astros", "KC - Kansas City Royals",
        "LAA - Los Angeles Angels", "LAD - Los Angeles Dodgers", "MIA - Miami Marlins", "MIL - Milwaukee Brewers",
        "MIN - Minnesota Twins", "NYM - New York Mets", "NYY - New York Yankees", "OAK - Athletics",
        "PHI - Philadelphia Phillies", "PIT - Pittsburgh Pirates", "SD - San Diego Padres", "SEA - Seattle Mariners",
        "SF - San Francisco Giants", "STL - St. Louis Cardinals", "TB - Tampa Bay Rays", "TEX - Texas Rangers",
        "TOR - Toronto Blue Jays", "WSH - Washington Nationals",
    ],
}

# Backward compatibility for legacy sport labels.
TEAM_OPTIONS_BY_SPORT["Intl Hockey"] = TEAM_OPTIONS_BY_SPORT.get("NHL", []) + [
    "USA - USA",
    "CAN - Canada",
    "FIN - Finland",
    "SVK - Slovakia",
]

TEAM_NAME_ALIASES = {
    "cleveland cavalier": "cleveland cavaliers",
    "cleveland cavs": "cleveland cavaliers",
    "cavs": "cavaliers",
    "charlotte hornet": "charlotte hornets",
    "hornets": "hornets",
    "new york knick": "new york knicks",
    "brooklyn net": "brooklyn nets",
    "indiana pacer": "indiana pacers",
    "knicks": "knicks",
    "nets": "nets",
    "wolves": "timberwolves",
    "twolves": "timberwolves",
    "mavs": "mavericks",
    "dubs": "warriors",
    "sixers": "76ers",
    "nugs": "nuggets",
    "pels": "pelicans",
    "spurs": "spurs",
    "blazers": "trail blazers",
    "clips": "clippers",
    "lakers": "lakers",
    "cananda": "canada",
}


def infer_market_type(market: str) -> str:
    m = normalize_token(market)
    if "player" in m or m in {
        "rbs", "rebs", "points", "assists", "player 3pm", "player p+a", "player p+r", "player r+a", "player pra",
        "atg", "atgs", "shots on goal"
    }:
        return "Player"
    if "team total" in m:
        return "Team"
    if m.startswith("1h") or "first half" in m:
        return "Period"
    return "Game"


def canonicalize_team(sport: str, value: Optional[str]) -> Optional[str]:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return None
    labels = TEAM_OPTIONS_BY_SPORT.get(sport, [])
    if not labels:
        return raw

    key = normalize_token(raw)
    key = TEAM_NAME_ALIASES.get(key, key)
    key_tokens = key.split()

    code_to_label: Dict[str, str] = {}
    exact_map: Dict[str, str] = {}
    nickname_to_labels: Dict[str, List[str]] = {}
    for label in labels:
        code, team_name = label.split(" - ", 1)
        code_key = normalize_token(code)
        team_key = normalize_token(team_name)
        team_key = TEAM_NAME_ALIASES.get(team_key, team_key)
        label_key = normalize_token(label)

        code_to_label[code_key] = label
        exact_map[code_key] = label
        exact_map[team_key] = label
        exact_map[label_key] = label
        nickname = team_key.split()[-1] if team_key.split() else team_key
        nickname_to_labels.setdefault(nickname, []).append(label)

    if key in exact_map:
        return exact_map[key]

    if key_tokens:
        first = key_tokens[0]
        if first in code_to_label:
            return code_to_label[first]
        if len(key_tokens) >= 2:
            # Handles entries like "DEN Nuggets" by trusting known code prefix.
            if first in code_to_label:
                return code_to_label[first]

    if key in nickname_to_labels and len(nickname_to_labels[key]) == 1:
        return nickname_to_labels[key][0]

    # Handle shorthand nicknames (e.g., "Cavs", "CLE Cavs", etc.)
    key_simple = key.replace(".", "").strip()
    key_simple = TEAM_NAME_ALIASES.get(key_simple, key_simple)
    key_simple = key_simple.rstrip("s")
    fuzzy_candidates: List[str] = []
    for label in labels:
        _, team_name = label.split(" - ", 1)
        nickname = normalize_token(team_name).split()[-1].replace(".", "")
        nick_simple = TEAM_NAME_ALIASES.get(nickname, nickname).rstrip("s")
        if key_simple == nick_simple:
            return label
        if len(key_simple) >= 3 and nick_simple.startswith(key_simple):
            fuzzy_candidates.append(label)
        elif len(nick_simple) >= 3 and key_simple.startswith(nick_simple):
            fuzzy_candidates.append(label)
    if len(set(fuzzy_candidates)) == 1:
        return fuzzy_candidates[0]

    loose_candidates: List[str] = []
    token_set = set(key_tokens)
    for label in labels:
        _, team_name = label.split(" - ", 1)
        team_key = TEAM_NAME_ALIASES.get(normalize_token(team_name), normalize_token(team_name))
        if token_set and token_set.issubset(set(team_key.split())):
            loose_candidates.append(label)
    if len(loose_candidates) == 1:
        return loose_candidates[0]

    return raw


def canonicalize_devig_method(value: Optional[str]) -> str:
    raw = "" if value is None else str(value).strip()
    key = normalize_token(raw)
    if not key:
        return "Market Avg"
    return DEVIG_METHOD_ALIASES.get(key, raw)


def validate_devig_details(method: str, details: Optional[str]) -> None:
    m = canonicalize_devig_method(method)
    d = "" if details is None else str(details).strip()
    if m == "Market Avg":
        return
    if not d:
        raise ValueError(f"'{m}' requires Devig Details (e.g., book names and weights).")


def matchup_key(team: Optional[str], opponent: Optional[str]) -> Optional[str]:
    t = "" if team is None else str(team).strip()
    o = "" if opponent is None else str(opponent).strip()
    if not t or not o:
        return None
    a, b = sorted([t, o], key=lambda x: x.lower())
    return f"{a} vs {b}"


def canonicalize_value(field: str, value: Optional[str]) -> str:
    raw = "" if value is None else str(value).strip()
    key = normalize_token(raw)
    if not key:
        return ""
    if field == "market":
        return MARKET_ALIASES.get(key, raw.title())
    if field == "book":
        key_simple = re.sub(r"[^a-z0-9]+", " ", key).strip()
        compact = key_simple.replace(" ", "")
        return BOOK_ALIASES.get(key, BOOK_ALIASES.get(key_simple, BOOK_ALIASES.get(compact, raw.title())))
    if field == "sport":
        return SPORT_ALIASES.get(key, raw.upper() if len(raw) <= 5 else raw.title())
    if field == "market_type":
        return MARKET_TYPE_ALIASES.get(key, raw.title())
    if field == "devig_method":
        return canonicalize_devig_method(raw)
    return raw


def history_options(ledger: "Ledger", field: str, defaults: List[str]) -> List[str]:
    seen = set()
    options: List[str] = []
    for val in defaults:
        c = canonicalize_value(field, val)
        if c and c not in seen:
            seen.add(c)
            options.append(c)
    for b in ledger.bets:
        c = canonicalize_value(field, getattr(b, field, ""))
        if c and c not in seen:
            seen.add(c)
            options.append(c)
    return options


def value_score(
    rec: Dict[str, Any],
    open_exposure_after: float,
    bankroll: float,
    team_concentration_after: float,
    matchup_concentration_after: float,
    open_bets_after: int = 0,
    concentration_min_open_bets: int = 20,
) -> Dict[str, Any]:
    base = 55.0
    score = base
    factors: List[Dict[str, Any]] = []
    ev_per_dollar = rec.get("ev_per_dollar")
    if ev_per_dollar is not None:
        delta = max(-20.0, min(30.0, float(ev_per_dollar) * 300.0))
        score += delta
        factors.append({"label": "EV edge", "delta": delta})
    else:
        score -= 8.0
        factors.append({"label": "Missing EV input", "delta": -8.0})

    if rec.get("was_capped"):
        score -= 10.0
        factors.append({"label": "Stake capped", "delta": -10.0})

    open_pct = (open_exposure_after / bankroll) if bankroll > 0 else 1.0
    if open_pct > 0.20:
        score -= 18.0
        factors.append({"label": "Open exposure > 20%", "delta": -18.0})
    elif open_pct > 0.12:
        score -= 8.0
        factors.append({"label": "Open exposure > 12%", "delta": -8.0})

    concentration_applied = int(open_bets_after) >= int(concentration_min_open_bets)
    if concentration_applied:
        if team_concentration_after > 0.45:
            score -= 12.0
            factors.append({"label": "Team concentration > 45%", "delta": -12.0})
        elif team_concentration_after > 0.30:
            score -= 6.0
            factors.append({"label": "Team concentration > 30%", "delta": -6.0})

        if matchup_concentration_after > 0.35:
            score -= 10.0
            factors.append({"label": "Matchup concentration > 35%", "delta": -10.0})
        elif matchup_concentration_after > 0.25:
            score -= 5.0
            factors.append({"label": "Matchup concentration > 25%", "delta": -5.0})

    score = max(0.0, min(100.0, score))
    if score >= 80:
        grade = "A"
        verdict = "Strong"
    elif score >= 65:
        grade = "B"
        verdict = "Good"
    elif score >= 50:
        grade = "C"
        verdict = "Neutral"
    elif score >= 35:
        grade = "D"
        verdict = "Caution"
    else:
        grade = "F"
        verdict = "Pass"
    return {
        "score": score,
        "grade": grade,
        "verdict": verdict,
        "base": base,
        "factors": factors,
        "open_exposure_pct": open_pct,
        "team_concentration_after": team_concentration_after,
        "matchup_concentration_after": matchup_concentration_after,
        "open_bets_after": int(open_bets_after),
        "concentration_applied": concentration_applied,
        "concentration_min_open_bets": int(concentration_min_open_bets),
    }


def recommend_live_stake(
    ledger: "Ledger",
    odds_american: float,
    fair_odds_american: Optional[float] = None,
    true_prob: Optional[float] = None,
    kelly_units_from_tool: Optional[float] = None,
    kelly_fraction: float = 0.25,
    max_fraction_of_bankroll: float = 0.03,
    live_max_fraction_of_bankroll: float = 0.015,
    min_stake: float = 1.0,
    round_step: float = 0.25,
) -> Dict[str, Any]:
    base = ledger.recommend_stake(
        odds_american=odds_american,
        fair_odds_american=fair_odds_american,
        true_prob=true_prob,
        kelly_units_from_tool=kelly_units_from_tool,
        kelly_fraction=kelly_fraction,
        max_fraction_of_bankroll=max_fraction_of_bankroll,
        min_stake=min_stake,
        round_step=round_step,
    )

    ev = base.get("ev_per_dollar")
    damp_mult = 1.0
    damp_reason = "No live EV dampening applied."
    ev_band = "N/A"
    if ev is not None:
        ev = float(ev)
        # Gradient tuned for live EV ranges that can run ~30% to ~195%.
        if ev < 0.30:
            damp_mult = 0.95
            ev_band = "<30%"
        elif ev < 0.50:
            # 30-50%: light damp 0.90 -> 0.82
            damp_mult = 0.90 - ((ev - 0.30) / 0.20) * 0.08
            ev_band = "30-50%"
        elif ev < 0.80:
            # 50-80%: moderate damp 0.82 -> 0.70
            damp_mult = 0.82 - ((ev - 0.50) / 0.30) * 0.12
            ev_band = "50-80%"
        elif ev < 1.20:
            # 80-120%: stronger damp 0.70 -> 0.56
            damp_mult = 0.70 - ((ev - 0.80) / 0.40) * 0.14
            ev_band = "80-120%"
        elif ev < 1.60:
            # 120-160%: aggressive damp 0.56 -> 0.44
            damp_mult = 0.56 - ((ev - 1.20) / 0.40) * 0.12
            ev_band = "120-160%"
        elif ev <= 1.95:
            # 160-195%: very aggressive damp 0.44 -> 0.35
            damp_mult = 0.44 - ((ev - 1.60) / 0.35) * 0.09
            ev_band = "160-195%"
        else:
            # Above expected range: keep tightly bounded.
            damp_mult = 0.35
            ev_band = ">195%"
        damp_mult = max(0.35, min(0.95, damp_mult))
        damp_reason = f"Live EV band {ev_band}: dampener {damp_mult:.2f}x (higher EV => stronger dampening)."

    bankroll = float(base["bankroll"])
    live_cap_amount = bankroll * float(live_max_fraction_of_bankroll)
    effective_cap = min(float(base["cap_amount"]), live_cap_amount)

    raw_after_damp = float(base["raw_stake_before_cap"]) * damp_mult
    after_cap = min(raw_after_damp, effective_cap)
    recommended = round_to(max(float(min_stake), after_cap), float(round_step))

    out = dict(base)
    out["recommended_stake"] = recommended
    out["raw_stake_live_damp"] = raw_after_damp
    out["live_ev_damp_mult"] = damp_mult
    out["live_ev_damp_reason"] = damp_reason
    out["live_ev_band"] = ev_band
    out["live_cap_amount"] = live_cap_amount
    out["effective_live_cap"] = effective_cap
    out["was_live_capped"] = raw_after_damp > effective_cap
    out["live_max_fraction_of_bankroll"] = float(live_max_fraction_of_bankroll)
    return out


def historical_stake_multiplier(
    settled_df: pd.DataFrame,
    sport: str,
    book: str,
    devig_method: str,
    market_type: str,
    total_threshold: int = 1000,
    min_slice_n: int = 30,
) -> Dict[str, Any]:
    if settled_df is None or settled_df.empty:
        return {"ready": False, "reason": "No settled history yet."}
    if len(settled_df) < total_threshold:
        return {"ready": False, "reason": f"Needs {total_threshold}+ settled bets (currently {len(settled_df)})."}

    scoped = settled_df[
        (settled_df["sport"] == sport) &
        (settled_df["book"] == book) &
        (settled_df["devig_method"] == devig_method) &
        (settled_df["market_type"] == market_type)
    ].copy()
    scope_label = "sport+book+devig+market_type"
    if len(scoped) < min_slice_n:
        scoped = settled_df[
            (settled_df["sport"] == sport) &
            (settled_df["market_type"] == market_type)
        ].copy()
        scope_label = "sport+market_type fallback"

    if len(scoped) < min_slice_n:
        scoped = settled_df.copy()
        scope_label = "overall fallback"

    n = len(scoped)
    staked = float(scoped["stake"].sum()) if n else 0.0
    pnl = float(scoped["pnl"].sum()) if n else 0.0
    roi = (pnl / staked) if staked > 0 else 0.0

    # Smoothly scale around 1.0 using ROI; bounded to avoid overreaction.
    multiplier = 1.0 + (roi * 0.75)
    multiplier = max(0.75, min(1.30, multiplier))

    confidence = min(1.0, n / 200.0)
    return {
        "ready": True,
        "scope": scope_label,
        "n": n,
        "roi": roi,
        "multiplier": multiplier,
        "confidence": confidence,
    }


def apply_time_window(df_in: pd.DataFrame, window_label: str, dt_col: str) -> pd.DataFrame:
    if df_in.empty or dt_col not in df_in.columns:
        return df_in
    out = df_in.copy()
    now = pd.Timestamp.now()
    if window_label == "Last 7 Days":
        cutoff = now - pd.Timedelta(days=7)
        return out[out[dt_col] >= cutoff]
    if window_label == "Last 30 Days":
        cutoff = now - pd.Timedelta(days=30)
        return out[out[dt_col] >= cutoff]
    if window_label == "Last 90 Days":
        cutoff = now - pd.Timedelta(days=90)
        return out[out[dt_col] >= cutoff]
    if window_label == "Season (YTD)":
        ytd = pd.Timestamp(year=now.year, month=1, day=1)
        return out[out[dt_col] >= ytd]
    return out


def roi_confidence_band(df_in: pd.DataFrame) -> Dict[str, Any]:
    if df_in.empty:
        return {"n": 0, "roi": 0.0, "ci_low": None, "ci_high": None, "staked": 0.0, "stability": "Low"}
    x = df_in.copy()
    x = x[x["stake"] > 0].copy()
    if x.empty:
        return {"n": 0, "roi": 0.0, "ci_low": None, "ci_high": None, "staked": 0.0, "stability": "Low"}
    x["ret_per_dollar"] = x["pnl"] / x["stake"]
    n = int(len(x))
    roi = float(x["ret_per_dollar"].mean())
    staked = float(x["stake"].sum())
    ci_low = None
    ci_high = None
    if n >= 2:
        se = float(x["ret_per_dollar"].std(ddof=1) / math.sqrt(n))
        ci_low = roi - 1.96 * se
        ci_high = roi + 1.96 * se
    if n >= 80 and staked >= 2000:
        stability = "High"
    elif n >= 30 and staked >= 750:
        stability = "Medium"
    else:
        stability = "Low"
    return {"n": n, "roi": roi, "ci_low": ci_low, "ci_high": ci_high, "staked": staked, "stability": stability}


def clv_metrics(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in.empty:
        return pd.DataFrame(columns=["bet_id", "clv_pct", "clv_edge_bps"])
    x = df_in.copy()
    x["clv_entry_odds"] = x["unboosted_odds_american"].where(x["unboosted_odds_american"].notna(), x["odds_american"])
    x["clv_entry_source"] = x["unboosted_odds_american"].apply(lambda v: "Non-Boosted Entry" if pd.notnull(v) else "As Placed")
    valid = x["clv_entry_odds"].notna() & x["closing_odds_american"].notna()
    x = x[valid].copy()
    if x.empty:
        return pd.DataFrame(columns=["bet_id", "clv_pct", "clv_edge_bps"])
    x = x[(x["clv_entry_odds"] != 0) & (x["closing_odds_american"] != 0)].copy()
    if x.empty:
        return pd.DataFrame(columns=["bet_id", "clv_pct", "clv_edge_bps"])
    x["dec_open"] = x["clv_entry_odds"].apply(lambda o: american_to_decimal(float(o)))
    x["dec_close"] = x["closing_odds_american"].apply(lambda o: american_to_decimal(float(o)))
    x["clv_pct"] = (x["dec_open"] / x["dec_close"]) - 1.0
    x["clv_edge_bps"] = x["clv_pct"] * 10000.0
    return x


def expected_edge_per_dollar(row: pd.Series) -> Optional[float]:
    try:
        ev_pct = row.get("ev_pct")
        if pd.notna(ev_pct):
            v = float(ev_pct)
            # Accept either 5 (percent) or 0.05 (decimal)
            return v / 100.0 if abs(v) > 1.0 else v
        p = row.get("true_prob")
        o = row.get("odds_american")
        if pd.notna(p) and pd.notna(o):
            dec = american_to_decimal(float(o))
            b = dec - 1.0
            return float(p) * b - (1.0 - float(p))
    except Exception:
        return None
    return None


def simulate_multiplier_impact(settled_df: pd.DataFrame, total_threshold: int = 1000, min_slice_n: int = 30) -> Dict[str, Any]:
    if settled_df.empty:
        return {"ready": False, "reason": "No settled bets."}

    ordered = settled_df.sort_values(["settled_at_dt", "placed_at_dt"], na_position="last").reset_index(drop=True).copy()
    sim_total_pnl = 0.0
    actual_total_pnl = float(ordered["pnl"].sum())
    bets_with_multiplier = 0

    for i in range(len(ordered)):
        row = ordered.iloc[i]
        stake = float(row["stake"]) if pd.notnull(row["stake"]) else 0.0
        pnl = float(row["pnl"]) if pd.notnull(row["pnl"]) else 0.0
        if stake <= 0:
            sim_total_pnl += pnl
            continue

        hist = ordered.iloc[:i].copy()
        info = historical_stake_multiplier(
            settled_df=hist,
            sport=str(row.get("sport", "")),
            book=str(row.get("book", "")),
            devig_method=str(row.get("devig_method", "")),
            market_type=str(row.get("market_type", "")),
            total_threshold=total_threshold,
            min_slice_n=min_slice_n,
        )
        mult = 1.0
        if info.get("ready"):
            mult = float(info.get("multiplier", 1.0))
            bets_with_multiplier += 1
        sim_total_pnl += pnl * mult

    uplift = sim_total_pnl - actual_total_pnl
    return {
        "ready": True,
        "actual_pnl": actual_total_pnl,
        "sim_pnl": sim_total_pnl,
        "uplift": uplift,
        "bets_with_multiplier": bets_with_multiplier,
        "total_bets": int(len(ordered)),
    }


def sample_flag(n: int, min_n: int = 30) -> str:
    return "Low Sample" if int(n) < int(min_n) else "OK"


def build_month_calendar_df(daily_df: pd.DataFrame, month_key: str) -> pd.DataFrame:
    year, month = [int(x) for x in month_key.split("-")]
    cal = calendar.Calendar(firstweekday=0)  # Monday
    value_map: Dict[pd.Timestamp, Dict[str, Any]] = {}
    if daily_df is not None and not daily_df.empty:
        for _, r in daily_df.iterrows():
            d = pd.Timestamp(r["day"]).normalize()
            value_map[d] = {
                "bets": int(r.get("bets", 0)),
                "staked": float(r.get("staked", 0.0)),
                "pnl": float(r.get("pnl", 0.0)),
                "net_units": float(r.get("net_units", 0.0)),
                "roi": float(r.get("roi", 0.0)) if pd.notnull(r.get("roi")) else 0.0,
            }

    rows: List[Dict[str, Any]] = []
    weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    weeks = cal.monthdatescalendar(year, month)
    for w_idx, week in enumerate(weeks):
        for d in week:
            d_ts = pd.Timestamp(d).normalize()
            in_month = (d.month == month)
            base = value_map.get(d_ts, {"bets": 0, "staked": 0.0, "pnl": 0.0, "net_units": 0.0, "roi": 0.0})
            has_value = in_month and base["bets"] > 0
            if not in_month:
                bucket = "outside"
            elif not has_value:
                bucket = "neutral"
            elif base["net_units"] > 0:
                bucket = "pos"
            elif base["net_units"] < 0:
                bucket = "neg"
            else:
                bucket = "flat"

            rows.append({
                "date": d_ts,
                "week_idx": w_idx,
                "weekday_idx": week.index(d),
                "weekday": weekday_labels[week.index(d)],
                "day_num": str(d.day),
                "in_month": in_month,
                "bets": base["bets"],
                "staked": base["staked"],
                "pnl": base["pnl"],
                "net_units": base["net_units"],
                "roi": base["roi"],
                "bucket": bucket,
                "units_label": f"{base['net_units']:+.1f}u" if has_value else "",
                "day_label_color": "#6b7280" if in_month else "#cbd5e1",
                "units_color": "#15803d" if base["net_units"] > 0 else ("#b91c1c" if base["net_units"] < 0 else "#475569"),
            })
    return pd.DataFrame(rows)


# -----------------------------
# Data model
# -----------------------------
@dataclass
class Bet:
    bet_id: str
    placed_at: str
    sport: str
    market: str
    selection: str
    book: str
    odds_american: Optional[float]
    stake: float
    unit_size: float
    market_type: str = "Game"
    team: Optional[str] = None
    opponent: Optional[str] = None
    devig_method: str = "Market Avg"
    devig_details: Optional[str] = None
    recommended_stake_snapshot: Optional[float] = None
    stake_source: Optional[str] = None
    is_live: bool = False
    is_parlay: bool = False
    parlay_leg_count: Optional[int] = None
    parlay_boost_pct: Optional[float] = None
    parlay_unboosted_odds_american: Optional[float] = None
    parlay_boosted_odds_american: Optional[float] = None
    parlay_true_prob: Optional[float] = None
    parlay_legs: Optional[List[Dict[str, Any]]] = None

    fair_odds_american: Optional[float] = None
    true_prob: Optional[float] = None
    ev_pct: Optional[float] = None
    kelly_fraction_used: Optional[float] = None
    kelly_units_from_tool: Optional[float] = None

    boost_pct: Optional[float] = None
    unboosted_odds_american: Optional[float] = None
    closing_odds_american: Optional[float] = None

    status: str = "OPEN"  # OPEN / WON / LOST / VOID
    settled_at: Optional[str] = None
    pnl: float = 0.0
    notes: Optional[str] = None


class Ledger:
    def __init__(self, starting_bankroll: float, unit_size: float, storage_path: str):
        # Bankroll baseline is fixed at $500; current bankroll is baseline + settled PnL.
        self.starting_bankroll = 500.0
        self.unit_size = float(unit_size)
        self.storage_path = storage_path
        self.bets: List[Bet] = []

    def save(self) -> None:
        payload = {
            "starting_bankroll": self.starting_bankroll,
            "unit_size": self.unit_size,
            "bets": [asdict(b) for b in self.bets],
        }
        save_ledger_payload(payload)

    @classmethod
    def load(cls, storage_path: str) -> "Ledger":
        payload = load_ledger_payload()
        led = cls(payload["starting_bankroll"], payload["unit_size"], storage_path=storage_path)
        bet_fields = set(Bet.__dataclass_fields__.keys())
        led.bets = []
        for b in payload.get("bets", []):
            if not isinstance(b, dict):
                continue
            safe_b = {k: v for k, v in b.items() if k in bet_fields}
            led.bets.append(Bet(**safe_b))
        return led

    def realized_bankroll(self) -> float:
        pnl = sum(b.pnl for b in self.bets if b.status in ("WON", "LOST", "VOID"))
        return self.starting_bankroll + pnl

    def open_exposure(self) -> float:
        return sum(b.stake for b in self.bets if b.status == "OPEN")

    def normalize_existing_bets(self) -> int:
        changes = 0
        for b in self.bets:
            sport_new = canonicalize_value("sport", b.sport)
            market_new = canonicalize_value("market", b.market)
            market_type_new = canonicalize_value("market_type", getattr(b, "market_type", "") or infer_market_type(market_new))
            book_new = canonicalize_value("book", b.book)
            team_new = canonicalize_team(sport_new, getattr(b, "team", None))
            opp_new = canonicalize_team(sport_new, getattr(b, "opponent", None))
            devig_new = canonicalize_devig_method(getattr(b, "devig_method", None))

            for attr, val in [
                ("sport", sport_new),
                ("market", market_new),
                ("market_type", market_type_new),
                ("book", book_new),
                ("team", team_new),
                ("opponent", opp_new),
                ("devig_method", devig_new),
            ]:
                if getattr(b, attr, None) != val:
                    setattr(b, attr, val)
                    changes += 1
        return changes

    def recommend_stake(
        self,
        odds_american: float,
        fair_odds_american: Optional[float] = None,
        true_prob: Optional[float] = None,
        kelly_units_from_tool: Optional[float] = None,
        kelly_fraction: float = 0.25,
        max_fraction_of_bankroll: float = 0.03,
        min_stake: float = 1.0,
        round_step: float = 0.25,
    ) -> Dict[str, Any]:
        br = self.realized_bankroll()
        cap_amount = br * float(max_fraction_of_bankroll)

        if kelly_units_from_tool is not None:
            raw_stake = float(kelly_units_from_tool) * self.unit_size
            method = "EVSharps ¼-Kelly units"
            full_kelly = None
            p_used = None
        else:
            if true_prob is None:
                if fair_odds_american is None:
                    raise ValueError("Provide true_prob OR fair_odds_american OR kelly_units_from_tool.")
                p_used = fair_prob_from_fair_american(float(fair_odds_american))
            else:
                p_used = float(true_prob)

            full_kelly = kelly_fraction_from_prob(p_used, float(odds_american))
            raw_stake = br * full_kelly * float(kelly_fraction)
            method = "Kelly (from prob/fair)"

        capped_stake = min(raw_stake, cap_amount)
        was_capped = raw_stake > cap_amount
        stake = capped_stake
        stake = max(float(min_stake), stake)
        stake = round_to(stake, float(round_step))

        ev_per_dollar = None
        if p_used is not None:
            dec = american_to_decimal(float(odds_american))
            b = dec - 1.0
            ev_per_dollar = p_used * b - (1 - p_used)

        return {
            "bankroll": br,
            "method": method,
            "recommended_stake": stake,
            "raw_stake_before_cap": raw_stake,
            "stake_after_cap_before_min_round": capped_stake,
            "cap_amount": cap_amount,
            "was_capped": was_capped,
            "odds_american": float(odds_american),
            "fair_odds_american": float(fair_odds_american) if fair_odds_american is not None else None,
            "true_prob": p_used,
            "full_kelly_fraction": full_kelly,
            "kelly_fraction_used": float(kelly_fraction) if kelly_units_from_tool is None else None,
            "ev_per_dollar": ev_per_dollar,
            "max_fraction_of_bankroll": float(max_fraction_of_bankroll),
            "min_stake": float(min_stake),
            "round_step": float(round_step),
        }

    def add_bet(
        self,
        sport: str,
        team: Optional[str],
        opponent: Optional[str],
        market: str,
        market_type: str,
        selection: str,
        book: str,
        devig_method: str,
        devig_details: Optional[str],
        recommended_stake_snapshot: Optional[float],
        stake_source: Optional[str],
        odds_american: Optional[float],
        stake: float,
        fair_odds_american: Optional[float] = None,
        true_prob: Optional[float] = None,
        ev_pct: Optional[float] = None,
        kelly_fraction_used: Optional[float] = None,
        kelly_units_from_tool: Optional[float] = None,
        is_live: bool = False,
        boost_pct: Optional[float] = None,
        unboosted_odds_american: Optional[float] = None,
        notes: Optional[str] = None,
        is_parlay: bool = False,
        parlay_leg_count: Optional[int] = None,
        parlay_boost_pct: Optional[float] = None,
        parlay_unboosted_odds_american: Optional[float] = None,
        parlay_boosted_odds_american: Optional[float] = None,
        parlay_true_prob: Optional[float] = None,
        parlay_legs: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        bet_id = str(uuid.uuid4())[:8]
        b = Bet(
            bet_id=bet_id,
            placed_at=now_ts(),
            sport=canonicalize_value("sport", sport),
            team=canonicalize_team(canonicalize_value("sport", sport), team),
            opponent=canonicalize_team(canonicalize_value("sport", sport), opponent),
            market=canonicalize_value("market", market),
            market_type=canonicalize_value("market_type", market_type),
            selection=selection,
            book=canonicalize_value("book", book),
            devig_method=canonicalize_devig_method(devig_method),
            devig_details=devig_details.strip() if isinstance(devig_details, str) and devig_details.strip() else None,
            recommended_stake_snapshot=float(recommended_stake_snapshot) if recommended_stake_snapshot is not None else None,
            stake_source=stake_source.strip() if isinstance(stake_source, str) and stake_source.strip() else None,
            is_live=bool(is_live),
            is_parlay=bool(is_parlay),
            parlay_leg_count=int(parlay_leg_count) if parlay_leg_count is not None else None,
            parlay_boost_pct=float(parlay_boost_pct) if parlay_boost_pct is not None else None,
            parlay_unboosted_odds_american=float(parlay_unboosted_odds_american) if parlay_unboosted_odds_american is not None else None,
            parlay_boosted_odds_american=float(parlay_boosted_odds_american) if parlay_boosted_odds_american is not None else None,
            parlay_true_prob=float(parlay_true_prob) if parlay_true_prob is not None else None,
            parlay_legs=parlay_legs if parlay_legs is not None else None,
            odds_american=odds_american,
            stake=float(stake),
            unit_size=float(self.unit_size),
            fair_odds_american=fair_odds_american,
            true_prob=true_prob,
            ev_pct=ev_pct,
            kelly_fraction_used=kelly_fraction_used,
            kelly_units_from_tool=kelly_units_from_tool,
            boost_pct=float(boost_pct) if boost_pct is not None else None,
            unboosted_odds_american=float(unboosted_odds_american) if unboosted_odds_american is not None else None,
            notes=notes,
        )
        validate_devig_details(b.devig_method, b.devig_details)
        self.bets.append(b)
        return bet_id

    def update_bet(self, bet_id: str, updates: Dict[str, Any]) -> None:
        b = next((x for x in self.bets if x.bet_id == bet_id), None)
        if b is None:
            raise ValueError(f"Bet id not found: {bet_id}")

        allowed = {
            "sport", "team", "opponent", "market", "market_type", "selection", "book", "devig_method", "devig_details",
            "recommended_stake_snapshot", "stake_source",
            "is_live",
            "odds_american", "stake",
            "fair_odds_american", "true_prob", "ev_pct", "kelly_fraction_used",
            "kelly_units_from_tool", "boost_pct", "unboosted_odds_american", "closing_odds_american", "notes"
        }

        prospective_method = canonicalize_devig_method(updates.get("devig_method", b.devig_method))
        prospective_details_raw = updates.get("devig_details", b.devig_details)
        prospective_details = prospective_details_raw.strip() if isinstance(prospective_details_raw, str) and prospective_details_raw.strip() else None
        validate_devig_details(prospective_method, prospective_details)

        for key, value in updates.items():
            if key not in allowed:
                continue
            if key in {"sport", "market", "book", "market_type"}:
                value = canonicalize_value(key, value)
            if key in {"team", "opponent"}:
                sport_for_team = canonicalize_value("sport", updates.get("sport", b.sport))
                value = canonicalize_team(sport_for_team, value)
            if key in {"devig_details"}:
                value = value.strip() if isinstance(value, str) and value.strip() else None
            if key == "devig_method":
                value = canonicalize_devig_method(value)
            if key == "stake" and float(value) < 0:
                raise ValueError("Stake cannot be negative.")
            if key == "odds_american" and value == 0:
                raise ValueError("Odds cannot be 0.")
            if key == "boost_pct" and value is not None and float(value) < 0:
                raise ValueError("Boost % cannot be negative.")
            setattr(b, key, value)

    def grade_bet(self, bet_id: str, result: str, closing_odds_american: Optional[float] = None) -> None:
        result = result.upper().strip()
        if result not in ("W", "L", "VOID"):
            raise ValueError("result must be 'W', 'L', or 'VOID'.")

        b = next((x for x in self.bets if x.bet_id == bet_id), None)
        if b is None:
            raise ValueError(f"Bet id not found: {bet_id}")
        if b.status != "OPEN":
            raise ValueError(f"Bet {bet_id} already settled as {b.status}")

        b.settled_at = now_ts()
        b.closing_odds_american = closing_odds_american

        if result == "W":
            if b.odds_american is None:
                raise ValueError("Cannot settle WIN without odds_american.")
            b.status = "WON"
            b.pnl = profit_on_win(b.stake, float(b.odds_american))
        elif result == "L":
            b.status = "LOST"
            b.pnl = -b.stake
        else:
            b.status = "VOID"
            b.pnl = 0.0

    def regrade_settled_bet(self, bet_id: str, result: str, closing_odds_american: Optional[float] = None) -> None:
        result = result.upper().strip()
        if result not in ("W", "L", "VOID"):
            raise ValueError("result must be 'W', 'L', or 'VOID'.")

        b = next((x for x in self.bets if x.bet_id == bet_id), None)
        if b is None:
            raise ValueError(f"Bet id not found: {bet_id}")
        if b.status not in ("WON", "LOST", "VOID"):
            raise ValueError(f"Bet {bet_id} is not settled. Use normal grading for OPEN bets.")

        b.settled_at = now_ts()
        if closing_odds_american is not None:
            b.closing_odds_american = closing_odds_american

        if result == "W":
            if b.odds_american is None:
                raise ValueError("Cannot settle WIN without odds_american.")
            b.status = "WON"
            b.pnl = profit_on_win(b.stake, float(b.odds_american))
        elif result == "L":
            b.status = "LOST"
            b.pnl = -b.stake
        else:
            b.status = "VOID"
            b.pnl = 0.0

    def set_bet_status(self, bet_id: str, target_status: str, closing_odds_american: Optional[float] = None) -> None:
        target = str(target_status).upper().strip()
        if target not in {"OPEN", "WON", "LOST", "VOID"}:
            raise ValueError("target_status must be one of OPEN, WON, LOST, VOID.")

        b = next((x for x in self.bets if x.bet_id == bet_id), None)
        if b is None:
            raise ValueError(f"Bet id not found: {bet_id}")

        current = str(b.status).upper().strip()
        if target == current:
            if target in {"WON", "LOST", "VOID"} and closing_odds_american is not None:
                b.closing_odds_american = closing_odds_american
            return

        if target == "OPEN":
            b.status = "OPEN"
            b.settled_at = None
            b.pnl = 0.0
            b.closing_odds_american = None
            return

        result_map = {"WON": "W", "LOST": "L", "VOID": "VOID"}
        if current == "OPEN":
            self.grade_bet(bet_id, result_map[target], closing_odds_american=closing_odds_american)
        else:
            self.regrade_settled_bet(bet_id, result_map[target], closing_odds_american=closing_odds_american)

    def to_df(self) -> pd.DataFrame:
        df = pd.DataFrame([asdict(b) for b in self.bets])
        expected_cols = [
            "bet_id","placed_at","sport","team","opponent","market","market_type","selection","book","devig_method","devig_details",
            "recommended_stake_snapshot","stake_source",
            "is_live",
            "is_parlay","parlay_leg_count","parlay_boost_pct","parlay_unboosted_odds_american",
            "parlay_boosted_odds_american","parlay_true_prob","parlay_legs",
            "odds_american","stake","unit_size","fair_odds_american","true_prob","ev_pct","kelly_fraction_used",
            "kelly_units_from_tool","boost_pct","unboosted_odds_american","closing_odds_american","status","settled_at","pnl","notes"
        ]

        # Always return expected columns so UI never breaks
        if df.empty:
            df = pd.DataFrame(columns=expected_cols)
        else:
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

        # Canonicalize key categorical fields for consistent filtering/grouping.
        df["sport"] = df.get("sport").apply(lambda x: canonicalize_value("sport", x) if pd.notnull(x) else x)
        df["market"] = df.get("market").apply(lambda x: canonicalize_value("market", x) if pd.notnull(x) else x)
        df["book"] = df.get("book").apply(lambda x: canonicalize_value("book", x) if pd.notnull(x) else x)
        df["devig_method"] = df.get("devig_method").apply(
            lambda x: canonicalize_value("devig_method", x) if pd.notnull(x) else "Market Avg"
        )
        df["is_parlay"] = df.get("is_parlay").fillna(False).astype(bool)
        if "is_live" not in df.columns:
            df["is_live"] = False
        df["is_live"] = df.get("is_live").fillna(False).astype(bool)
        # Backward compatibility for older logs before is_live existed.
        legacy_live = df.get("stake_source").fillna("").astype(str).str.contains("Live", case=False) | df.get("notes").fillna("").astype(str).str.contains(r"\[LIVE\]", case=False, regex=True)
        df["is_live"] = df["is_live"] | legacy_live
        df["bet_type"] = df.apply(
            lambda r: "LIVE" if bool(r.get("is_live")) else ("PARLAY" if bool(r.get("is_parlay")) else "STRAIGHT"),
            axis=1,
        )
        df["team"] = df.apply(
            lambda r: canonicalize_team(str(r["sport"]) if pd.notnull(r["sport"]) else "", r.get("team")),
            axis=1,
        )
        df["opponent"] = df.apply(
            lambda r: canonicalize_team(str(r["sport"]) if pd.notnull(r["sport"]) else "", r.get("opponent")),
            axis=1,
        )

        df["placed_at_dt"] = pd.to_datetime(df.get("placed_at"), errors="coerce")
        df["settled_at_dt"] = pd.to_datetime(df.get("settled_at"), errors="coerce")

        df["stake"] = pd.to_numeric(df.get("stake"), errors="coerce").fillna(0.0)
        df["units"] = df["stake"] / float(self.unit_size)

        df["implied_prob"] = df.get("odds_american").apply(
            lambda x: american_implied_prob(x) if pd.notnull(x) else None
        )
        df["odds_band"] = df.get("odds_american").apply(odds_band)

        # Safe sort
        df = df.sort_values(["placed_at_dt", "bet_id"], ascending=[True, True], na_position="last")
        return df

    def metrics(self) -> Dict[str, Any]:
        df = self.to_df()
        realized_br = self.realized_bankroll()
        open_exp = self.open_exposure()

        settled = df[df["status"].isin(["WON", "LOST", "VOID"])]
        open_df = df[df["status"] == "OPEN"]

        total_staked_settled = float(settled["stake"].sum()) if not settled.empty else 0.0
        total_pnl_settled = float(settled["pnl"].sum()) if not settled.empty else 0.0
        roi = (total_pnl_settled / total_staked_settled) if total_staked_settled > 0 else 0.0

        wins = int((settled["status"] == "WON").sum()) if not settled.empty else 0
        losses = int((settled["status"] == "LOST").sum()) if not settled.empty else 0
        voids = int((settled["status"] == "VOID").sum()) if not settled.empty else 0
        win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0

        units_pnl = total_pnl_settled / self.unit_size if self.unit_size else 0.0
        open_units = open_exp / self.unit_size if self.unit_size else 0.0

        return {
            "starting_bankroll": self.starting_bankroll,
            "realized_bankroll": realized_br,
            "open_exposure": open_exp,
            "effective_bankroll_if_all_open_lose": realized_br - open_exp,
            "settled_pnl": total_pnl_settled,
            "settled_units_pnl": units_pnl,
            "settled_roi": roi,
            "settled_wins": wins,
            "settled_losses": losses,
            "settled_voids": voids,
            "settled_win_rate": win_rate,
            "open_bets": int(len(open_df)),
            "open_units": open_units,
            "unit_size": self.unit_size,
        }


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="EV Betting Dashboard", layout="wide")
st.title("📈 EV Betting Dashboard")

with st.sidebar:
    st.header("Settings")
    storage_path = "auto"
    st.text_input("Ledger backend", value=get_storage_backend_label(), disabled=True)
    st.divider()
    if st.button("🔄 Reload"):
        st.rerun()

try:
    ledger = Ledger.load(storage_path)
except Exception as e:
    st.error(f"Could not load ledger ({get_storage_backend_label()} backend).\n\n{e}")
    st.stop()

normalized_count = ledger.normalize_existing_bets()
if normalized_count > 0:
    try:
        ledger.save()
        st.info(f"Standardized {normalized_count} existing field values (books/teams/labels).")
    except Exception as e:
        st.warning(f"Normalization changes were made in memory but could not be saved: {e}")

df = ledger.to_df()
m = ledger.metrics()

# KPIs
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Bankroll", f"${m['realized_bankroll']:.2f}")
c2.metric("Open Exposure", f"${m['open_exposure']:.2f}")
c3.metric("Worst-case Bankroll", f"${m['effective_bankroll_if_all_open_lose']:.2f}")
c4.metric("Settled PnL", f"${m['settled_pnl']:.2f}", f"{m['settled_units_pnl']:+.2f}u")
settled_n_all = int((df["status"].isin(["WON", "LOST", "VOID"])).sum()) if not df.empty else 0
c5.metric(
    "Settled ROI",
    f"{m['settled_roi']*100:.2f}%",
    help=f"Settled bets: {settled_n_all} ({sample_flag(settled_n_all)})",
)
c6.metric("Win Rate (settled)", f"{m['settled_win_rate']*100:.1f}%")
if settled_n_all < 30 and settled_n_all > 0:
    st.warning("ROI sample warning: less than 30 settled bets can make ROI noisy.")

open_risk_df = df[df["status"] == "OPEN"].copy()
if not open_risk_df.empty and m["realized_bankroll"] > 0:
    open_risk_df["team_norm"] = open_risk_df["team"].apply(lambda x: None if pd.isna(x) else str(x).strip())
    open_risk_df["matchup_norm"] = open_risk_df.apply(
        lambda r: matchup_key(r.get("team"), r.get("opponent")),
        axis=1,
    )
    open_pct = m["open_exposure"] / m["realized_bankroll"]
    known_team_df = open_risk_df[open_risk_df["team_norm"].notna() & (open_risk_df["team_norm"] != "")]
    known_matchup_df = open_risk_df[open_risk_df["matchup_norm"].notna() & (open_risk_df["matchup_norm"] != "")]
    team_open = known_team_df.groupby("team_norm", dropna=False)["stake"].sum().sort_values(ascending=False) if not known_team_df.empty else pd.Series(dtype=float)
    matchup_open = known_matchup_df.groupby("matchup_norm", dropna=False)["stake"].sum().sort_values(ascending=False) if not known_matchup_df.empty else pd.Series(dtype=float)
    top_team = str(team_open.index[0]) if not team_open.empty else "Unknown"
    top_matchup = str(matchup_open.index[0]) if not matchup_open.empty else "Unknown"
    top_team_pct = float(team_open.iloc[0] / m["open_exposure"]) if (m["open_exposure"] > 0 and not team_open.empty) else 0.0
    top_matchup_pct = float(matchup_open.iloc[0] / m["open_exposure"]) if (m["open_exposure"] > 0 and not matchup_open.empty) else 0.0
    st.caption(
        f"Risk Snapshot: open {open_pct*100:.1f}% of BR | top team {top_team} ({top_team_pct*100:.1f}%) | "
        f"top matchup {top_matchup} ({top_matchup_pct*100:.1f}%)"
    )
    if int(len(open_risk_df)) >= 35:
        if open_pct > 0.20:
            st.warning("Portfolio risk elevated: total open exposure is above 20% of realized bankroll.")
        if top_team_pct > 0.45:
            st.warning(f"Concentration elevated: {top_team} is above 45% of open exposure.")
        if top_matchup_pct > 0.35:
            st.warning(f"Matchup concentration elevated: {top_matchup} is above 35% of open exposure.")

st.divider()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["📊 Dashboard", "➕ New Bet", "✏️ Edit Bets", "✅ Grade Bets", "📉 Closing Odds", "⚙️ Export / Raw"]
)

# -----------------------------
# TAB 1: Dashboard
# -----------------------------
with tab1:
    settled_base = df[df["status"].isin(["WON", "LOST", "VOID"])].copy()
    open_base = df[df["status"] == "OPEN"].copy()

    st.markdown("## Model Performance Dashboard")
    st.caption("Past performance is not indicative of future results.")

    perf1, perf2, perf3, perf4 = st.columns([1.2, 0.9, 1.1, 1.2])
    with perf1:
        perf_window = st.radio("Window", ["All Time", "This Week"], horizontal=True, index=0)
    with perf2:
        season_years = sorted(
            [int(y) for y in settled_base["placed_at_dt"].dropna().dt.year.unique().tolist()],
            reverse=True
        ) if not settled_base.empty else [datetime.now().year]
        season_opts: List[Any] = ["All"] + season_years
        perf_season = st.selectbox("Season", options=season_opts, index=0)
    with perf3:
        perf_sport_options = sorted([x for x in settled_base["sport"].dropna().unique().tolist() if str(x).strip()])
        perf_sports = st.multiselect(
            "Sport",
            options=perf_sport_options,
            default=[],
            placeholder="All sports",
            key="perf_sports_filter",
        )
    with perf4:
        perf_type_options = [x for x in ["LIVE", "STRAIGHT", "PARLAY"] if x in settled_base.get("bet_type", pd.Series(dtype=str)).astype(str).unique().tolist()]
        perf_bet_types = st.multiselect(
            "Bet Type",
            options=perf_type_options,
            default=[],
            placeholder="All types",
            key="perf_bet_type_filter",
        )

    perf_settled = settled_base.copy()
    if not perf_settled.empty:
        if perf_sports:
            perf_settled = perf_settled[perf_settled["sport"].isin(perf_sports)]
        if perf_bet_types:
            perf_settled = perf_settled[perf_settled["bet_type"].isin(perf_bet_types)]
        if perf_season != "All":
            perf_settled = perf_settled[perf_settled["placed_at_dt"].dt.year == int(perf_season)]
        if perf_window == "This Week":
            this_monday = pd.Timestamp.now().normalize() - pd.to_timedelta(pd.Timestamp.now().weekday(), unit="D")
            perf_settled = perf_settled[perf_settled["placed_at_dt"] >= this_monday]

    total_bets_perf = int(len(perf_settled))
    units_risked_perf = float(perf_settled["stake"].sum() / ledger.unit_size) if (not perf_settled.empty and ledger.unit_size) else 0.0
    pnl_perf = float(perf_settled["pnl"].sum()) if not perf_settled.empty else 0.0
    net_units_perf = pnl_perf / ledger.unit_size if ledger.unit_size else 0.0
    wins_perf = int((perf_settled["status"] == "WON").sum()) if not perf_settled.empty else 0
    losses_perf = int((perf_settled["status"] == "LOST").sum()) if not perf_settled.empty else 0
    win_pct_perf = (wins_perf / (wins_perf + losses_perf)) if (wins_perf + losses_perf) > 0 else 0.0
    roi_perf = (pnl_perf / float(perf_settled["stake"].sum())) if (not perf_settled.empty and float(perf_settled["stake"].sum()) > 0) else 0.0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Bets", f"{total_bets_perf}")
    k2.metric("Record", f"{wins_perf}-{losses_perf}")
    k3.metric("Win %", f"{win_pct_perf*100:.1f}%")
    k4.metric("Units Risked", f"{units_risked_perf:.1f}u")
    k5.metric("Net Units", f"{net_units_perf:+.1f}u")
    k6.metric("ROI", f"{roi_perf*100:.2f}%")
    if total_bets_perf < 30 and total_bets_perf > 0:
        st.warning("Performance sample warning: fewer than 30 settled bets in this window.")

    if not perf_settled.empty:
        perf_chart = perf_settled.sort_values("placed_at_dt").copy()
        perf_chart["net_units"] = perf_chart["pnl"] / ledger.unit_size if ledger.unit_size else 0.0
        perf_chart["day"] = perf_chart["placed_at_dt"].dt.floor("D")
        by_day = perf_chart.groupby("day", dropna=False).agg(net_units=("net_units", "sum")).reset_index().sort_values("day")
        by_day["cum_net_units"] = by_day["net_units"].cumsum()

        c_left, c_right = st.columns(2)
        with c_left:
            st.altair_chart(
                alt.Chart(by_day).mark_line(point=True).encode(
                    x=alt.X("day:T", title="Date"),
                    y=alt.Y("cum_net_units:Q", title="Net Units"),
                    tooltip=["day:T", "cum_net_units:Q", "net_units:Q"],
                ).properties(height=220, title="Cumulative Net Units (By Day)"),
                width="stretch"
            )
        with c_right:
            st.altair_chart(
                alt.Chart(by_day).mark_bar().encode(
                    x=alt.X("day:T", title="Date"),
                    y=alt.Y("net_units:Q", title="Net Units"),
                    color=alt.condition(alt.datum.net_units >= 0, alt.value("#2ca02c"), alt.value("#d9534f")),
                    tooltip=["day:T", "net_units:Q"],
                ).properties(height=220, title="Net Units by Day"),
                width="stretch"
            )

        # Calendar for selected perf window
        day_cal = perf_chart.groupby("day", dropna=False).agg(
            bets=("bet_id", "count"),
            staked=("stake", "sum"),
            pnl=("pnl", "sum"),
            net_units=("net_units", "sum"),
        ).reset_index()
        day_cal["roi"] = day_cal["pnl"] / day_cal["staked"]
        day_cal["month"] = day_cal["day"].dt.strftime("%Y-%m")
        month_opts_perf = sorted(day_cal["month"].unique().tolist(), reverse=True)
        chosen_month = st.selectbox("Calendar Month (Performance Window)", options=month_opts_perf, index=0, key="perf_calendar_month")
        cal_plot = build_month_calendar_df(day_cal, chosen_month)
        rect = alt.Chart(cal_plot).mark_rect(cornerRadius=6, stroke="#d1d5db", strokeWidth=1).encode(
            x=alt.X("weekday:O", sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], title=None),
            y=alt.Y("week_idx:O", title=None),
            color=alt.Color(
                "bucket:N",
                scale=alt.Scale(
                    domain=["outside", "neutral", "flat", "pos", "neg"],
                    range=["#f8fafc", "#ffffff", "#eef2ff", "#e9f9ee", "#fdecec"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("yearmonthdate(date):T", title="Date"),
                alt.Tooltip("bets:Q", title="# Bets"),
                alt.Tooltip("staked:Q", title="Staked $"),
                alt.Tooltip("pnl:Q", title="PnL $"),
                alt.Tooltip("net_units:Q", title="Units"),
                alt.Tooltip("roi:Q", title="ROI"),
            ],
        )
        day_numbers = alt.Chart(cal_plot).mark_text(align="left", baseline="top", dx=-22, dy=-12, fontSize=11).encode(
            x=alt.X("weekday:O", sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], title=None),
            y=alt.Y("week_idx:O", title=None),
            text=alt.Text("day_num:N"),
            color=alt.Color("day_label_color:N", scale=None),
        )
        unit_labels = alt.Chart(cal_plot).mark_text(fontSize=12, fontWeight="bold").encode(
            x=alt.X("weekday:O", sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], title=None),
            y=alt.Y("week_idx:O", title=None),
            text=alt.Text("units_label:N"),
            color=alt.Color("units_color:N", scale=None),
        )
        st.altair_chart(
            (rect + day_numbers + unit_labels).properties(
                height=260,
                title=f"Daily Net Units Calendar ({chosen_month})"
            ).configure_axis(labelColor="#475569"),
            width="stretch"
        )

        # Team profitability panels
        team_perf = perf_chart[perf_chart["team"].notna() & (perf_chart["team"].astype(str).str.strip() != "")].copy()
        if not team_perf.empty:
            team_rows = team_perf.groupby("team", dropna=False).agg(
                wins=("status", lambda s: int((s == "WON").sum())),
                losses=("status", lambda s: int((s == "LOST").sum())),
                net_units=("net_units", "sum"),
            ).reset_index()
            team_rows["record"] = team_rows["wins"].astype(str) + "-" + team_rows["losses"].astype(str)
            team_rows["fade_net_units"] = -team_rows["net_units"]
            team_rows["fade_record"] = team_rows["losses"].astype(str) + "-" + team_rows["wins"].astype(str)

            p1, p2 = st.columns(2)
            with p1:
                st.caption("Most / Least Profitable Teams to Bet")
                st.dataframe(
                    pd.concat(
                        [
                            team_rows.sort_values("net_units", ascending=False).head(5).assign(category="Most Profitable"),
                            team_rows.sort_values("net_units", ascending=True).head(5).assign(category="Least Profitable"),
                        ]
                    )[["category", "team", "record", "net_units"]],
                    width="stretch",
                    hide_index=True
                )
            with p2:
                st.caption("Most / Least Profitable Teams to Fade")
                st.dataframe(
                    pd.concat(
                        [
                            team_rows.sort_values("fade_net_units", ascending=False).head(5).assign(category="Most Profitable Fade"),
                            team_rows.sort_values("fade_net_units", ascending=True).head(5).assign(category="Least Profitable Fade"),
                        ]
                    )[["category", "team", "fade_record", "fade_net_units"]].rename(
                        columns={"fade_record": "record", "fade_net_units": "net_units"}
                    ),
                    width="stretch",
                    hide_index=True
                )

        perf_chart["placed_day"] = perf_chart["day"].dt.strftime("%Y-%m-%d")
        recent_cols = ["placed_day", "bet_type", "team", "opponent", "selection", "stake", "status", "pnl", "net_units", "book", "market"]
        recent = perf_chart.sort_values("placed_at_dt", ascending=False).copy().head(25)
        st.caption("Recent Picks")
        st.dataframe(recent[recent_cols], width="stretch", hide_index=True)

    st.divider()

    st.markdown("**Slice Analytics**")
    f1, f2, f3, f4, f5, f6 = st.columns(6)
    with f1:
        sport_filter = st.multiselect(
            "Sport",
            options=sorted([x for x in settled_base["sport"].dropna().unique().tolist() if str(x).strip()]),
            default=[],
            placeholder="All sports",
        )
    with f2:
        market_type_filter = st.multiselect(
            "Market Type",
            options=sorted([x for x in settled_base["market_type"].dropna().unique().tolist() if str(x).strip()]),
            default=[],
            placeholder="All market types",
        )
    with f3:
        devig_filter = st.multiselect(
            "Devig Method",
            options=sorted([x for x in settled_base["devig_method"].dropna().unique().tolist() if str(x).strip()]),
            default=[],
            placeholder="All devig methods",
        )
    with f4:
        book_filter = st.multiselect(
            "Book",
            options=sorted([x for x in settled_base["book"].dropna().unique().tolist() if str(x).strip()]),
            default=[],
            placeholder="All books",
        )
    with f5:
        time_window = st.selectbox(
            "Time Window",
            options=["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Season (YTD)"],
            index=0,
        )
    with f6:
        bet_type_filter = st.multiselect(
            "Bet Type",
            options=sorted([x for x in settled_base["bet_type"].dropna().unique().tolist() if str(x).strip()]),
            default=[],
            placeholder="All types",
        )

    def _apply_dim_filters(local_df: pd.DataFrame) -> pd.DataFrame:
        out = local_df.copy()
        if sport_filter:
            out = out[out["sport"].isin(sport_filter)]
        if market_type_filter:
            out = out[out["market_type"].isin(market_type_filter)]
        if devig_filter:
            out = out[out["devig_method"].isin(devig_filter)]
        if book_filter:
            out = out[out["book"].isin(book_filter)]
        if bet_type_filter:
            out = out[out["bet_type"].isin(bet_type_filter)]
        return out

    settled = _apply_dim_filters(settled_base)
    open_only = _apply_dim_filters(open_base)
    settled = apply_time_window(settled, time_window, "placed_at_dt").sort_values("placed_at_dt", na_position="last")
    open_only = apply_time_window(open_only, time_window, "placed_at_dt")

    if not settled.empty:
        staked_slice = float(settled["stake"].sum())
        pnl_slice = float(settled["pnl"].sum())
        roi_slice = (pnl_slice / staked_slice) if staked_slice > 0 else 0.0
        confidence = roi_confidence_band(settled)
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Settled Bets (Slice)", f"{len(settled)}")
        s2.metric("Staked (Slice)", f"${staked_slice:.2f}")
        s3.metric("PnL (Slice)", f"${pnl_slice:.2f}")
        s4.metric(
            "ROI (Slice)",
            f"{roi_slice*100:.2f}%",
            help=f"Sample size {len(settled)} ({sample_flag(len(settled))}) for current slice.",
        )
        ci_txt = "N/A" if confidence["ci_low"] is None else f"[{confidence['ci_low']*100:.2f}%, {confidence['ci_high']*100:.2f}%]"
        s5.metric("Stability", confidence["stability"], help=f"95% ROI band: {ci_txt} | n={confidence['n']}, staked=${confidence['staked']:.2f}")
    else:
        st.info("No settled bets match the current slice.")

    if not settled.empty:
        settled_day = (
            settled.dropna(subset=["placed_at_dt"])
            .assign(day=lambda x: x["placed_at_dt"].dt.floor("D"))
            .groupby("day", dropna=False)
            .agg(daily_pnl=("pnl", "sum"))
            .reset_index()
            .sort_values("day")
        )
        settled_day["cum_pnl"] = settled_day["daily_pnl"].cumsum()
        pnl_chart = alt.Chart(settled_day).mark_line(point=True).encode(
            x=alt.X("day:T", title="Date"),
            y=alt.Y("cum_pnl:Q", title="Cumulative PnL ($)"),
            tooltip=["day:T", "daily_pnl:Q", "cum_pnl:Q"],
        ).properties(height=300, title="Cumulative PnL (Settled, By Day)")
        st.altair_chart(pnl_chart, width="stretch")
    else:
        st.info("No settled bets yet to plot cumulative PnL.")

    if not open_only.empty:
        by_team = open_only[open_only["team"].notna() & (open_only["team"].astype(str).str.strip() != "")]
        by_team = by_team.groupby("team", dropna=False)["stake"].sum().reset_index().sort_values("stake", ascending=False).head(16)
        if by_team.empty:
            st.info("No team labels on open bets yet.")
        else:
            exp_team = alt.Chart(by_team).mark_bar().encode(
                x=alt.X("stake:Q", title="Open Exposure ($)"),
                y=alt.Y("team:N", sort="-x", title="Team"),
                tooltip=["team:N", "stake:Q"],
            ).properties(height=320, title="Open Exposure by Team")
            st.altair_chart(exp_team, width="stretch")
    else:
        st.info("No open bets.")

    st.subheader("Tables")
    t1, t2 = st.columns(2)

    # IMPORTANT FIX: sort FIRST, then select columns (avoids KeyError)
    open_tbl = open_only.copy().sort_values("placed_at_dt", ascending=False, na_position="last")
    settled_tbl = settled.copy().sort_values("placed_at_dt", ascending=False, na_position="last")
    open_tbl["placed_day"] = open_tbl["placed_at_dt"].dt.strftime("%Y-%m-%d")
    settled_tbl["placed_day"] = settled_tbl["placed_at_dt"].dt.strftime("%Y-%m-%d")

    with t1:
        st.caption("OPEN")
        st.dataframe(
            open_tbl[[
                "bet_id","bet_type","sport","team","opponent","market","market_type","selection","book","devig_method",
                "odds_american","stake","units","placed_day","notes"
            ]],
            width="stretch",
            hide_index=True
        )
    with t2:
        st.caption("SETTLED (most recent)")
        settled_show = settled_tbl[[
                "bet_id","bet_type","sport","team","opponent","market","market_type","selection","book","devig_method",
                "odds_american","stake","units","status","pnl","placed_day"
            ]].head(25).copy()

        def _settled_row_style(row: pd.Series) -> List[str]:
            status = str(row.get("status", "")).upper()
            if status == "WON":
                return ["background-color: #e8f7e8; color: #111111"] * len(row)
            if status == "LOST":
                return ["background-color: #fdecec; color: #111111"] * len(row)
            return [""] * len(row)

        st.dataframe(
            settled_show.style.apply(_settled_row_style, axis=1),
            width="stretch",
            hide_index=True
        )

    st.subheader("Performance Breakdown (Settled)")
    if not settled.empty:
        settled2 = settled.copy()
        settled2["staked"] = settled2["stake"]

        sport_perf = settled2.groupby("sport", dropna=False).agg(
            staked=("staked","sum"), pnl=("pnl","sum"), n=("bet_id","count")
        ).reset_index()
        sport_perf["roi"] = sport_perf["pnl"] / sport_perf["staked"]
        sport_perf["sample_flag"] = sport_perf["n"].apply(sample_flag)

        book_perf = settled2.groupby("book", dropna=False).agg(
            staked=("staked","sum"), pnl=("pnl","sum"), n=("bet_id","count")
        ).reset_index()
        book_perf["roi"] = book_perf["pnl"] / book_perf["staked"]
        book_perf["sample_flag"] = book_perf["n"].apply(sample_flag)

        band_src = settled2[settled2["odds_band"] != "N/A"].copy()
        band_perf = band_src.groupby("odds_band", dropna=False).agg(
            staked=("staked","sum"), pnl=("pnl","sum"), n=("bet_id","count")
        ).reset_index()
        if not band_perf.empty:
            band_perf["roi"] = band_perf["pnl"] / band_perf["staked"]
            band_perf["sample_flag"] = band_perf["n"].apply(sample_flag)

        devig_perf = settled2.groupby("devig_method", dropna=False).agg(
            staked=("staked","sum"), pnl=("pnl","sum"), n=("bet_id","count")
        ).reset_index()
        devig_perf["roi"] = devig_perf["pnl"] / devig_perf["staked"]
        devig_perf["sample_flag"] = devig_perf["n"].apply(sample_flag)

        sport_devig_perf = settled2.groupby(["sport", "devig_method"], dropna=False).agg(
            staked=("staked", "sum"), pnl=("pnl", "sum"), n=("bet_id", "count")
        ).reset_index()
        sport_devig_perf["roi"] = sport_devig_perf["pnl"] / sport_devig_perf["staked"]
        sport_devig_perf = sport_devig_perf.sort_values(["sport", "roi"], ascending=[True, False])

        market_devig_perf = settled2.groupby(["market", "devig_method"], dropna=False).agg(
            staked=("staked", "sum"), pnl=("pnl", "sum"), n=("bet_id", "count")
        ).reset_index()
        market_devig_perf["roi"] = market_devig_perf["pnl"] / market_devig_perf["staked"]
        market_devig_perf = market_devig_perf.sort_values(["market", "roi"], ascending=[True, False])

        p1, p2, p3 = st.columns(3)
        p1.altair_chart(
            alt.Chart(sport_perf).mark_bar().encode(
                x=alt.X("roi:Q", title="ROI"),
                y=alt.Y("sport:N", sort="-x"),
                tooltip=["sport:N","roi:Q","pnl:Q","staked:Q","n:Q","sample_flag:N"]
            ).properties(height=260, title="ROI by Sport"),
            width="stretch"
        )
        p2.altair_chart(
            alt.Chart(book_perf).mark_bar().encode(
                x=alt.X("roi:Q", title="ROI"),
                y=alt.Y("book:N", sort="-x"),
                tooltip=["book:N","roi:Q","pnl:Q","staked:Q","n:Q","sample_flag:N"]
            ).properties(height=260, title="ROI by Book"),
            width="stretch"
        )
        if not band_perf.empty:
            p3.altair_chart(
                alt.Chart(band_perf).mark_bar().encode(
                    x=alt.X("roi:Q", title="ROI"),
                    y=alt.Y("odds_band:N", sort="-x"),
                    tooltip=["odds_band:N","roi:Q","pnl:Q","staked:Q","n:Q","sample_flag:N"]
                ).properties(height=260, title="ROI by Odds Band"),
                width="stretch"
            )
        else:
            p3.info("No valid odds bands in current slice (N/A excluded).")
        st.altair_chart(
            alt.Chart(devig_perf).mark_bar().encode(
                x=alt.X("roi:Q", title="ROI"),
                y=alt.Y("devig_method:N", sort="-x", title="Devig Method"),
                tooltip=["devig_method:N","roi:Q","pnl:Q","staked:Q","n:Q","sample_flag:N"]
            ).properties(height=240, title="ROI by Devig Method"),
            width="stretch"
        )
        st.markdown("**Devig Drilldowns**")
        d1, d2 = st.columns(2)
        with d1:
            st.caption("By Sport x Devig")
            st.dataframe(
                sport_devig_perf[["sport", "devig_method", "n", "staked", "pnl", "roi"]],
                width="stretch",
                hide_index=True,
            )
        with d2:
            st.caption("By Market x Devig")
            st.dataframe(
                market_devig_perf[["market", "devig_method", "n", "staked", "pnl", "roi"]],
                width="stretch",
                hide_index=True,
            )

        st.markdown("**Strategy & Matchup Intelligence**")
        intel = settled2.copy()
        intel["team_clean"] = intel["team"].fillna("").astype(str).str.strip()
        intel["opp_clean"] = intel["opponent"].fillna("").astype(str).str.strip()
        intel["matchup"] = intel.apply(
            lambda r: f"{r['team_clean']} vs {r['opp_clean']}" if r["team_clean"] and r["opp_clean"] else "Unknown",
            axis=1,
        )

        strategy_perf = intel.groupby(["book", "devig_method", "market_type"], dropna=False).agg(
            staked=("stake", "sum"),
            pnl=("pnl", "sum"),
            n=("bet_id", "count"),
            avg_stake=("stake", "mean"),
        ).reset_index()
        strategy_perf["roi"] = strategy_perf["pnl"] / strategy_perf["staked"]
        strategy_perf = strategy_perf.sort_values(["roi", "staked"], ascending=[False, False])

        matchup_perf = intel[intel["matchup"] != "Unknown"].groupby("matchup", dropna=False).agg(
            staked=("stake", "sum"),
            pnl=("pnl", "sum"),
            n=("bet_id", "count"),
        ).reset_index()
        if not matchup_perf.empty:
            matchup_perf["roi"] = matchup_perf["pnl"] / matchup_perf["staked"]
            matchup_perf = matchup_perf.sort_values(["staked", "n"], ascending=[False, False])

        i1, i2 = st.columns(2)
        with i1:
            st.caption("Efficiency by Book x Devig x Market Type")
            st.dataframe(
                strategy_perf[["book", "devig_method", "market_type", "n", "staked", "avg_stake", "pnl", "roi"]].head(30),
                width="stretch",
                hide_index=True,
            )
        with i2:
            st.caption("Matchup Performance (Known Team/Opponent)")
            if matchup_perf.empty:
                st.info("No settled bets with both team and opponent populated yet.")
            else:
                st.dataframe(
                    matchup_perf[["matchup", "n", "staked", "pnl", "roi"]].head(25),
                    width="stretch",
                    hide_index=True,
                )

                matchup_chart_df = matchup_perf.sort_values("staked", ascending=False).head(12)
                st.altair_chart(
                    alt.Chart(matchup_chart_df).mark_bar().encode(
                        x=alt.X("roi:Q", title="ROI"),
                        y=alt.Y("matchup:N", sort="-x", title="Matchup"),
                        tooltip=["matchup:N", "roi:Q", "pnl:Q", "staked:Q", "n:Q"],
                    ).properties(height=280, title="ROI by Matchup (Top Staked)"),
                    width="stretch",
                )

        st.markdown("**Advanced Analytics**")
        cal_df = settled2.dropna(subset=["placed_at_dt"]).copy()
        if not cal_df.empty:
            cal_df["placed_date"] = cal_df["placed_at_dt"].dt.floor("D")
            daily = cal_df.groupby("placed_date", dropna=False).agg(
                bets=("bet_id", "count"),
                staked=("stake", "sum"),
                pnl=("pnl", "sum"),
            ).reset_index()
            daily["units"] = daily["pnl"] / float(ledger.unit_size) if ledger.unit_size else 0.0
            daily["roi"] = daily["pnl"] / daily["staked"]
            daily["month"] = daily["placed_date"].dt.strftime("%Y-%m")

            st.markdown("**Daily Performance Calendar**")
            csel1, csel2 = st.columns(2)
            with csel1:
                month_opts = sorted(daily["month"].dropna().unique().tolist(), reverse=True)
                cal_month = st.selectbox("Calendar Month", options=month_opts, index=0)
            with csel2:
                cal_metric = st.selectbox("Calendar Metric", options=["Units", "ROI %", "PnL $"], index=0)

            month_daily = daily[daily["month"] == cal_month].copy()
            metric_field = "units" if cal_metric == "Units" else ("roi" if cal_metric == "ROI %" else "pnl")
            color_title = "Units" if cal_metric == "Units" else ("ROI" if cal_metric == "ROI %" else "PnL")
            if cal_metric == "ROI %":
                month_daily["roi_display"] = month_daily["roi"] * 100.0
                metric_field = "roi_display"
                color_title = "ROI %"

            cal_chart = alt.Chart(month_daily).mark_rect(cornerRadius=4).encode(
                x=alt.X("day(placed_date):O", title="Day of Month"),
                y=alt.Y("week(placed_date):O", title="Week"),
                color=alt.Color(
                    f"{metric_field}:Q",
                    title=color_title,
                    scale=alt.Scale(scheme="redyellowgreen", domainMid=0),
                ),
                tooltip=[
                    alt.Tooltip("yearmonthdate(placed_date):T", title="Date"),
                    alt.Tooltip("bets:Q", title="# Bets"),
                    alt.Tooltip("staked:Q", title="Staked $"),
                    alt.Tooltip("pnl:Q", title="PnL $"),
                    alt.Tooltip("units:Q", title="Units"),
                    alt.Tooltip("roi:Q", title="ROI"),
                ],
            ).properties(height=240, title=f"Daily {cal_metric} Calendar ({cal_month})")
            st.altair_chart(cal_chart, width="stretch")

            w1, w2, w3 = st.columns(3)
            month_staked = float(month_daily["staked"].sum())
            month_pnl = float(month_daily["pnl"].sum())
            month_roi = (month_pnl / month_staked) if month_staked > 0 else 0.0
            w1.metric("Month PnL", f"${month_pnl:.2f}")
            w2.metric("Month Units", f"{(month_pnl / ledger.unit_size) if ledger.unit_size else 0.0:+.2f}u")
            w3.metric("Month ROI", f"{month_roi*100:.2f}%", help=f"Month sample {int(month_daily['bets'].sum())} ({sample_flag(int(month_daily['bets'].sum()))})")
        else:
            st.info("No settled dates available for daily calendar yet.")

        a1, a2 = st.columns(2)
        with a1:
            st.caption("CLV (Closing Line Value)")
            clv_df = clv_metrics(settled2)
            if clv_df.empty:
                st.info("No CLV yet. Add closing odds when grading to unlock this panel.")
            else:
                st.caption("CLV uses Non-Boosted Entry Odds when available; otherwise As-Placed Odds.")
                c1, c2, c3 = st.columns(3)
                c1.metric("Bets With CLV", f"{len(clv_df)}")
                c2.metric("Avg CLV", f"{clv_df['clv_pct'].mean()*100:.2f}%")
                c3.metric("Positive CLV Rate", f"{(clv_df['clv_pct'] > 0).mean()*100:.1f}%")
                clv_trend = clv_df.sort_values("placed_at_dt").copy()
                clv_trend["cum_avg_clv_pct"] = clv_trend["clv_pct"].expanding().mean() * 100.0
                st.altair_chart(
                    alt.Chart(clv_trend).mark_line().encode(
                        x=alt.X("placed_at_dt:T", title="Placed Time"),
                        y=alt.Y("cum_avg_clv_pct:Q", title="Cumulative Avg CLV (%)"),
                        tooltip=["placed_at_dt:T", "clv_pct:Q", "cum_avg_clv_pct:Q", "clv_entry_source:N", "book:N", "devig_method:N", "market:N"],
                    ).properties(height=240, title="CLV Trend (Cumulative Avg)"),
                    width="stretch",
                )

        with a2:
            st.caption("Drawdown")
            dd_df = settled2.sort_values("placed_at_dt").copy()
            dd_df["cum_pnl"] = dd_df["pnl"].cumsum()
            dd_df["bankroll"] = ledger.starting_bankroll + dd_df["cum_pnl"]
            dd_df["peak_bankroll"] = dd_df["bankroll"].cummax()
            dd_df["drawdown_pct"] = ((dd_df["bankroll"] - dd_df["peak_bankroll"]) / dd_df["peak_bankroll"]).fillna(0.0)
            if dd_df.empty:
                st.info("No settled bets for drawdown.")
            else:
                max_dd = float(dd_df["drawdown_pct"].min())
                current_dd = float(dd_df["drawdown_pct"].iloc[-1])
                d1, d2 = st.columns(2)
                d1.metric("Max Drawdown", f"{max_dd*100:.2f}%")
                d2.metric("Current Drawdown", f"{current_dd*100:.2f}%")
                st.altair_chart(
                    alt.Chart(dd_df).mark_line(color="#d9534f").encode(
                        x=alt.X("placed_at_dt:T", title="Placed Time"),
                        y=alt.Y("drawdown_pct:Q", title="Drawdown (%)"),
                        tooltip=["placed_at_dt:T", "drawdown_pct:Q", "bankroll:Q", "peak_bankroll:Q"],
                    ).properties(height=240, title="Drawdown Curve"),
                    width="stretch",
                )

        st.caption("Confidence by strategy slices (ROI with 95% band and stability)")
        confidence_rows: List[Dict[str, Any]] = []
        for dims in [["book"], ["devig_method"], ["market_type"], ["book", "devig_method", "market_type"]]:
            gdf = settled2.groupby(dims, dropna=False)
            for key, sub in gdf:
                stats = roi_confidence_band(sub)
                label = key if isinstance(key, str) else " | ".join([str(x) for x in (key if isinstance(key, tuple) else [key])])
                confidence_rows.append({
                    "slice": "+".join(dims),
                    "value": label,
                    "n": stats["n"],
                    "staked": stats["staked"],
                    "roi": stats["roi"],
                    "ci_low": stats["ci_low"],
                    "ci_high": stats["ci_high"],
                    "stability": stats["stability"],
                })
        conf_df = pd.DataFrame(confidence_rows)
        if not conf_df.empty:
            order_map = {"High": 0, "Medium": 1, "Low": 2}
            conf_df["stability_order"] = conf_df["stability"].map(order_map).fillna(9)
            conf_df = conf_df.sort_values(["stability_order", "n", "staked"], ascending=[True, False, False]).drop(columns=["stability_order"])
        st.dataframe(conf_df.head(40), width="stretch", hide_index=True)

        st.markdown("**Top Contexts & Rolling Signals**")
        top_ctx = settled2.copy()
        top_ctx["matchup"] = top_ctx.apply(lambda r: matchup_key(r.get("team"), r.get("opponent")) or "Unknown", axis=1)
        top_ctx["edge"] = top_ctx.apply(expected_edge_per_dollar, axis=1)
        top_ctx = top_ctx[top_ctx["edge"].notna()].copy()
        if not top_ctx.empty:
            ctx = top_ctx.groupby(["book", "devig_method", "market_type", "matchup"], dropna=False).agg(
                n=("bet_id", "count"),
                staked=("stake", "sum"),
                pnl=("pnl", "sum"),
                avg_edge=("edge", "mean"),
            ).reset_index()
            ctx["roi"] = ctx["pnl"] / ctx["staked"]
            ctx = ctx[ctx["n"] >= 8].copy()
            if not ctx.empty:
                ctx["context_score"] = (ctx["avg_edge"] * 100.0) * ctx["n"].pow(0.5)
                ctx = ctx.sort_values(["context_score", "roi", "staked"], ascending=[False, False, False])
                st.caption("Top 5 +EV contexts (min 8 bets in-context)")
                st.dataframe(
                    ctx[["book", "devig_method", "market_type", "matchup", "n", "avg_edge", "roi", "pnl", "staked"]].head(5),
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.info("Not enough EV-tagged samples by context yet (need min 8 per context).")
        else:
            st.info("No EV/true-prob tagged settled bets yet for +EV context ranking.")

        roll_df = settled2.sort_values("placed_at_dt").copy()
        roll_df["ret_per_dollar"] = roll_df.apply(
            lambda r: (float(r["pnl"]) / float(r["stake"])) if pd.notnull(r["stake"]) and float(r["stake"]) > 0 else 0.0,
            axis=1,
        )
        roll_df["rolling_30_roi_pct"] = roll_df["ret_per_dollar"].rolling(30, min_periods=10).mean() * 100.0
        clv_roll = clv_metrics(roll_df)
        if not clv_roll.empty:
            clv_roll = clv_roll.sort_values("placed_at_dt").copy()
            clv_roll["rolling_30_clv_pct"] = clv_roll["clv_pct"].rolling(30, min_periods=10).mean() * 100.0

        r1, r2 = st.columns(2)
        with r1:
            st.caption("Rolling 30-Bet ROI")
            st.altair_chart(
                alt.Chart(roll_df.dropna(subset=["rolling_30_roi_pct"])).mark_line(color="#1f77b4").encode(
                    x=alt.X("placed_at_dt:T", title="Placed Time"),
                    y=alt.Y("rolling_30_roi_pct:Q", title="Rolling ROI (%)"),
                    tooltip=["placed_at_dt:T", "rolling_30_roi_pct:Q", "ret_per_dollar:Q"],
                ).properties(height=220, title="Rolling 30-Bet ROI"),
                width="stretch",
            )
        with r2:
            st.caption("Rolling 30-Bet CLV")
            if clv_roll.empty:
                st.info("No closing odds yet for rolling CLV.")
            else:
                st.altair_chart(
                    alt.Chart(clv_roll.dropna(subset=["rolling_30_clv_pct"])).mark_line(color="#2ca02c").encode(
                        x=alt.X("placed_at_dt:T", title="Placed Time"),
                        y=alt.Y("rolling_30_clv_pct:Q", title="Rolling CLV (%)"),
                        tooltip=["placed_at_dt:T", "rolling_30_clv_pct:Q", "clv_pct:Q"],
                    ).properties(height=220, title="Rolling 30-Bet CLV"),
                    width="stretch",
                )

        st.caption("Stake Utilization (Actual vs Recommended Snapshot)")
        util_df = settled2.copy()
        util_df = util_df[util_df["recommended_stake_snapshot"].notna()].copy()
        if util_df.empty:
            st.info("No utilization data yet. New bets now store recommended snapshot for this panel.")
        else:
            util_df = util_df[util_df["recommended_stake_snapshot"] > 0].copy()
            if util_df.empty:
                st.info("No positive recommended stake snapshots yet for utilization.")
            else:
                util_df["utilization_ratio"] = util_df["stake"] / util_df["recommended_stake_snapshot"]
                u1, u2, u3 = st.columns(3)
                u1.metric("Avg Utilization", f"{util_df['utilization_ratio'].mean():.2f}x")
                u2.metric("% Over 1.0x", f"{(util_df['utilization_ratio'] > 1.0).mean()*100:.1f}%")
                u3.metric("Tracked Bets", f"{len(util_df)}")
                by_source = util_df.groupby("stake_source", dropna=False).agg(
                    n=("bet_id", "count"),
                    avg_util=("utilization_ratio", "mean"),
                    roi=("pnl", lambda s: float(s.sum()) / float(util_df.loc[s.index, 'stake'].sum()) if float(util_df.loc[s.index, 'stake'].sum()) > 0 else 0.0),
                ).reset_index().sort_values("n", ascending=False)
                st.dataframe(by_source, width="stretch", hide_index=True)

        sim = simulate_multiplier_impact(settled2, total_threshold=1000, min_slice_n=30)
        st.markdown("**Multiplier Backtest**")
        if sim.get("ready"):
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Actual PnL", f"${sim['actual_pnl']:.2f}")
            m2.metric("Simulated PnL", f"${sim['sim_pnl']:.2f}")
            m3.metric("Uplift", f"${sim['uplift']:.2f}")
            m4.metric("Bets Multiplied", f"{sim['bets_with_multiplier']}/{sim['total_bets']}")
            st.caption("Simulation applies the same multiplier logic using only prior settled history available at each bet timestamp.")
        else:
            st.info(sim.get("reason", "Multiplier simulation unavailable."))
    else:
        st.info("No settled bets yet for performance breakdown charts.")

# -----------------------------
# TAB 2: New Bet
# -----------------------------
with tab2:
    st.subheader("New Bet")
    st.caption("Type inside dropdowns to search prior entries. Similar entries are standardized automatically.")

    sport_options = history_options(ledger, "sport", ["NHL", "Intl Hockey", "NBA", "CBB", "NFL", "MLB", "EPL", "UFC"])
    book_options = ["DraftKings", "FanDuel", "BetMGM", "Caesars", "Fanatics", "BetRivers", "theScore", "Pinnacle"]

    colA, colB, colC = st.columns(3)
    with colA:
        sport_pick = st.selectbox("Sport", options=sport_options, index=0)
        sport_for_preset = canonicalize_value("sport", sport_pick)
        market_defaults = MARKET_PRESETS_BY_SPORT.get(sport_for_preset, DEFAULT_MARKET_OPTIONS)
        market_options = history_options(ledger, "market", market_defaults + DEFAULT_MARKET_OPTIONS)
        market_input_mode = st.radio(
            "Market Input",
            options=["Select existing", "Type custom"],
            horizontal=True,
            key="market_input_mode",
        )
        if market_input_mode == "Type custom":
            market_pick = st.text_input("Market (custom)", value="", placeholder="e.g., Fighter Takedowns")
        else:
            market_pick = st.selectbox("Market", options=market_options, index=0)

        market_type_default = infer_market_type(canonicalize_value("market", market_pick))
        market_type_input_mode = st.radio(
            "Market Type Input",
            options=["Select existing", "Type custom"],
            horizontal=True,
            key="market_type_input_mode",
        )
        if market_type_input_mode == "Type custom":
            market_type = st.text_input("Market Type (custom)", value=market_type_default, placeholder="e.g., Prop")
        else:
            market_type = st.selectbox(
                "Market Type",
                options=["Game", "Team", "Player", "Period", "Other"],
                index=["Game", "Team", "Player", "Period", "Other"].index(
                    market_type_default if market_type_default in ["Game", "Team", "Player", "Period", "Other"] else "Other"
                ),
            )
        selection = st.text_input("Selection", value="")

        team_list = TEAM_OPTIONS_BY_SPORT.get(sport_for_preset, [])
        team_input_mode = st.radio(
            "Team/Opponent Input",
            options=["Select existing", "Type custom"],
            horizontal=True,
            key="team_opp_input_mode",
        )
        if team_list and team_input_mode == "Select existing":
            team_pick = st.selectbox("Team (optional)", options=[""] + team_list, index=0)
            opponent_pick = st.selectbox("Opponent (optional)", options=[""] + team_list, index=0)
        else:
            team_pick = st.text_input("Team (optional)", value="")
            opponent_pick = st.text_input("Opponent (optional)", value="")
    with colB:
        book_pick = st.selectbox("Book", options=book_options, index=0)
        odds_american = float(st.number_input("Odds (American)", value=-110, step=1))
        boost_pct_input = st.text_input("Boost % (optional)", value="", placeholder="e.g., 20 for 20%")
        notes = st.text_input("Notes (optional)", value="")
        devig_method = st.selectbox(
            "Devig Method",
            options=DEVIG_METHOD_OPTIONS,
            index=0
        )
        devig_requires_details = devig_method in {"Single Book (100%)", "Split Weights"}
        devig_details = st.text_input(
            "Devig Details (required)" if devig_requires_details else "Devig Details (optional)",
            value="",
            placeholder="e.g., Pinnacle 100% or FanDuel 50% / DraftKings 50%",
            help="Required for Single Book (100%) and Split Weights. Include book names and percentages.",
        )
    with colC:
        kelly_units = st.text_input("EVSharps 1/4-Kelly units (optional)", value="")
        fair_odds = st.text_input("Fair odds (American) (optional)", value="")
        true_prob = st.text_input("True probability p (0-1) (optional)", value="")

    sport = canonicalize_value("sport", sport_pick)
    market = canonicalize_value("market", market_pick)
    market_type_final = canonicalize_value("market_type", market_type)
    book = canonicalize_value("book", book_pick)
    team = canonicalize_team(sport, team_pick)
    opponent = canonicalize_team(sport, opponent_pick)

    if sport in {"NHL", "Intl Hockey", "NBA", "CBB"}:
        st.caption(f"{sport} matchup tracking enabled: optional Team/Opponent fields will be saved.")
    if team is not None and opponent is not None and normalize_token(team) == normalize_token(opponent):
        st.warning("Team and Opponent are the same. Double-check matchup entry.")

    with st.expander("Sizing Settings", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            kelly_fraction = st.selectbox("Kelly fraction", [0.10, 0.20, 0.25, 0.33, 0.50], index=2)
        with c2:
            max_frac_br = st.selectbox("Max % bankroll cap", [0.01, 0.02, 0.03, 0.05], index=2)
        with c3:
            min_stake = st.selectbox("Min stake", [1.0, 2.0, 2.5, 5.0], index=0)
        with c4:
            round_step = st.selectbox("Round to", [0.25, 0.50, 1.0], index=0)

    rec = None
    rec_error = None
    parse_error = None

    try:
        kelly_units_val = float(kelly_units) if kelly_units.strip() else None
        fair_odds_val = float(fair_odds) if fair_odds.strip() else None
        true_prob_val = float(true_prob) if true_prob.strip() else None
        boost_pct_val = float(boost_pct_input) if boost_pct_input.strip() else None
        if boost_pct_val is not None and boost_pct_val < 0:
            raise ValueError("Boost % cannot be negative.")
    except Exception:
        parse_error = "Inputs for EVSharps units / fair odds / true probability / boost % must be numeric."
        kelly_units_val, fair_odds_val, true_prob_val, boost_pct_val = None, None, None, None

    has_sizing_signal = any(x is not None for x in [kelly_units_val, fair_odds_val, true_prob_val])
    if parse_error:
        st.warning(parse_error)
    elif odds_american == 0:
        st.warning("Odds cannot be 0.")
    elif has_sizing_signal:
        try:
            rec = ledger.recommend_stake(
                odds_american=odds_american,
                fair_odds_american=fair_odds_val,
                true_prob=true_prob_val,
                kelly_units_from_tool=kelly_units_val,
                kelly_fraction=float(kelly_fraction),
                max_fraction_of_bankroll=float(max_frac_br),
                min_stake=float(min_stake),
                round_step=float(round_step),
            )
        except Exception as e:
            rec_error = str(e)
            st.warning(f"Recommendation unavailable: {rec_error}")
    else:
        st.info("Enter EVSharps units, fair odds, or true probability to see an automatic recommendation.")

    if rec is not None:
        st.markdown("### Suggested Stake")
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Recommended", f"${rec['recommended_stake']:.2f}")
        r2.metric("Cap Status", "CAPPED" if rec["was_capped"] else "Not capped")
        r3.metric("Raw Before Cap", f"${rec['raw_stake_before_cap']:.2f}")
        r4.metric("Cap Amount", f"${rec['cap_amount']:.2f}")

        p1, p2, p3 = st.columns(3)
        potential_profit = profit_on_win(rec["recommended_stake"], odds_american)
        post_open_exposure = m["open_exposure"] + rec["recommended_stake"]
        worst_case_after = m["realized_bankroll"] - post_open_exposure
        p1.metric("Potential Profit (Win)", f"${potential_profit:.2f}")
        p2.metric("Open Exposure if Placed", f"${post_open_exposure:.2f}")
        p3.metric("Worst-case BR After Place", f"${worst_case_after:.2f}")

        st.caption(
            f"Sizing path: raw ${rec['raw_stake_before_cap']:.2f} -> "
            f"cap ${rec['cap_amount']:.2f} -> post-cap ${rec['stake_after_cap_before_min_round']:.2f} -> "
            f"rounded ${rec['recommended_stake']:.2f}"
        )

    st.divider()
    stake_mode = st.radio(
        "Stake mode",
        options=["Use recommended stake", "Manual stake"],
        horizontal=True,
        index=0 if rec is not None else 1,
        key="stake_mode_choice",
    )
    manual_stake = st.number_input("Manual stake ($)", value=1.0, min_value=0.0, step=0.25, key="manual_stake_input")
    confirm_add = st.checkbox("Confirm add as OPEN", value=False)

    proposed_stake = float(manual_stake) if stake_mode == "Manual stake" else (float(rec["recommended_stake"]) if rec else 0.0)
    open_only_df = df[df["status"] == "OPEN"].copy()
    team_open_now = 0.0
    matchup_open_now = 0.0
    target_matchup = matchup_key(team, opponent)
    if not open_only_df.empty:
        if team:
            team_open_now = float(
                open_only_df[
                    open_only_df["team"].apply(lambda x: ("" if pd.isna(x) else str(x).strip()) == team)
                ]["stake"].sum()
            )
        if target_matchup:
            matchup_open_now = float(
                open_only_df[
                    open_only_df.apply(lambda r: matchup_key(r.get("team"), r.get("opponent")) == target_matchup, axis=1)
                ]["stake"].sum()
            )

    open_after = m["open_exposure"] + proposed_stake
    open_bets_after = int(len(open_only_df) + (1 if proposed_stake > 0 else 0))
    realized_br = m["realized_bankroll"]
    open_after_pct = (open_after / realized_br) if realized_br > 0 else 0.0
    team_concentration_after = ((team_open_now + proposed_stake) / open_after) if (open_after > 0 and team) else 0.0
    matchup_concentration_after = ((matchup_open_now + proposed_stake) / open_after) if (open_after > 0 and target_matchup) else 0.0
    concentration_enabled = open_bets_after >= 20

    st.markdown("### Risk Checks")
    rc1, rc2, rc3 = st.columns(3)
    rc1.metric("Open Exposure / BR (after)", f"{open_after_pct*100:.1f}%")
    rc2.metric("Team Concentration (after)", f"{team_concentration_after*100:.1f}%" if team else "N/A")
    rc3.metric("Matchup Concentration (after)", f"{matchup_concentration_after*100:.1f}%" if target_matchup else "N/A")

    if open_after_pct > 0.20:
        st.warning("High risk: open exposure would exceed 20% of realized bankroll.")
    elif open_after_pct > 0.12:
        st.info("Moderate risk: open exposure would exceed 12% of realized bankroll.")
    if concentration_enabled and team and team_concentration_after > 0.45:
        st.warning(f"Concentration risk: more than 45% of open exposure would be in {team}.")
    if concentration_enabled and target_matchup and matchup_concentration_after > 0.35:
        st.warning(f"Matchup risk: more than 35% of open exposure would be tied to {target_matchup}.")
    if not concentration_enabled:
        st.caption(f"Concentration checks activate at 20+ open bets (projected open bets after place: {open_bets_after}).")
    if not team or not opponent:
        st.info("Add Team + Opponent to enable full matchup concentration checks.")
    if rec is not None and stake_mode == "Manual stake" and manual_stake > rec["cap_amount"]:
        st.warning("Manual stake exceeds current bankroll cap amount from sizing settings.")

    if rec is not None:
        vs = value_score(
            rec=rec,
            open_exposure_after=open_after,
            bankroll=realized_br,
            team_concentration_after=team_concentration_after,
            matchup_concentration_after=matchup_concentration_after,
            open_bets_after=open_bets_after,
            concentration_min_open_bets=20,
        )
        st.markdown("### Value Score")
        v1, v2, v3 = st.columns(3)
        v1.metric(
            "Score",
            f"{vs['score']:.0f}/100",
            help="Composite from EV edge, cap status, open exposure %, team concentration, and matchup concentration.",
        )
        v2.metric("Grade", vs["grade"])
        v3.metric("Verdict", vs["verdict"], help="A/B strong-good, C neutral, D caution, F pass.")
        with st.expander("Why this score?"):
            st.write(f"Base: {vs['base']:.0f}")
            for factor in vs["factors"]:
                st.write(f"- {factor['label']}: {factor['delta']:+.1f}")
            st.write(f"- Open exposure after bet: {vs['open_exposure_pct']*100:.1f}%")
            st.write(f"- Team concentration after bet: {vs['team_concentration_after']*100:.1f}%")
            st.write(f"- Matchup concentration after bet: {vs['matchup_concentration_after']*100:.1f}%")
            st.write(
                f"- Concentration impact active: {'Yes' if vs['concentration_applied'] else 'No'} "
                f"(requires {vs['concentration_min_open_bets']}+ open bets, projected {vs['open_bets_after']})"
            )

    devig_valid = True
    try:
        validate_devig_details(devig_method, devig_details)
    except Exception as e:
        devig_valid = False
        st.warning(str(e))

    rec_stake_for_add = float(rec["recommended_stake"]) if rec is not None else 0.0
    apply_hist_mult = False
    if rec is not None:
        settled_all = df[df["status"].isin(["WON", "LOST", "VOID"])].copy()
        mult_info = historical_stake_multiplier(
            settled_df=settled_all,
            sport=sport,
            book=book,
            devig_method=devig_method,
            market_type=market_type_final,
            total_threshold=1000,
            min_slice_n=30,
        )
        if mult_info["ready"]:
            st.markdown("### Historical Stake Multiplier")
            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Multiplier", f"{mult_info['multiplier']:.2f}x")
            h2.metric("Scope", mult_info["scope"])
            h3.metric("Sample Size", f"{mult_info['n']}")
            h4.metric("ROI (Scope)", f"{mult_info['roi']*100:.2f}%")
            apply_hist_mult = st.checkbox("Apply historical multiplier to recommended stake", value=False)
            if apply_hist_mult:
                rec_stake_for_add = round_to(rec_stake_for_add * float(mult_info["multiplier"]), float(round_step))
                st.caption(
                    f"Adjusted recommendation: ${rec['recommended_stake']:.2f} -> ${rec_stake_for_add:.2f} "
                    f"(rounded to {round_step}, confidence {mult_info['confidence']*100:.0f}%)"
                )
        else:
            st.caption(f"Historical multiplier locked: {mult_info['reason']}")

    if st.button("➕ Add OPEN Bet", disabled=not confirm_add):
        try:
            if odds_american == 0:
                raise ValueError("Odds cannot be 0.")
            if parse_error:
                raise ValueError(parse_error)
            if not devig_valid:
                raise ValueError("Fix Devig Details for selected Devig Method.")

            if stake_mode == "Manual stake":
                stake = float(manual_stake)
                if stake <= 0:
                    raise ValueError("Manual stake must be greater than 0.")
                stake_source = "Manual"
            else:
                if rec is None:
                    raise ValueError("Recommendation unavailable. Provide sizing inputs or switch to Manual stake.")
                stake = float(rec_stake_for_add)
                stake_source = "Recommended + Historical Multiplier" if apply_hist_mult else "Recommended"

            unboosted_for_store: Optional[float] = None
            if boost_pct_val is not None and boost_pct_val > 0 and odds_american != 0:
                try:
                    unboosted_for_store = float(unboosted_american_from_boosted(odds_american, boost_pct_val))
                except Exception:
                    unboosted_for_store = None

            bet_id = ledger.add_bet(
                sport=sport,
                team=team,
                opponent=opponent,
                market=market,
                market_type=market_type_final,
                selection=selection,
                book=book,
                devig_method=devig_method,
                devig_details=devig_details,
                recommended_stake_snapshot=float(rec_stake_for_add) if rec is not None else None,
                stake_source=stake_source,
                odds_american=odds_american,
                stake=stake,
                fair_odds_american=fair_odds_val,
                true_prob=true_prob_val,
                kelly_fraction_used=float(kelly_fraction) if kelly_units_val is None else None,
                kelly_units_from_tool=kelly_units_val,
                boost_pct=boost_pct_val,
                unboosted_odds_american=unboosted_for_store,
                notes=notes,
            )
            ledger.save()
            st.success(f"Added OPEN bet: {bet_id}")
            if rec is not None:
                st.markdown(
                    f"**Sizing:** {'CAPPED' if rec['was_capped'] else 'Not capped'} | "
                    f"raw ${rec['raw_stake_before_cap']:.2f} -> cap ${rec['cap_amount']:.2f} -> rec ${rec['recommended_stake']:.2f}"
                )
            st.rerun()
        except Exception as e:
            st.error(f"Failed to add bet: {e}")

    st.divider()
    st.markdown("### Parlay Builder (2-8 Legs + Boost)")
    st.caption("Build and log parlays with per-leg details, boost-aware sizing, and value scoring.")

    parlay_leg_count = int(st.number_input("Number of Legs", min_value=2, max_value=8, value=3, step=1, key="parlay_leg_count"))
    parlay_boost_pct = st.number_input("Parlay Boost % (optional)", min_value=0.0, max_value=500.0, value=0.0, step=1.0, key="parlay_boost_pct")
    parlay_book = st.selectbox("Parlay Book", options=book_options, index=0, key="parlay_book_pick")
    parlay_devig_method = st.selectbox("Parlay Devig Method", options=DEVIG_METHOD_OPTIONS, index=0, key="parlay_devig_method")
    parlay_devig_details = st.text_input(
        "Parlay Devig Details (required for Single/Split)" if parlay_devig_method in {"Single Book (100%)", "Split Weights"} else "Parlay Devig Details (optional)",
        value="",
        key="parlay_devig_details",
    )
    parlay_label = st.text_input("Parlay Label (optional)", value="", placeholder="e.g., Friday 4-Leg NHL", key="parlay_label")

    parlay_legs_input: List[Dict[str, Any]] = []
    for i in range(parlay_leg_count):
        with st.expander(f"Leg {i+1}", expanded=(i < 2)):
            lc1, lc2, lc3 = st.columns(3)
            with lc1:
                leg_sport = st.selectbox(f"Sport {i+1}", options=sport_options, index=0, key=f"parlay_leg_{i}_sport")
                leg_market_defaults = MARKET_PRESETS_BY_SPORT.get(canonicalize_value("sport", leg_sport), DEFAULT_MARKET_OPTIONS)
                leg_market_options = history_options(ledger, "market", leg_market_defaults + DEFAULT_MARKET_OPTIONS)
                leg_market = st.selectbox(f"Market {i+1}", options=leg_market_options, index=0, key=f"parlay_leg_{i}_market")
                leg_selection = st.text_input(f"Selection {i+1}", value="", key=f"parlay_leg_{i}_selection")
            with lc2:
                leg_team_options = TEAM_OPTIONS_BY_SPORT.get(canonicalize_value("sport", leg_sport), [])
                if leg_team_options:
                    leg_team = st.selectbox(f"Team {i+1} (optional)", options=[""] + leg_team_options, index=0, key=f"parlay_leg_{i}_team")
                    leg_opponent = st.selectbox(f"Opponent {i+1} (optional)", options=[""] + leg_team_options, index=0, key=f"parlay_leg_{i}_opp")
                    leg_team_override = st.text_input(f"Team {i+1} override (optional)", value="", key=f"parlay_leg_{i}_team_override")
                    leg_opp_override = st.text_input(f"Opponent {i+1} override (optional)", value="", key=f"parlay_leg_{i}_opp_override")
                else:
                    leg_team = st.text_input(f"Team {i+1} (optional)", value="", key=f"parlay_leg_{i}_team_txt")
                    leg_opponent = st.text_input(f"Opponent {i+1} (optional)", value="", key=f"parlay_leg_{i}_opp_txt")
                    leg_team_override = ""
                    leg_opp_override = ""
            with lc3:
                leg_odds = float(st.number_input(f"Odds {i+1} (American)", value=100, step=1, key=f"parlay_leg_{i}_odds"))
                leg_fair = st.text_input(f"Fair Odds {i+1} (optional)", value="", key=f"parlay_leg_{i}_fair")
                leg_prob = st.text_input(f"True Prob {i+1} (optional)", value="", key=f"parlay_leg_{i}_prob")

            parlay_legs_input.append({
                "sport": canonicalize_value("sport", leg_sport),
                "market": canonicalize_value("market", leg_market),
                "selection": leg_selection.strip(),
                "team": canonicalize_team(
                    canonicalize_value("sport", leg_sport),
                    leg_team_override if str(leg_team_override).strip() else (leg_team if str(leg_team).strip() else None),
                ),
                "opponent": canonicalize_team(
                    canonicalize_value("sport", leg_sport),
                    leg_opp_override if str(leg_opp_override).strip() else (leg_opponent if str(leg_opponent).strip() else None),
                ),
                "book": canonicalize_value("book", parlay_book),
                "odds_american": leg_odds,
                "fair_odds_american": leg_fair.strip(),
                "true_prob": leg_prob.strip(),
            })

    compute_parlay = st.button("🧮 Compute Parlay Recommendation", key="compute_parlay_reco")
    if compute_parlay:
        try:
            validate_devig_details(parlay_devig_method, parlay_devig_details)
            leg_probs: List[float] = []
            parlay_dec = 1.0
            for i, leg in enumerate(parlay_legs_input, start=1):
                o = float(leg["odds_american"])
                if o == 0:
                    raise ValueError(f"Leg {i}: odds cannot be 0.")
                parlay_dec *= american_to_decimal(o)

                p_val: Optional[float] = None
                if str(leg["true_prob"]).strip():
                    p_val = float(str(leg["true_prob"]).strip())
                elif str(leg["fair_odds_american"]).strip():
                    p_val = fair_prob_from_fair_american(float(str(leg["fair_odds_american"]).strip()))
                if p_val is None:
                    raise ValueError(f"Leg {i}: provide True Prob or Fair Odds.")
                if p_val <= 0 or p_val >= 1:
                    raise ValueError(f"Leg {i}: true probability must be between 0 and 1.")
                leg_probs.append(p_val)

            parlay_true_prob = 1.0
            for p in leg_probs:
                parlay_true_prob *= p

            boosted_parlay_dec = 1.0 + (parlay_dec - 1.0) * (1.0 + float(parlay_boost_pct) / 100.0)
            boosted_parlay_american = decimal_to_american(boosted_parlay_dec)
            unboosted_parlay_american = decimal_to_american(parlay_dec)

            parlay_rec = ledger.recommend_stake(
                odds_american=boosted_parlay_american,
                true_prob=parlay_true_prob,
                kelly_fraction=float(kelly_fraction),
                max_fraction_of_bankroll=float(max_frac_br),
                min_stake=float(min_stake),
                round_step=float(round_step),
            )

            # Parlay concentration: use worst concentration across included teams/matchups.
            unique_teams = sorted({str(l["team"]).strip() for l in parlay_legs_input if l.get("team")})
            unique_matchups = sorted({
                matchup_key(l.get("team"), l.get("opponent")) for l in parlay_legs_input if matchup_key(l.get("team"), l.get("opponent"))
            })
            open_after_parlay = m["open_exposure"] + float(parlay_rec["recommended_stake"])
            open_bets_after_parlay = int(len(open_only_df) + (1 if float(parlay_rec["recommended_stake"]) > 0 else 0))
            max_team_conc = 0.0
            max_matchup_conc = 0.0
            if open_after_parlay > 0 and not open_only_df.empty:
                for t in unique_teams:
                    t_open = float(
                        open_only_df[open_only_df["team"].apply(lambda x: ("" if pd.isna(x) else str(x).strip()) == t)]["stake"].sum()
                    )
                    max_team_conc = max(max_team_conc, (t_open + float(parlay_rec["recommended_stake"])) / open_after_parlay)
                for mu in unique_matchups:
                    mu_open = float(
                        open_only_df[
                            open_only_df.apply(lambda r: matchup_key(r.get("team"), r.get("opponent")) == mu, axis=1)
                        ]["stake"].sum()
                    )
                    max_matchup_conc = max(max_matchup_conc, (mu_open + float(parlay_rec["recommended_stake"])) / open_after_parlay)

            parlay_vs = value_score(
                rec=parlay_rec,
                open_exposure_after=open_after_parlay,
                bankroll=m["realized_bankroll"],
                team_concentration_after=max_team_conc,
                matchup_concentration_after=max_matchup_conc,
                open_bets_after=open_bets_after_parlay,
                concentration_min_open_bets=20,
            )

            # Check overlap against ACTIVE exposure only:
            # 1) OPEN straight bets
            # 2) legs inside other OPEN parlays
            if "is_parlay" not in df.columns:
                open_straights = df[df["status"] == "OPEN"].copy()
                open_parlays_existing = df.iloc[0:0].copy()
            else:
                open_straights = df[(df["status"] == "OPEN") & (df["is_parlay"] == False)].copy()
                open_parlays_existing = df[(df["status"] == "OPEN") & (df["is_parlay"] == True)].copy()
            for col in ["sport", "market", "selection", "team", "opponent", "status", "parlay_legs", "bet_id"]:
                if col not in open_straights.columns:
                    open_straights[col] = None
                if col not in open_parlays_existing.columns:
                    open_parlays_existing[col] = None

            duplicate_info: List[Dict[str, Any]] = []
            for i, leg in enumerate(parlay_legs_input, start=1):
                leg_s = normalize_token(leg.get("sport"))
                leg_m = normalize_token(leg.get("market"))
                leg_sel = normalize_token(leg.get("selection"))
                leg_t = normalize_token(leg.get("team"))
                leg_o = normalize_token(leg.get("opponent"))

                sub_open_straight = open_straights[
                    open_straights.apply(
                        lambda r: (
                            normalize_token(r.get("sport")) == leg_s
                            and normalize_token(r.get("market")) == leg_m
                            and normalize_token(r.get("selection")) == leg_sel
                            and ((not leg_t) or (normalize_token(r.get("team")) == leg_t))
                            and ((not leg_o) or (normalize_token(r.get("opponent")) == leg_o))
                        ),
                        axis=1,
                    )
                ].copy()

                open_straight_matches = int(len(sub_open_straight))

                open_parlay_leg_matches = 0
                open_parlay_ids: List[str] = []
                for _, pr in open_parlays_existing.iterrows():
                    legs_raw = pr.get("parlay_legs")
                    legs_list: List[Dict[str, Any]] = []
                    if isinstance(legs_raw, list):
                        legs_list = [x for x in legs_raw if isinstance(x, dict)]
                    elif isinstance(legs_raw, str) and legs_raw.strip():
                        try:
                            parsed = json.loads(legs_raw)
                            if isinstance(parsed, list):
                                legs_list = [x for x in parsed if isinstance(x, dict)]
                        except Exception:
                            legs_list = []
                    for ol in legs_list:
                        ol_s = normalize_token(ol.get("sport"))
                        ol_m = normalize_token(ol.get("market"))
                        ol_sel = normalize_token(ol.get("selection"))
                        ol_t = normalize_token(ol.get("team"))
                        ol_o = normalize_token(ol.get("opponent"))
                        if (
                            ol_s == leg_s
                            and ol_m == leg_m
                            and ol_sel == leg_sel
                            and ((not leg_t) or (ol_t == leg_t))
                            and ((not leg_o) or (ol_o == leg_o))
                        ):
                            open_parlay_leg_matches += 1
                            pid = "" if pd.isna(pr.get("bet_id")) else str(pr.get("bet_id")).strip()
                            if pid and pid not in open_parlay_ids:
                                open_parlay_ids.append(pid)

                duplicate_info.append({
                    "leg": i,
                    "selection": leg.get("selection"),
                    "market": leg.get("market"),
                    "team": leg.get("team"),
                    "opponent": leg.get("opponent"),
                    "open_straight_matches": open_straight_matches,
                    "open_parlay_leg_matches": int(open_parlay_leg_matches),
                    "open_parlay_bets": ", ".join(open_parlay_ids),
                })

            st.session_state["parlay_calc"] = {
                "legs": parlay_legs_input,
                "rec": parlay_rec,
                "value": parlay_vs,
                "true_prob": parlay_true_prob,
                "parlay_dec": parlay_dec,
                "boosted_parlay_dec": boosted_parlay_dec,
                "unboosted_parlay_american": unboosted_parlay_american,
                "boosted_parlay_american": boosted_parlay_american,
                "boost_pct": float(parlay_boost_pct),
                "book": canonicalize_value("book", parlay_book),
                "devig_method": canonicalize_devig_method(parlay_devig_method),
                "devig_details": parlay_devig_details.strip() if parlay_devig_details.strip() else None,
                "leg_count": parlay_leg_count,
                "duplicate_info": duplicate_info,
            }
        except Exception as e:
            st.error(f"Could not compute parlay recommendation: {e}")

    parlay_calc = st.session_state.get("parlay_calc")
    if parlay_calc:
        pc = parlay_calc
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Parlay Odds (No Boost)", f"{pc['parlay_dec']:.3f}x")
        c2.metric("Parlay Odds (Boosted)", f"{pc['boosted_parlay_dec']:.3f}x")
        c3.metric("Parlay True Prob", f"{pc['true_prob']*100:.2f}%")
        c4.metric("Recommended Stake", f"${pc['rec']['recommended_stake']:.2f}")
        v1, v2, v3 = st.columns(3)
        v1.metric("Parlay Value Score", f"{pc['value']['score']:.0f}/100")
        v2.metric("Grade", pc["value"]["grade"])
        v3.metric("Verdict", pc["value"]["verdict"])
        st.caption(
            f"Boosted American odds: {pc['boosted_parlay_american']:+.0f} | "
            f"Raw ${pc['rec']['raw_stake_before_cap']:.2f} -> cap ${pc['rec']['cap_amount']:.2f} -> rec ${pc['rec']['recommended_stake']:.2f}"
        )
        with st.expander("Why this parlay score?"):
            st.write(f"Base: {pc['value']['base']:.0f}")
            for factor in pc["value"]["factors"]:
                st.write(f"- {factor['label']}: {factor['delta']:+.1f}")

        dup_df = pd.DataFrame(pc.get("duplicate_info", []))
        if not dup_df.empty:
            for col in ["open_straight_matches", "open_parlay_leg_matches", "open_parlay_bets"]:
                if col not in dup_df.columns:
                    dup_df[col] = 0 if col != "open_parlay_bets" else ""
            total_open_straight_dups = int(dup_df["open_straight_matches"].sum())
            total_open_parlay_leg_dups = int(dup_df["open_parlay_leg_matches"].sum()) if "open_parlay_leg_matches" in dup_df.columns else 0
            if total_open_straight_dups > 0 or total_open_parlay_leg_dups > 0:
                st.warning(
                    f"Active overlap alert: {total_open_straight_dups} open straight overlap(s) and "
                    f"{total_open_parlay_leg_dups} overlapping leg(s) in other open parlays."
                )
            st.caption("Active overlap by leg (open straights + other open parlays)")
            st.dataframe(
                dup_df[["leg", "team", "opponent", "market", "selection", "open_straight_matches", "open_parlay_leg_matches", "open_parlay_bets"]],
                width="stretch",
                hide_index=True,
            )

        ps1, ps2, ps3 = st.columns(3)
        parlay_stake_mode = ps1.radio("Parlay Stake Mode", ["Use recommended stake", "Manual stake"], horizontal=True, key="parlay_stake_mode")
        parlay_manual_stake = ps2.number_input("Parlay Manual Stake ($)", value=1.0, min_value=0.0, step=0.25, key="parlay_manual_stake")
        parlay_confirm_add = ps3.checkbox("Confirm add parlay", value=False, key="parlay_confirm_add")

        if st.button("➕ Add OPEN Parlay Bet", disabled=not parlay_confirm_add, key="add_parlay_bet_btn"):
            try:
                if parlay_stake_mode == "Manual stake":
                    parlay_stake = float(parlay_manual_stake)
                    if parlay_stake <= 0:
                        raise ValueError("Parlay manual stake must be > 0.")
                    parlay_stake_source = "Manual"
                else:
                    parlay_stake = float(pc["rec"]["recommended_stake"])
                    parlay_stake_source = "Recommended"

                unique_sports = sorted({str(l.get("sport", "")).strip() for l in pc["legs"] if str(l.get("sport", "")).strip()})
                parlay_sport = unique_sports[0] if len(unique_sports) == 1 else "Multi-Sport"
                parlay_selection = parlay_label.strip() if parlay_label.strip() else f"{pc['leg_count']}-Leg Parlay"

                parlay_bet_id = ledger.add_bet(
                    sport=parlay_sport,
                    team=None,
                    opponent=None,
                    market="Parlay",
                    market_type="Other",
                    selection=parlay_selection,
                    book=pc["book"],
                    devig_method=pc["devig_method"],
                    devig_details=pc["devig_details"],
                    recommended_stake_snapshot=float(pc["rec"]["recommended_stake"]),
                    stake_source=parlay_stake_source,
                    odds_american=float(pc["boosted_parlay_american"]),
                    stake=float(parlay_stake),
                    fair_odds_american=float(pc["unboosted_parlay_american"]),
                    true_prob=float(pc["true_prob"]),
                    kelly_fraction_used=float(kelly_fraction),
                    kelly_units_from_tool=None,
                    notes=notes if notes.strip() else None,
                    is_parlay=True,
                    parlay_leg_count=int(pc["leg_count"]),
                    parlay_boost_pct=float(pc["boost_pct"]),
                    parlay_unboosted_odds_american=float(pc["unboosted_parlay_american"]),
                    parlay_boosted_odds_american=float(pc["boosted_parlay_american"]),
                    parlay_true_prob=float(pc["true_prob"]),
                    parlay_legs=pc["legs"],
                )
                ledger.save()
                st.success(f"Added OPEN parlay bet: {parlay_bet_id}")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add parlay bet: {e}")

    st.divider()
    st.markdown("### Live Bet Entry")
    st.caption("In-game straight bets with live-specific stake dampening for very large EV discrepancies.")

    lv1, lv2, lv3 = st.columns(3)
    with lv1:
        live_sport_pick = st.selectbox("Live Sport", options=sport_options, index=0, key="live_sport_pick")
        live_sport = canonicalize_value("sport", live_sport_pick)
        live_market_defaults = MARKET_PRESETS_BY_SPORT.get(live_sport, DEFAULT_MARKET_OPTIONS)
        live_market_options = history_options(ledger, "market", live_market_defaults + DEFAULT_MARKET_OPTIONS)
        live_market_pick = st.selectbox("Live Market", options=live_market_options, index=0, key="live_market_pick")
        live_market = canonicalize_value("market", live_market_pick)
        live_market_type_default = infer_market_type(live_market)
        live_market_type = st.selectbox(
            "Live Market Type",
            options=["Game", "Team", "Player", "Period", "Other"],
            index=["Game", "Team", "Player", "Period", "Other"].index(
                live_market_type_default if live_market_type_default in ["Game", "Team", "Player", "Period", "Other"] else "Other"
            ),
            key="live_market_type",
        )
        live_selection = st.text_input("Live Selection", value="", key="live_selection")

        live_team_list = TEAM_OPTIONS_BY_SPORT.get(live_sport, [])
        if live_team_list:
            live_team_pick = st.selectbox("Live Team (optional)", options=[""] + live_team_list, index=0, key="live_team_pick")
            live_opponent_pick = st.selectbox("Live Opponent (optional)", options=[""] + live_team_list, index=0, key="live_opp_pick")
        else:
            live_team_pick = st.text_input("Live Team (optional)", value="", key="live_team_pick_txt")
            live_opponent_pick = st.text_input("Live Opponent (optional)", value="", key="live_opp_pick_txt")
        live_team = canonicalize_team(live_sport, live_team_pick if str(live_team_pick).strip() else None)
        live_opponent = canonicalize_team(live_sport, live_opponent_pick if str(live_opponent_pick).strip() else None)

    with lv2:
        live_book_pick = st.selectbox("Live Book", options=book_options, index=0, key="live_book_pick")
        live_book = canonicalize_value("book", live_book_pick)
        live_odds = float(st.number_input("Live Odds (American)", value=-110, step=1, key="live_odds"))
        live_fair = st.text_input("Live Fair Odds (optional)", value="", key="live_fair")
        live_prob = st.text_input("Live True Prob (0-1, optional)", value="", key="live_prob")
        live_kelly_units = st.text_input("Live EVSharps 1/4-Kelly units (optional)", value="", key="live_kelly_units")
        live_boost_pct_input = st.text_input("Live Boost % (optional)", value="", key="live_boost_pct")
        live_notes = st.text_input("Live Notes (optional)", value="", placeholder="e.g., 7:40 Q3", key="live_notes")

    with lv3:
        live_devig_method = st.selectbox("Live Devig Method", options=DEVIG_METHOD_OPTIONS, index=0, key="live_devig_method")
        live_devig_details = st.text_input(
            "Live Devig Details (required for Single/Split)" if live_devig_method in {"Single Book (100%)", "Split Weights"} else "Live Devig Details (optional)",
            value="",
            key="live_devig_details",
        )
        live_max_frac = st.selectbox(
            "Live Max % BR Cap",
            options=[0.005, 0.01, 0.015, 0.02],
            index=2,
            format_func=lambda x: f"{x*100:.2f}%",
            key="live_max_frac",
        )
        st.caption("Live sizing applies an additional dampener when EV is extreme (especially > +65%).")

    compute_live = st.button("🧮 Compute Live Recommendation", key="compute_live_reco")
    if compute_live:
        try:
            live_kelly_val = float(live_kelly_units) if live_kelly_units.strip() else None
            live_fair_val = float(live_fair) if live_fair.strip() else None
            live_prob_val = float(live_prob) if live_prob.strip() else None
            live_boost_pct_val = float(live_boost_pct_input) if live_boost_pct_input.strip() else None
            if live_boost_pct_val is not None and live_boost_pct_val < 0:
                raise ValueError("Live Boost % cannot be negative.")
            if live_odds == 0:
                raise ValueError("Live odds cannot be 0.")
            if not any(x is not None for x in [live_kelly_val, live_fair_val, live_prob_val]):
                raise ValueError("Provide Live EVSharps units, Live Fair Odds, or Live True Prob.")
            validate_devig_details(live_devig_method, live_devig_details)

            live_rec = recommend_live_stake(
                ledger=ledger,
                odds_american=live_odds,
                fair_odds_american=live_fair_val,
                true_prob=live_prob_val,
                kelly_units_from_tool=live_kelly_val,
                kelly_fraction=float(kelly_fraction),
                max_fraction_of_bankroll=float(max_frac_br),
                live_max_fraction_of_bankroll=float(live_max_frac),
                min_stake=float(min_stake),
                round_step=float(round_step),
            )

            live_unboosted_for_store: Optional[float] = None
            if live_boost_pct_val is not None and live_boost_pct_val > 0:
                try:
                    live_unboosted_for_store = float(unboosted_american_from_boosted(live_odds, live_boost_pct_val))
                except Exception:
                    live_unboosted_for_store = None

            live_open_after = m["open_exposure"] + float(live_rec["recommended_stake"])
            live_open_bets_after = int(len(open_only_df) + (1 if float(live_rec["recommended_stake"]) > 0 else 0))
            live_team_open_now = 0.0
            live_matchup_open_now = 0.0
            live_target_matchup = matchup_key(live_team, live_opponent)
            if not open_only_df.empty:
                if live_team:
                    live_team_open_now = float(
                        open_only_df[
                            open_only_df["team"].apply(lambda x: ("" if pd.isna(x) else str(x).strip()) == live_team)
                        ]["stake"].sum()
                    )
                if live_target_matchup:
                    live_matchup_open_now = float(
                        open_only_df[
                            open_only_df.apply(lambda r: matchup_key(r.get("team"), r.get("opponent")) == live_target_matchup, axis=1)
                        ]["stake"].sum()
                    )

            live_team_conc_after = ((live_team_open_now + float(live_rec["recommended_stake"])) / live_open_after) if (live_open_after > 0 and live_team) else 0.0
            live_matchup_conc_after = ((live_matchup_open_now + float(live_rec["recommended_stake"])) / live_open_after) if (live_open_after > 0 and live_target_matchup) else 0.0
            live_vs = value_score(
                rec=live_rec,
                open_exposure_after=live_open_after,
                bankroll=m["realized_bankroll"],
                team_concentration_after=live_team_conc_after,
                matchup_concentration_after=live_matchup_conc_after,
                open_bets_after=live_open_bets_after,
                concentration_min_open_bets=20,
            )

            st.session_state["live_calc"] = {
                "sport": live_sport,
                "team": live_team,
                "opponent": live_opponent,
                "market": live_market,
                "market_type": live_market_type,
                "selection": live_selection.strip(),
                "book": live_book,
                "odds_american": float(live_odds),
                "devig_method": canonicalize_devig_method(live_devig_method),
                "devig_details": live_devig_details.strip() if live_devig_details.strip() else None,
                "fair_odds_american": live_fair_val,
                "true_prob": live_prob_val,
                "kelly_units": live_kelly_val,
                "boost_pct": live_boost_pct_val,
                "unboosted_odds_american": live_unboosted_for_store,
                "notes": live_notes.strip(),
                "rec": live_rec,
                "value": live_vs,
            }
        except Exception as e:
            st.error(f"Could not compute live recommendation: {e}")

    live_calc = st.session_state.get("live_calc")
    if live_calc:
        lr = live_calc["rec"]
        lvs = live_calc["value"]
        lm1, lm2, lm3, lm4 = st.columns(4)
        lm1.metric("Live Recommended Stake", f"${lr['recommended_stake']:.2f}")
        lm2.metric("EV/Dollar", "N/A" if lr.get("ev_per_dollar") is None else f"{float(lr['ev_per_dollar'])*100:.1f}%")
        lm3.metric("Live Dampener", f"{float(lr['live_ev_damp_mult']):.2f}x")
        lm4.metric("Live Value Score", f"{lvs['score']:.0f}/100")
        st.caption(
            f"Live sizing path: raw ${lr['raw_stake_before_cap']:.2f} -> damped ${lr['raw_stake_live_damp']:.2f} "
            f"-> live cap ${lr['effective_live_cap']:.2f} -> rec ${lr['recommended_stake']:.2f}"
        )
        st.caption(f"Dampener logic: {lr['live_ev_damp_reason']}")

        ls1, ls2, ls3 = st.columns(3)
        live_stake_mode = ls1.radio("Live Stake Mode", ["Use recommended stake", "Manual stake"], horizontal=True, key="live_stake_mode")
        live_manual_stake = ls2.number_input("Live Manual Stake ($)", value=1.0, min_value=0.0, step=0.25, key="live_manual_stake")
        live_confirm_add = ls3.checkbox("Confirm add live bet", value=False, key="live_confirm_add")

        if st.button("➕ Add OPEN Live Bet", disabled=not live_confirm_add, key="add_live_bet_btn"):
            try:
                if live_stake_mode == "Manual stake":
                    live_stake = float(live_manual_stake)
                    if live_stake <= 0:
                        raise ValueError("Live manual stake must be > 0.")
                    live_stake_source = "Live Manual"
                else:
                    live_stake = float(lr["recommended_stake"])
                    live_stake_source = "Live Recommended"

                live_notes_tagged = f"[LIVE] {live_calc['notes']}".strip()
                live_bet_id = ledger.add_bet(
                    sport=live_calc["sport"],
                    team=live_calc["team"],
                    opponent=live_calc["opponent"],
                    market=live_calc["market"],
                    market_type=live_calc["market_type"],
                    selection=live_calc["selection"],
                    book=live_calc["book"],
                    devig_method=live_calc["devig_method"],
                    devig_details=live_calc["devig_details"],
                    recommended_stake_snapshot=float(lr["recommended_stake"]),
                    stake_source=live_stake_source,
                    is_live=True,
                    odds_american=float(live_calc["odds_american"]),
                    stake=float(live_stake),
                    fair_odds_american=live_calc["fair_odds_american"],
                    true_prob=live_calc["true_prob"],
                    kelly_fraction_used=float(kelly_fraction) if live_calc["kelly_units"] is None else None,
                    kelly_units_from_tool=live_calc["kelly_units"],
                    boost_pct=live_calc["boost_pct"],
                    unboosted_odds_american=live_calc["unboosted_odds_american"],
                    notes=live_notes_tagged,
                )
                ledger.save()
                st.success(f"Added OPEN live bet: {live_bet_id}")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to add live bet: {e}")

# -----------------------------
# TAB 3: Edit Bets
# -----------------------------
with tab3:
    st.subheader("Edit Logged Bet")
    if df.empty:
        st.info("No bets found.")
    else:
        edit_df = df.sort_values("placed_at_dt", ascending=False, na_position="last").copy()
        edit_df["label"] = edit_df.apply(
            lambda r: f"{r['bet_id']} | {r['status']} | {r['sport']} | {r['selection']} | ${r['stake']:.2f}",
            axis=1
        )
        id_to_label = dict(zip(edit_df["bet_id"], edit_df["label"]))
        selected_bet_id = st.selectbox(
            "Select bet",
            options=edit_df["bet_id"].tolist(),
            format_func=lambda x: id_to_label.get(x, x)
        )
        selected = next((b for b in ledger.bets if b.bet_id == selected_bet_id), None)

        if selected is not None:
            with st.form(f"edit_form_{selected_bet_id}"):
                e1, e2, e3 = st.columns(3)
                with e1:
                    sport_e = st.text_input("Sport", value=selected.sport)
                    team_e = st.text_input("Team (optional)", value="" if selected.team is None else selected.team)
                    opponent_e = st.text_input("Opponent (optional)", value="" if selected.opponent is None else selected.opponent)
                    market_e = st.text_input("Market", value=selected.market)
                    market_type_e = st.selectbox(
                        "Market Type",
                        options=["Game", "Team", "Player", "Period", "Other"],
                        index=["Game", "Team", "Player", "Period", "Other"].index(
                            selected.market_type if selected.market_type in ["Game", "Team", "Player", "Period", "Other"] else "Game"
                        )
                    )
                    selection_e = st.text_input("Selection", value=selected.selection)
                    book_options_edit = ["DraftKings", "FanDuel", "BetMGM", "Caesars", "Fanatics", "BetRivers", "theScore", "Pinnacle"]
                    selected_book_norm = canonicalize_value("book", selected.book)
                    if selected_book_norm not in book_options_edit:
                        book_options_edit = [selected_book_norm] + book_options_edit
                    book_e = st.selectbox(
                        "Book",
                        options=book_options_edit,
                        index=book_options_edit.index(selected_book_norm) if selected_book_norm in book_options_edit else 0,
                    )
                with e2:
                    devig_method_e = st.selectbox(
                        "Devig Method",
                        options=DEVIG_METHOD_OPTIONS,
                        index=DEVIG_METHOD_OPTIONS.index(
                            selected.devig_method if selected.devig_method in DEVIG_METHOD_OPTIONS else "Market Avg"
                        )
                    )
                    devig_details_e = st.text_input(
                        "Devig Details (required for Single/Split)" if devig_method_e in {"Single Book (100%)", "Split Weights"} else "Devig Details (optional)",
                        value="" if selected.devig_details is None else selected.devig_details
                    )
                    odds_e = st.text_input(
                        "Odds (American)", value="" if selected.odds_american is None else str(selected.odds_american)
                    )
                    boost_e = st.text_input(
                        "Boost % (optional)", value="" if selected.boost_pct is None else str(selected.boost_pct)
                    )
                    unboosted_e = st.text_input(
                        "Non-Boosted Odds (optional)", value="" if selected.unboosted_odds_american is None else str(selected.unboosted_odds_american)
                    )
                    stake_e = st.text_input("Stake ($)", value=str(selected.stake))
                    fair_e = st.text_input(
                        "Fair odds (optional)",
                        value="" if selected.fair_odds_american is None else str(selected.fair_odds_american)
                    )
                    true_p_e = st.text_input(
                        "True prob (optional)",
                        value="" if selected.true_prob is None else str(selected.true_prob)
                    )
                with e3:
                    status_e = st.selectbox(
                        "Status",
                        options=["OPEN", "WON", "LOST", "VOID"],
                        index=["OPEN", "WON", "LOST", "VOID"].index(
                            selected.status if selected.status in ["OPEN", "WON", "LOST", "VOID"] else "OPEN"
                        ),
                    )
                    ev_e = st.text_input("EV % (optional)", value="" if selected.ev_pct is None else str(selected.ev_pct))
                    kf_e = st.text_input(
                        "Kelly fraction used (optional)",
                        value="" if selected.kelly_fraction_used is None else str(selected.kelly_fraction_used)
                    )
                    ku_e = st.text_input(
                        "EVSharps 1/4-Kelly units (optional)",
                        value="" if selected.kelly_units_from_tool is None else str(selected.kelly_units_from_tool)
                    )
                    close_e = st.text_input(
                        "Closing odds (optional)",
                        value="" if selected.closing_odds_american is None else str(selected.closing_odds_american)
                    )

                notes_e = st.text_input("Notes (optional)", value="" if selected.notes is None else selected.notes)
                save_edit = st.form_submit_button("💾 Save Edits")

            if save_edit:
                try:
                    prior_status = str(selected.status).upper().strip()
                    closing_val = float(close_e) if close_e.strip() else None
                    updates = {
                        "sport": sport_e,
                        "team": team_e,
                        "opponent": opponent_e,
                        "market": market_e,
                        "market_type": market_type_e,
                        "selection": selection_e,
                        "book": book_e,
                        "devig_method": devig_method_e,
                        "devig_details": devig_details_e,
                        "odds_american": float(odds_e) if odds_e.strip() else None,
                        "boost_pct": float(boost_e) if boost_e.strip() else None,
                        "unboosted_odds_american": float(unboosted_e) if unboosted_e.strip() else None,
                        "stake": float(stake_e),
                        "fair_odds_american": float(fair_e) if fair_e.strip() else None,
                        "true_prob": float(true_p_e) if true_p_e.strip() else None,
                        "ev_pct": float(ev_e) if ev_e.strip() else None,
                        "kelly_fraction_used": float(kf_e) if kf_e.strip() else None,
                        "kelly_units_from_tool": float(ku_e) if ku_e.strip() else None,
                        "closing_odds_american": closing_val,
                        "notes": notes_e,
                    }
                    ledger.update_bet(selected_bet_id, updates)
                    if status_e != prior_status:
                        ledger.set_bet_status(
                            selected_bet_id,
                            status_e,
                            closing_odds_american=closing_val,
                        )
                    ledger.save()
                    st.success(f"Updated bet {selected_bet_id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to update bet: {e}")

# -----------------------------
# TAB 4: Grade Bets (3 buttons)
# -----------------------------
with tab4:
    st.subheader("Grade OPEN Bets")
    open_df = df[df["status"] == "OPEN"].copy().sort_values("placed_at_dt", ascending=False, na_position="last")

    if open_df.empty:
        st.info("No OPEN bets to grade.")
    else:
        st.caption("Inline quick edits: double-click a cell like Team/Opponent/Notes, then save.")
        editor_cols = [
            "bet_id","bet_type","sport","team","opponent","market","market_type","selection","book","devig_method","devig_details",
            "odds_american","stake","units","placed_at","notes"
        ]
        editable_cols = {"team", "opponent", "devig_method", "devig_details", "notes"}
        open_editor_df = open_df[editor_cols].copy()
        open_editor_df[["team", "opponent", "devig_method", "devig_details", "notes"]] = (
            open_editor_df[["team", "opponent", "devig_method", "devig_details", "notes"]].fillna("")
        )

        edited_open_df = st.data_editor(
            open_editor_df,
            width="stretch",
            hide_index=True,
            column_config={
                "devig_method": st.column_config.SelectboxColumn(
                    "Devig Method",
                    options=DEVIG_METHOD_OPTIONS,
                    required=False,
                ),
            },
            disabled=[c for c in editor_cols if c not in editable_cols],
            key="grade_open_inline_editor",
        )

        if st.button("💾 Save Inline Updates"):
            try:
                updates_made = 0
                for idx in range(len(open_editor_df)):
                    bet_id_row = str(open_editor_df.iloc[idx]["bet_id"])
                    updates: Dict[str, Any] = {}
                    for c in editable_cols:
                        old_v = open_editor_df.iloc[idx][c]
                        new_v = edited_open_df.iloc[idx][c]
                        old_s = "" if pd.isna(old_v) else str(old_v).strip()
                        new_s = "" if pd.isna(new_v) else str(new_v).strip()
                        if old_s != new_s:
                            if c in {"team", "opponent", "devig_details", "notes"}:
                                updates[c] = new_s if new_s else None
                            elif c == "devig_method":
                                updates[c] = new_s if new_s else "Market Avg"
                    if updates:
                        ledger.update_bet(bet_id_row, updates)
                        updates_made += 1
                if updates_made > 0:
                    ledger.save()
                    st.success(f"Saved inline updates for {updates_made} bet(s).")
                    st.rerun()
                else:
                    st.info("No inline changes detected.")
            except Exception as e:
                st.error(f"Failed to save inline updates: {e}")

        st.divider()
        st.markdown("### Select Bet to Grade")
        gf1, gf2, gf3, gf4 = st.columns([1.2, 1.1, 1.1, 1.2])
        with gf1:
            grade_team_options = sorted([x for x in open_df["team"].dropna().astype(str).unique().tolist() if x.strip()])
            grade_team_filter = st.selectbox("Team Filter", options=["All Teams"] + grade_team_options, index=0, key="grade_team_filter")
        with gf2:
            grade_sport_options = sorted([x for x in open_df["sport"].dropna().astype(str).unique().tolist() if x.strip()])
            grade_sport_filter = st.selectbox("Sport Filter", options=["All Sports"] + grade_sport_options, index=0, key="grade_sport_filter")
        with gf3:
            grade_book_options = sorted([x for x in open_df["book"].dropna().astype(str).unique().tolist() if x.strip()])
            grade_book_filter = st.selectbox("Book Filter", options=["All Books"] + grade_book_options, index=0, key="grade_book_filter")
        with gf4:
            grade_order = st.selectbox("Order", options=["Newest Placed", "Team A-Z"], index=0, key="grade_order")

        open_grade_df = open_df.copy()
        if grade_team_filter != "All Teams":
            open_grade_df = open_grade_df[open_grade_df["team"] == grade_team_filter]
        if grade_sport_filter != "All Sports":
            open_grade_df = open_grade_df[open_grade_df["sport"] == grade_sport_filter]
        if grade_book_filter != "All Books":
            open_grade_df = open_grade_df[open_grade_df["book"] == grade_book_filter]

        if grade_order == "Team A-Z":
            open_grade_df = open_grade_df.sort_values(["team", "placed_at_dt"], ascending=[True, False], na_position="last")
        else:
            open_grade_df = open_grade_df.sort_values("placed_at_dt", ascending=False, na_position="last")

        st.caption(f"Filtered OPEN bets: {len(open_grade_df)}")
        if open_grade_df.empty:
            st.info("No OPEN bets match the selected filters.")
        else:
            open_grade_df["grade_label"] = open_grade_df.apply(
                lambda r: (
                    f"{r['placed_at']} | {r['bet_type']} | {r['sport']} | {'' if pd.isna(r['team']) else r['team']} vs "
                    f"{'' if pd.isna(r['opponent']) else r['opponent']} | {r['market']} | {r['selection']} | "
                    f"{r['book']} | {r['odds_american']} | ${float(r['stake']):.2f} | id:{r['bet_id']}"
                ),
                axis=1
            )
            bet_options = open_grade_df["bet_id"].tolist()
            bet_label_map = dict(zip(open_grade_df["bet_id"], open_grade_df["grade_label"]))
            bet_id = st.selectbox(
                "Select bet to grade",
                bet_options,
                format_func=lambda x: bet_label_map.get(x, x)
            )
            closing = st.text_input("Closing odds (optional)", value="")
            existing_open_closing = open_grade_df.set_index("bet_id")["closing_odds_american"].to_dict()

            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("✅ WIN"):
                    try:
                        existing_val = existing_open_closing.get(bet_id)
                        closing_val = float(closing) if closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.grade_bet(bet_id, "W", closing_odds_american=closing_val)
                        ledger.save()
                        st.success("Graded WIN")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with col2:
                if st.button("❌ LOSS"):
                    try:
                        existing_val = existing_open_closing.get(bet_id)
                        closing_val = float(closing) if closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.grade_bet(bet_id, "L", closing_odds_american=closing_val)
                        ledger.save()
                        st.success("Graded LOSS")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with col3:
                if st.button("🟡 VOID"):
                    try:
                        existing_val = existing_open_closing.get(bet_id)
                        closing_val = float(closing) if closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.grade_bet(bet_id, "VOID", closing_odds_american=closing_val)
                        ledger.save()
                        st.success("Graded VOID")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

        st.divider()
        st.subheader("Correct Settled Bet Grade")
        st.caption("Use this when a settled result was graded incorrectly (e.g., WON should be LOSS).")
        settled_for_fix = df[df["status"].isin(["WON", "LOST", "VOID"])].copy().sort_values("settled_at_dt", ascending=False, na_position="last")
        if settled_for_fix.empty:
            st.info("No settled bets available to regrade.")
        else:
            settled_for_fix["fix_label"] = settled_for_fix.apply(
                lambda r: (
                    f"{r['settled_at']} | {r['status']} | {r['bet_type']} | {r['sport']} | "
                    f"{'' if pd.isna(r['team']) else r['team']} vs {'' if pd.isna(r['opponent']) else r['opponent']} | "
                    f"{r['market']} | {r['selection']} | {r['book']} | {r['odds_american']} | "
                    f"${float(r['stake']):.2f} | pnl ${float(r['pnl']):.2f} | id:{r['bet_id']}"
                ),
                axis=1
            )
            fix_options = settled_for_fix["bet_id"].tolist()
            fix_label_map = dict(zip(settled_for_fix["bet_id"], settled_for_fix["fix_label"]))
            fix_bet_id = st.selectbox(
                "Select settled bet to correct",
                fix_options,
                format_func=lambda x: fix_label_map.get(x, x),
                key="regrade_settled_select",
            )
            fix_closing = st.text_input("Corrected closing odds (optional)", value="", key="regrade_settled_closing")
            existing_settled_closing = settled_for_fix.set_index("bet_id")["closing_odds_american"].to_dict()
            r1, r2, r3 = st.columns(3)
            with r1:
                if st.button("↩️ Regrade to WIN"):
                    try:
                        existing_val = existing_settled_closing.get(fix_bet_id)
                        fix_closing_val = float(fix_closing) if fix_closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.regrade_settled_bet(fix_bet_id, "W", closing_odds_american=fix_closing_val)
                        ledger.save()
                        st.success("Regraded to WIN")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with r2:
                if st.button("↩️ Regrade to LOSS"):
                    try:
                        existing_val = existing_settled_closing.get(fix_bet_id)
                        fix_closing_val = float(fix_closing) if fix_closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.regrade_settled_bet(fix_bet_id, "L", closing_odds_american=fix_closing_val)
                        ledger.save()
                        st.success("Regraded to LOSS")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with r3:
                if st.button("↩️ Regrade to VOID"):
                    try:
                        existing_val = existing_settled_closing.get(fix_bet_id)
                        fix_closing_val = float(fix_closing) if fix_closing.strip() else (float(existing_val) if pd.notnull(existing_val) else None)
                        ledger.regrade_settled_bet(fix_bet_id, "VOID", closing_odds_american=fix_closing_val)
                        ledger.save()
                        st.success("Regraded to VOID")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

# -----------------------------
# TAB 5: Closing Odds
# -----------------------------
with tab5:
    st.subheader("Log Closing Odds / Boost Adjustments")
    st.caption("Condensed view for OPEN bets. For boosted straights, set Non-Boosted Odds so CLV is measured on true entry price.")

    closing_df = df[df["status"] == "OPEN"].copy().sort_values(["team", "placed_at_dt"], ascending=[True, False], na_position="last")
    if closing_df.empty:
        st.info("No OPEN bets found.")
    else:
        closing_df["placed_day"] = closing_df["placed_at_dt"].dt.strftime("%Y-%m-%d")
        closing_df["team_group"] = closing_df["team"].apply(
            lambda v: str(v).strip() if pd.notnull(v) and str(v).strip() else "Unassigned Team"
        )
        team_groups = sorted(closing_df["team_group"].dropna().unique().tolist())
        team_pick = st.selectbox("Team Group", options=["All Teams"] + team_groups, index=0, key="closing_team_pick")
        if team_pick != "All Teams":
            closing_df = closing_df[closing_df["team_group"] == team_pick].copy()

        editor_cols = [
            "bet_id", "placed_day", "sport", "team", "market", "selection", "book",
            "odds_american", "boost_pct", "unboosted_odds_american", "closing_odds_american"
        ]
        edit_view = closing_df[editor_cols].copy()
        for c in ["boost_pct", "unboosted_odds_american", "closing_odds_american"]:
            edit_view[c] = edit_view[c].apply(lambda v: "" if pd.isna(v) else str(v))

        edited_closing_df = st.data_editor(
            edit_view,
            width="stretch",
            height=380,
            hide_index=True,
            disabled=[c for c in editor_cols if c not in {"boost_pct", "unboosted_odds_american", "closing_odds_american"}],
            key=f"closing_odds_editor_{normalize_token(team_pick).replace(' ', '_') if team_pick else 'all'}",
        )

        if st.button("💾 Save Closing Odds Updates", key=f"save_closing_odds_updates_{normalize_token(team_pick).replace(' ', '_') if team_pick else 'all'}"):
            try:
                updates_made = 0
                for idx in range(len(edit_view)):
                    row_id = str(edit_view.iloc[idx]["bet_id"])
                    updates: Dict[str, Any] = {}
                    for c in ["boost_pct", "unboosted_odds_american", "closing_odds_american"]:
                        old_s = "" if pd.isna(edit_view.iloc[idx][c]) else str(edit_view.iloc[idx][c]).strip()
                        new_s = "" if pd.isna(edited_closing_df.iloc[idx][c]) else str(edited_closing_df.iloc[idx][c]).strip()
                        if old_s == new_s:
                            continue
                        if not new_s:
                            updates[c] = None
                        else:
                            updates[c] = float(new_s)

                    if "boost_pct" in updates and updates["boost_pct"] is not None and float(updates["boost_pct"]) < 0:
                        raise ValueError(f"Bet {row_id}: Boost % cannot be negative.")

                    if updates:
                        ledger.update_bet(row_id, updates)
                        updates_made += 1

                if updates_made > 0:
                    ledger.save()
                    st.success(f"Saved {updates_made} closing odds update(s).")
                    st.rerun()
                else:
                    st.info("No changes detected.")
            except Exception as e:
                st.error(f"Failed to save updates: {e}")

# -----------------------------
# TAB 6: Export / Raw
# -----------------------------
with tab6:
    st.subheader("Export / Backup")

    if not df.empty:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", data=csv, file_name="ev_ledger.csv", mime="text/csv")

    st.divider()
    st.subheader("Raw JSON")
    try:
        raw = load_ledger_payload()
        st.json(raw)
    except Exception as e:
        st.error(f"Could not read ledger payload: {e}")
