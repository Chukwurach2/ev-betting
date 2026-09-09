from __future__ import annotations
import numpy as np
import pandas as pd
from .odds import add_no_vig_probabilities, expected_value

def price_market(sim: pd.DataFrame, market: str, selection: str, team: str | None = None, line=None, home=None, away=None):
    sel = str(selection).strip().lower()
    market = str(market).strip().upper()

    if market == "FIRST_DRIVE_TD":
        if team == home:
            p_yes = sim["home_first_drive_td"].mean()
        elif team == away:
            p_yes = sim["away_first_drive_td"].mean()
        else:
            raise ValueError(f"Unknown team {team}")
        return p_yes if sel == "yes" else 1 - p_yes

    if market == "FIRST_DRIVE_SCORE":
        if team == home:
            p_yes = sim["home_first_drive_score"].mean()
        elif team == away:
            p_yes = sim["away_first_drive_score"].mean()
        else:
            raise ValueError(f"Unknown team {team}")
        return p_yes if sel == "yes" else 1 - p_yes

    if market == "Q1_ANY_TD":
        p_yes = sim["q1_any_td"].mean()
        return p_yes if sel == "yes" else 1 - p_yes

    if market == "Q1_BOTH_SCORE":
        p_yes = sim["q1_both_score"].mean()
        return p_yes if sel == "yes" else 1 - p_yes

    if market == "FIRST_5_MIN_SCORE":
        p_yes = sim["first_5_min_score"].mean()
        return p_yes if sel == "yes" else 1 - p_yes

    if market == "Q1_TOTAL":
        if pd.isna(line):
            raise ValueError("Q1_TOTAL requires a line")
        line = float(line)
        if sel == "over":
            return float((sim["q1_total"] > line).mean())
        if sel == "under":
            return float((sim["q1_total"] < line).mean())
        raise ValueError("Q1_TOTAL selection must be Over or Under")

    raise ValueError(f"Unsupported market {market}")

def evaluate_odds(
    sim_by_game: dict[str, pd.DataFrame],
    matchups: pd.DataFrame,
    odds: pd.DataFrame,
    min_american_odds: int = -150,
    min_edge: float = 0.03,
    min_ev: float = 0.04,
):
    x = add_no_vig_probabilities(odds)
    matchup_map = matchups.set_index("game_id").to_dict("index")
    probs = []
    errors = []
    for i, r in x.iterrows():
        try:
            m = matchup_map[r["game_id"]]
            sim = sim_by_game[r["game_id"]]
            p = price_market(
                sim,
                market=r["market"],
                selection=r["selection"],
                team=None if pd.isna(r.get("team")) else r.get("team"),
                line=r.get("line"),
                home=m["home_team"],
                away=m["away_team"],
            )
            probs.append(p)
            errors.append("")
        except Exception as e:
            probs.append(np.nan)
            errors.append(str(e))
    x["model_prob"] = probs
    x["pricing_error"] = errors
    x["edge"] = x["model_prob"] - x["fair_market_prob"]
    x["ev"] = [
        expected_value(p, o) if np.isfinite(p) else np.nan
        for p, o in zip(x["model_prob"], x["american_odds"])
    ]
    x["qualifies"] = (
        x["american_odds"].ge(min_american_odds)
        & x["edge"].ge(min_edge)
        & x["ev"].ge(min_ev)
        & x["model_prob"].notna()
    )
    x["edge_pct"] = 100 * x["edge"]
    x["ev_pct"] = 100 * x["ev"]
    return x.sort_values(["qualifies", "ev", "edge"], ascending=[False, False, False])
