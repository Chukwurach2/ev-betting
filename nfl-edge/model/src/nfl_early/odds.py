from __future__ import annotations
import math
import pandas as pd

def american_to_decimal(odds: float) -> float:
    odds = float(odds)
    if odds == 0:
        raise ValueError("American odds cannot be 0")
    return 1.0 + (odds / 100.0 if odds > 0 else 100.0 / abs(odds))

def american_to_implied(odds: float) -> float:
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    raise ValueError("American odds cannot be 0")

def expected_value(model_prob: float, american_odds: float) -> float:
    return float(model_prob) * american_to_decimal(american_odds) - 1.0

def add_no_vig_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    De-vigs selections within the same book/game/market/team/line.
    Works for two-way or multi-way groups by normalizing raw implied probabilities.
    """
    out = df.copy()
    out["raw_implied_prob"] = out["american_odds"].map(american_to_implied)
    group_cols = ["game_id", "bookmaker", "market", "team", "line"]
    # Preserve NaN group keys.
    temp = out[group_cols].fillna("__NA__").astype(str)
    key = temp.agg("|".join, axis=1)
    denom = out["raw_implied_prob"].groupby(key).transform("sum")
    counts = out["raw_implied_prob"].groupby(key).transform("count")
    out["fair_market_prob"] = out["raw_implied_prob"]
    mask = counts >= 2
    out.loc[mask, "fair_market_prob"] = out.loc[mask, "raw_implied_prob"] / denom[mask]
    return out
