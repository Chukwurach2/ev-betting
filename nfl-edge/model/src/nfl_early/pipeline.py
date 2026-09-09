from __future__ import annotations
from pathlib import Path
import pandas as pd
import yaml
from sklearn.metrics import log_loss, brier_score_loss

from .data import load_pbp, build_drive_table
from .features import build_pregame_states
from .model import DrivePricingModel, OUTCOMES
from .simulate import simulate_game
from .markets import evaluate_odds

def load_config(path: str | Path | None):
    if path is None:
        return {}
    return yaml.safe_load(Path(path).read_text()) or {}

def train_from_pbp(pbp: pd.DataFrame, model_dir, alpha=0.28):
    drives = build_drive_table(pbp)
    featured, latest, defaults = build_pregame_states(drives, alpha=alpha)
    model = DrivePricingModel.fit(featured, latest, defaults)
    model.save(model_dir)
    return model, featured

def score_files(
    model_dir,
    matchups_csv,
    odds_csv,
    output_csv,
    n_sims=50000,
    seed=42,
    min_american_odds=-150,
    min_edge=0.03,
    min_ev=0.04,
):
    model = DrivePricingModel.load(model_dir)
    matchups = pd.read_csv(matchups_csv)
    odds = pd.read_csv(odds_csv)

    # Preserve empty team/line values as NaN.
    for c in ["team", "line"]:
        if c not in odds:
            odds[c] = None

    sims = {}
    for j, r in matchups.iterrows():
        sims[r["game_id"]] = simulate_game(
            model, r.to_dict(), n_sims=n_sims, seed=seed + j
        )

    result = evaluate_odds(
        sims, matchups, odds,
        min_american_odds=min_american_odds,
        min_edge=min_edge,
        min_ev=min_ev,
    )
    result.to_csv(output_csv, index=False)
    return result
