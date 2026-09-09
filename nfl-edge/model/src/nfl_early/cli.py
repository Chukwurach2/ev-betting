from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
from .data import parse_seasons, download_season, load_pbp
from .pipeline import train_from_pbp, score_files
from .features import build_pregame_states
from .data import build_drive_table
from .model import DrivePricingModel

def cmd_download(a):
    seasons = parse_seasons(a.seasons)
    for s in seasons:
        p = download_season(s, a.data_dir)
        print(f"Downloaded {s}: {p}")

def cmd_train(a):
    seasons = parse_seasons(a.seasons)
    pbp = load_pbp(a.data_dir, seasons)
    model, featured = train_from_pbp(pbp, a.model_dir, alpha=a.alpha)
    print(f"Trained on {len(featured):,} Q1-starting drives")
    print(f"Saved model to {a.model_dir}")

def cmd_score(a):
    result = score_files(
        a.model_dir, a.matchups, a.odds, a.output,
        n_sims=a.sims, seed=a.seed,
        min_american_odds=a.min_odds,
        min_edge=a.min_edge,
        min_ev=a.min_ev,
    )
    cols = [
        "game_id", "bookmaker", "market", "team", "selection", "line",
        "american_odds", "model_prob", "fair_market_prob", "edge_pct", "ev_pct", "qualifies"
    ]
    print(result[cols].to_string(index=False))
    print(f"\nSaved: {a.output}")

def cmd_validate(a):
    train_seasons = parse_seasons(a.train_seasons)
    test_seasons = parse_seasons(a.test_seasons)
    all_seasons = sorted(set(train_seasons + test_seasons))
    pbp = load_pbp(a.data_dir, all_seasons)
    drives = build_drive_table(pbp)
    featured, latest, defaults = build_pregame_states(drives, alpha=a.alpha)

    train = featured[featured["season"].isin(train_seasons)].copy()
    test = featured[featured["season"].isin(test_seasons)].copy()

    # States passed to fit do not affect validation predictions.
    model = DrivePricingModel.fit(train, latest, defaults)
    p = model.predict_outcome_proba(test)
    classes = list(p.columns)
    y = test["outcome"].where(test["outcome"].isin(classes), "OTHER")
    # sklearn log_loss needs labels in same order.
    from sklearn.metrics import log_loss
    ll = log_loss(y, p[classes], labels=classes)

    td_prob = p["TD"].to_numpy()
    td_actual = test["outcome"].eq("TD").astype(float).to_numpy()
    brier = ((td_prob - td_actual) ** 2).mean()

    print(f"Train drives: {len(train):,}")
    print(f"Test drives:  {len(test):,}")
    print(f"Multiclass log loss: {ll:.5f}")
    print(f"TD Brier score:      {brier:.5f}")

def build_parser():
    p = argparse.ArgumentParser(prog="nfl-early")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download")
    d.add_argument("--seasons", required=True)
    d.add_argument("--data-dir", default="data/pbp")
    d.set_defaults(func=cmd_download)

    t = sub.add_parser("train")
    t.add_argument("--seasons", required=True)
    t.add_argument("--data-dir", default="data/pbp")
    t.add_argument("--model-dir", default="artifacts")
    t.add_argument("--alpha", type=float, default=0.28)
    t.set_defaults(func=cmd_train)

    s = sub.add_parser("score")
    s.add_argument("--model-dir", default="artifacts")
    s.add_argument("--matchups", required=True)
    s.add_argument("--odds", required=True)
    s.add_argument("--output", default="picks.csv")
    s.add_argument("--sims", type=int, default=50000)
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--min-odds", type=int, default=-150)
    s.add_argument("--min-edge", type=float, default=0.03)
    s.add_argument("--min-ev", type=float, default=0.04)
    s.set_defaults(func=cmd_score)

    v = sub.add_parser("validate")
    v.add_argument("--data-dir", default="data/pbp")
    v.add_argument("--train-seasons", required=True)
    v.add_argument("--test-seasons", required=True)
    v.add_argument("--alpha", type=float, default=0.28)
    v.set_defaults(func=cmd_validate)
    return p

def main():
    a = build_parser().parse_args()
    a.func(a)

if __name__ == "__main__":
    main()
