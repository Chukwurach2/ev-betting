"""Reproducible chronological drive-model experiment; never auto-promotes."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import pathlib
import platform
import numpy as np
import pandas as pd
import sklearn
from sklearn.metrics import log_loss
from nfl_early.data import load_pbp, build_drive_table, parse_seasons, download_season
from nfl_early.features import build_pregame_states
from nfl_early.model import DrivePricingModel, NUMERIC_FEATURES, CATEGORICAL_FEATURES
from production_gate import ARTIFACT_FILES

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir', required=True)
    p.add_argument('--train', default='2016:2022')
    p.add_argument('--validate', default='2023:2025')
    p.add_argument('--out', required=True)
    p.add_argument('--download', action='store_true')
    p.add_argument('--version', default='nfl-edge-v2.5-drive-shadow')
    a = p.parse_args()
    tr, va = parse_seasons(a.train), parse_seasons(a.validate)
    if not tr or not va or max(tr) >= min(va):
        raise ValueError('Train seasons must strictly precede validation seasons')
    if a.download:
        for year in sorted(set(tr + va)):
            print('Downloading season', year, flush=True)
            download_season(year, a.data_dir)
    dr = build_drive_table(load_pbp(a.data_dir, sorted(set(tr + va))))
    feat, latest, defaults = build_pregame_states(dr)
    train, test = feat[feat.season.isin(tr)].copy(), feat[feat.season.isin(va)].copy()
    if len(train) < 1000 or len(test) < 1000:
        raise ValueError('Insufficient train or validation drives')
    prefix, _, prefix_defaults = build_pregame_states(dr[dr.season.isin(tr)].copy())
    keys = ['game_id', 'drive_id']
    columns = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    pd.testing.assert_frame_equal(train.sort_values(keys)[keys + columns].reset_index(drop=True),
                                  prefix.sort_values(keys)[keys + columns].reset_index(drop=True))
    assert defaults == prefix_defaults
    model = DrivePricingModel.fit(train, latest, defaults)
    probs = model.predict_outcome_proba(test)
    classes = sorted(probs.columns)
    y = test.outcome.where(test.outcome.isin(classes), 'OTHER')
    prior = train.outcome.value_counts().reindex(classes, fill_value=0).to_numpy() + 1
    prior = prior / prior.sum()
    baseline = np.tile(prior, (len(test), 1))
    actual = test.outcome.eq('TD').astype(float).to_numpy()
    out = pathlib.Path(a.out)
    model.save(out)
    metrics = {
        'version_name': a.version, 'production_validated': False, 'role': 'challenger',
        'training_cutoff': str(max(tr)) + ' regular season',
        'state_cutoff': str(max(va)) + ' regular season',
        'train_seasons': tr, 'validation_seasons': va,
        'train_drives': len(train), 'validation_drives': len(test),
        'multiclass_log_loss': float(log_loss(y, probs[classes], labels=classes)),
        'baseline_log_loss': float(log_loss(y, baseline, labels=classes)),
        'td_brier_score': float(np.mean((probs.TD.to_numpy() - actual) ** 2)),
        'baseline_td_brier_score': float(np.mean((prior[classes.index('TD')] - actual) ** 2)),
        'feature_availability_audit': True,
        'feature_audit_checks': ['future_truncation_invariance', 'fixed_cold_start_priors',
                                 'disjoint_chronological_split', 'exclude_unstamped_weather_and_closing_lines'],
        'feature_columns': columns,
        'artifact_hashes': {f: hashlib.sha256((out / f).read_bytes()).hexdigest() for f in ARTIFACT_FILES},
        'data_hashes': {str(s): hashlib.sha256((pathlib.Path(a.data_dir) / f'play_by_play_{s}.parquet').read_bytes()).hexdigest() for s in tr + va},
        'runtime': {'python': platform.python_version(), 'sklearn': sklearn.__version__},
        'limitations': ['No historical executable odds validation', 'Drive labels do not validate Q1 simulation',
                        'Simulation omits defensive/special-teams scores and exact extra points',
                        'No automatic production promotion'],
        'created_at': dt.datetime.now(dt.timezone.utc).isoformat()
    }
    (out / 'validation.json').write_text(json.dumps(metrics, indent=2, allow_nan=False))
    print(json.dumps(metrics, indent=2, allow_nan=False))

if __name__ == '__main__':
    main()
