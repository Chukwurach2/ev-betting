"""Artifact integrity is necessary; drive validation alone cannot certify wagers."""
from __future__ import annotations
import hashlib
import json
import math
from pathlib import Path

ARTIFACT_FILES = ['outcome_model.joblib', 'duration_model.joblib', 'states.json', 'defaults.json']

def validate_artifact(model_dir):
    p = Path(model_dir)
    try:
        v = json.loads((p / 'validation.json').read_text())
        train, test = v['train_seasons'], v['validation_seasons']
        if not train or not test or max(train) >= min(test):
            raise ValueError('Training and validation must be chronological and disjoint')
        for name in ARTIFACT_FILES:
            actual = hashlib.sha256((p / name).read_bytes()).hexdigest()
            if actual != v['artifact_hashes'][name]:
                raise ValueError('Artifact hash mismatch: ' + name)
        for metric in ['multiclass_log_loss', 'td_brier_score', 'baseline_log_loss', 'baseline_td_brier_score']:
            if not math.isfinite(v[metric]) or v[metric] < 0:
                raise ValueError('Invalid validation metric: ' + metric)
        if not 0 <= v['td_brier_score'] <= 1 or v['validation_drives'] < 1000:
            raise ValueError('Insufficient or invalid validation evidence')
        if v.get('feature_availability_audit') is not True or not v.get('feature_audit_checks'):
            raise ValueError('Missing feature audit evidence')
        return {'ready': False, 'artifact_valid': True,
                'reason': 'Drive model is a shadow candidate; market-level validation and live acceptance are required',
                'validation': v}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {'ready': False, 'artifact_valid': False, 'reason': str(exc)}
