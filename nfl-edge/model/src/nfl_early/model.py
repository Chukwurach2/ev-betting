from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import log_loss

OUTCOMES = ["TD", "FG", "PUNT", "TO", "DOWNS", "OTHER"]

NUMERIC_FEATURES = [
    "is_home",
    "drive_index", "opening_drive",
    "off_fd_td_rate", "off_fd_score_rate", "off_q1_td_rate", "off_q1_score_rate",
    "off_q1_epa", "off_q1_success", "off_q1_duration", "off_q1_plays",
    "def_fd_td_rate", "def_fd_score_rate", "def_q1_td_rate", "def_q1_score_rate",
    "def_q1_epa", "def_q1_success", "def_q1_duration", "def_q1_plays",
]
# Exclude weather and closing lines lacking pregame observation timestamps.
CATEGORICAL_FEATURES = []

def _preprocessor():
    num = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
    ])
    cat = Pipeline([
        ("impute", SimpleImputer(strategy="most_frequent")),
        ("oh", OneHotEncoder(handle_unknown="ignore", min_frequency=3)),
    ])
    return ColumnTransformer([
        ("num", num, NUMERIC_FEATURES),
        ("cat", cat, CATEGORICAL_FEATURES),
    ])

@dataclass
class DrivePricingModel:
    outcome_model: Pipeline
    duration_model: Pipeline
    latest_states: dict
    defaults: dict

    @classmethod
    def fit(cls, train: pd.DataFrame, latest_states: dict, defaults: dict):
        x = train.copy()
        for c in CATEGORICAL_FEATURES:
            if c not in x:
                x[c] = "unknown"
        for c in NUMERIC_FEATURES:
            if c not in x:
                x[c] = np.nan

        y = x["outcome"].where(x["outcome"].isin(OUTCOMES), "OTHER")
        outcome = Pipeline([
            ("prep", _preprocessor()),
            ("clf", LogisticRegression(
                max_iter=2000,
                C=0.65,
                class_weight=None,
            )),
        ])
        outcome.fit(x[NUMERIC_FEATURES + CATEGORICAL_FEATURES], y)

        dur = x[pd.to_numeric(x["duration"], errors="coerce").between(10, 900)].copy()
        ydur = np.log(pd.to_numeric(dur["duration"], errors="coerce"))
        duration = Pipeline([
            ("prep", _preprocessor()),
            ("reg", Ridge(alpha=8.0)),
        ])
        duration.fit(dur[NUMERIC_FEATURES + CATEGORICAL_FEATURES], ydur)

        return cls(outcome, duration, latest_states, defaults)

    def predict_outcome_proba(self, features: pd.DataFrame) -> pd.DataFrame:
        x = features.copy()
        for c in CATEGORICAL_FEATURES:
            if c not in x:
                x[c] = "unknown"
        for c in NUMERIC_FEATURES:
            if c not in x:
                x[c] = np.nan
        p = self.outcome_model.predict_proba(x[NUMERIC_FEATURES + CATEGORICAL_FEATURES])
        classes = list(self.outcome_model.named_steps["clf"].classes_)
        out = pd.DataFrame(p, columns=classes, index=x.index)
        for c in OUTCOMES:
            if c not in out:
                out[c] = 0.0
        return out[OUTCOMES]

    def predict_duration_median(self, features: pd.DataFrame) -> np.ndarray:
        x = features.copy()
        for c in CATEGORICAL_FEATURES:
            if c not in x:
                x[c] = "unknown"
        for c in NUMERIC_FEATURES:
            if c not in x:
                x[c] = np.nan
        return np.exp(self.duration_model.predict(x[NUMERIC_FEATURES + CATEGORICAL_FEATURES]))

    def save(self, model_dir: str | Path):
        p = Path(model_dir)
        p.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.outcome_model, p / "outcome_model.joblib")
        joblib.dump(self.duration_model, p / "duration_model.joblib")
        (p / "states.json").write_text(json.dumps(self.latest_states, indent=2))
        (p / "defaults.json").write_text(json.dumps(self.defaults, indent=2))

    @classmethod
    def load(cls, model_dir: str | Path):
        p = Path(model_dir)
        return cls(
            joblib.load(p / "outcome_model.joblib"),
            joblib.load(p / "duration_model.joblib"),
            json.loads((p / "states.json").read_text()),
            json.loads((p / "defaults.json").read_text()),
        )
