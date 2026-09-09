from __future__ import annotations
import numpy as np
import pandas as pd
from .features import future_drive_features
from .model import OUTCOMES, DrivePricingModel

POINTS = {"TD": 7, "FG": 3, "PUNT": 0, "TO": 0, "DOWNS": 0, "OTHER": 0}

def _draw_outcome(rng, probs):
    p = np.asarray(probs, dtype=float)
    p = np.clip(p, 0, None)
    p = p / p.sum()
    return rng.choice(OUTCOMES, p=p)

def simulate_game(
    model: DrivePricingModel,
    matchup: dict,
    n_sims: int = 50000,
    seed: int = 42,
    duration_sigma: float = 0.38,
    min_drive_seconds: int = 20,
    max_drive_seconds: int = 600,
):
    rng = np.random.default_rng(seed)
    home, away = matchup["home_team"], matchup["away_team"]
    states = model.latest_states
    defaults = model.defaults

    # Cache drive-index-specific predictions for speed.
    cache = {}
    for team, opp in [(home, away), (away, home)]:
        for idx in range(1, 7):
            f = future_drive_features(
                matchup, team, opp,
                states.get(team, {}), states.get(opp, {}), defaults, idx
            )
            df = pd.DataFrame([f])
            probs = model.predict_outcome_proba(df).iloc[0].to_dict()
            med = float(model.predict_duration_median(df)[0])
            cache[(team, idx)] = (probs, med)

    records = []
    for _ in range(int(n_sims)):
        # Coin toss / choice is not known pregame. 50/50 possession is a clean baseline.
        poss = home if rng.random() < 0.5 else away
        clock = 900.0
        drive_count = {home: 0, away: 0}
        score = {home: 0, away: 0}
        scored = {home: False, away: False}
        fd_td = {home: False, away: False}
        fd_score = {home: False, away: False}
        first_score_elapsed = np.inf

        while clock > 0 and (drive_count[home] + drive_count[away]) < 12:
            drive_count[poss] += 1
            idx = min(drive_count[poss], 6)
            probs, med = cache[(poss, idx)]

            # Lognormal with 'med' as the median.
            duration = float(rng.lognormal(mean=np.log(max(med, 1.0)), sigma=duration_sigma))
            duration = float(np.clip(duration, min_drive_seconds, max_drive_seconds))
            elapsed_before = 900.0 - clock

            if duration >= clock:
                clock = 0.0
                break

            outcome = _draw_outcome(rng, [probs[o] for o in OUTCOMES])
            pts = POINTS[outcome]
            if pts:
                score[poss] += pts
                scored[poss] = True
                if first_score_elapsed == np.inf:
                    first_score_elapsed = elapsed_before + duration
            if drive_count[poss] == 1:
                fd_td[poss] = outcome == "TD"
                fd_score[poss] = outcome in ("TD", "FG")

            clock -= duration
            poss = away if poss == home else home

        records.append({
            "home_points_q1": score[home],
            "away_points_q1": score[away],
            "q1_total": score[home] + score[away],
            "home_first_drive_td": fd_td[home],
            "away_first_drive_td": fd_td[away],
            "home_first_drive_score": fd_score[home],
            "away_first_drive_score": fd_score[away],
            "q1_any_td": (score[home] >= 7 or score[away] >= 7),
            "q1_both_score": (scored[home] and scored[away]),
            "first_5_min_score": first_score_elapsed <= 300.0,
        })
    return pd.DataFrame(records)
