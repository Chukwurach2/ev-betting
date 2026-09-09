import hashlib
import json
import pathlib
import sys
import tempfile
import unittest
import numpy as np
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / 'model'))
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / 'model/src'))
from production_gate import validate_artifact, ARTIFACT_FILES
from nfl_early.features import build_pregame_states
from nfl_early.model import DrivePricingModel, NUMERIC_FEATURES


def drives():
    rows = []
    for game in range(12):
        for i, (team, opp) in enumerate([('A', 'B'), ('B', 'A')]):
            rows.append(dict(game_id=f'g{game:02}', drive_id=i+1, posteam=team, defteam=opp,
                             drive_index=1, season=2020+game//6, week=1+game%6,
                             game_date=f'{2020+game//6}-09-{1+game%6:02}',
                             outcome=['TD','FG','PUNT','TO','DOWNS','OTHER'][(game+i)%6],
                             epa_per_play=.1, success_rate=.4, duration=150+game,
                             plays=6, is_home=i==0, opening_drive=1))
    return pd.DataFrame(rows)


class IntegrityTests(unittest.TestCase):
    def test_future_outcomes_do_not_change_prior_features(self):
        d = drives()
        full, _, defaults = build_pregame_states(d)
        prefix, _, prior_defaults = build_pregame_states(d[d.season == 2020])
        pd.testing.assert_frame_equal(full[full.season == 2020].reset_index(drop=True), prefix.reset_index(drop=True))
        self.assertEqual(defaults, prior_defaults)
        altered = d.copy()
        altered.loc[altered.season == 2021, ['outcome','epa_per_play']] = ['TD', 500]
        changed, _, _ = build_pregame_states(altered)
        pd.testing.assert_frame_equal(full[full.season == 2020].reset_index(drop=True), changed[changed.season == 2020].reset_index(drop=True))

    def test_model_probability_pipeline_fits_without_game_time_features(self):
        x, latest, defaults = build_pregame_states(drives())
        m = DrivePricingModel.fit(x, latest, defaults)
        p = m.predict_outcome_proba(x)
        self.assertTrue(np.allclose(p.sum(axis=1), 1))
        self.assertTrue(np.isfinite(m.predict_duration_median(x)).all())
        self.assertTrue(set(NUMERIC_FEATURES).isdisjoint({'wind','temp','total_line','team_favored_by'}))

    def test_missing_or_fabricated_validation_cannot_promote(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(validate_artifact(directory)['ready'])
            p = pathlib.Path(directory)
            (p/'validation.json').write_text(json.dumps({'feature_availability_audit':True, 'production_validated':True}))
            self.assertFalse(validate_artifact(directory)['artifact_valid'])

    def test_hash_and_chronology_and_shadow_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            p = pathlib.Path(directory)
            for f in ARTIFACT_FILES:
                (p/f).write_text('fixture')
            v = dict(train_seasons=[2020], validation_seasons=[2021], validation_drives=1200,
                     multiclass_log_loss=1.2, baseline_log_loss=1.4, td_brier_score=.1,
                     baseline_td_brier_score=.2, feature_availability_audit=True,
                     feature_audit_checks=['invariance'], production_validated=True,
                     artifact_hashes={f:hashlib.sha256((p/f).read_bytes()).hexdigest() for f in ARTIFACT_FILES})
            (p/'validation.json').write_text(json.dumps(v))
            self.assertTrue(validate_artifact(directory)['artifact_valid'])
            self.assertFalse(validate_artifact(directory)['ready'])
            (p/ARTIFACT_FILES[0]).write_text('tampered')
            self.assertFalse(validate_artifact(directory)['artifact_valid'])
            v['validation_seasons'] = [2020]
            (p/'validation.json').write_text(json.dumps(v))
            self.assertFalse(validate_artifact(directory)['artifact_valid'])

if __name__ == '__main__':
    unittest.main()
