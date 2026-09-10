import pathlib
import sys
import unittest
import numpy as np
import pandas as pd
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
from quarter_validate import quarter_scores,pregame_features,predict_portable,FEATURES,LINES

class QuarterModelTests(unittest.TestCase):
    def pbp(self):
        base=dict(game_id='g1',season=2020,week=1,home_team='A',away_team='B',season_type='REG')
        # A defensive touchdown, two-point attempt, and a Q2 score do not need
        # guessed drive-point labels: the marker records the exact Q1 score.
        return pd.DataFrame([{**base,'play_id':i,'qtr':q,'quarter_end':end,
            'total_home_score':h,'total_away_score':a} for i,q,end,h,a in
            [(1,1,0,0,0),(2,1,0,6,0),(3,1,0,8,0),(4,1,1,8,3),(5,2,0,8,3),(6,2,0,15,3)]])
    def test_exact_quarter_marker_excludes_q2(self):
        scores=quarter_scores(self.pbp())
        self.assertEqual(scores.iloc[0].total,11)
    def test_missing_or_duplicate_marker_fails_closed(self):
        p=self.pbp()
        with self.assertRaises(ValueError): quarter_scores(p[p.quarter_end.eq(0)])
        with self.assertRaises(ValueError): quarter_scores(pd.concat([p,p[p.quarter_end.eq(1)]]))
    def test_current_week_and_future_labels_cannot_change_features(self):
        rows=[]
        for week in range(1,7):
            for j in range(2):
                rows.append(dict(game_id=f'{week}-{j}',season=2020,week=week,home_team='A',away_team='B',
                                 home_points=7,away_points=3,total=10))
        games=pd.DataFrame(rows)
        original,_=pregame_features(games)
        changed=games.copy(); changed.loc[changed.week>=4,['home_points','total']]=100
        mutated,_=pregame_features(changed)
        pd.testing.assert_frame_equal(original.loc[original.week<=4,FEATURES],mutated.loc[mutated.week<=4,FEATURES])
        prefix,_=pregame_features(games[games.week<=3])
        pd.testing.assert_frame_equal(original[original.week<=3].reset_index(drop=True),prefix)
    def test_portable_probabilities_are_bounded_and_monotonic(self):
        artifact=dict(mean=[0]*4,scale=[1]*4,coefficients=[[0]*4]*len(LINES),intercepts=[-2,2,1,0,-1,-3])
        p=predict_portable(artifact,[[1,2,3,4]])
        self.assertTrue(np.all((p>0)&(p<1)))
        self.assertTrue(np.all(np.diff(p,axis=1)<=0))
        with self.assertRaises(ValueError): predict_portable(artifact,[[float('nan')]*4])

if __name__=='__main__': unittest.main()
