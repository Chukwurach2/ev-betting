import unittest
from unittest.mock import patch

from .nhl_rematch_weekly import (
    bootstrap, evaluate, interpolated_quantile, summarize_weeks, weekly_statistics,
)


class WeeklyTests(unittest.TestCase):
    def test_calendar_retains_empty_and_noncomparable_weeks(self):
        games = [
            dict(game_id='1', start_time_utc='2025-10-13T01:00:00Z',
                 home_score=2, away_score=1, close_home_win_prob=0.75),
            dict(game_id='2', start_time_utc='2025-10-13T02:00:00Z',
                 home_score=1, away_score=2, close_home_win_prob=0.25),
            dict(game_id='3', start_time_utc='2025-10-21T02:00:00Z',
                 home_score=2, away_score=1, close_home_win_prob=0.5),
            dict(game_id='4', start_time_utc='2025-10-28T02:00:00Z'),
        ]
        assignments = {
            '1': dict(eligible=True, treated=True),
            '2': dict(eligible=True, treated=False),
            '3': dict(eligible=True, treated=False),
            '4': dict(eligible=False),
        }
        self.assertEqual(weekly_statistics(games, assignments), [
            dict(week='2025-10-06', treated=1, controls=1, contrast=0.5),
            dict(week='2025-10-13', treated=0, controls=0, contrast=None),
            dict(week='2025-10-20', treated=0, controls=1, contrast=None),
            dict(week='2025-10-27', treated=0, controls=0, contrast=None),
        ])

    @patch('research.nhl_rematch_weekly.bootstrap')
    def test_treated_weighting_and_coverage(self, sample):
        report = summarize_weeks([
            dict(treated=10, controls=90, contrast=0.5),
            dict(treated=30, controls=10, contrast=-0.5),
            dict(treated=90, controls=0, contrast=None),
        ])
        self.assertEqual(report['contrast'], -0.25)
        self.assertEqual(report['comparable_treated'], 40)
        self.assertEqual(report['comparable_controls'], 100)
        self.assertEqual(report['status'], 'insufficient_coverage')
        sample.assert_not_called()

    @patch('research.nhl_rematch_weekly.Random')
    def test_bootstrap_wraps_and_truncates(self, random):
        random.return_value.randrange.side_effect = [4, 2]
        result = bootstrap([(1, n) for n in range(5)], attempts=1, seed=123)
        random.assert_called_once_with(123)
        self.assertEqual(result['interval'], [1.8, 1.8])

    @patch('research.nhl_rematch_weekly.Random')
    def test_skips_zero_weight_samples(self, random):
        random.return_value.randrange.side_effect = [1, 1, 0, 0]
        result = bootstrap([(1, 0.4)] + [(0, 0)] * 4, attempts=2, seed=123)
        self.assertEqual(result, dict(attempts=2, valid=1, skipped=1, interval=[0.4, 0.4]))

    def test_determinism_and_interpolation(self):
        weeks = [(1, n / 10) for n in range(7)]
        self.assertEqual(bootstrap(weeks, 50, 20261110), bootstrap(weeks, 50, 20261110))
        self.assertEqual(interpolated_quantile([0, 2, 4], 0.125), 0.5)
        self.assertEqual(bootstrap([(0, 0)], 5, 1)['interval'], None)

    @patch('research.nhl_rematch_weekly.bootstrap')
    def test_frozen_gate(self, sample):
        weeks = [dict(treated=3, controls=8, contrast=0.03)] * 10
        for valid, lower, status in [
            (189999, 0.01, 'insufficient_coverage'),
            (190000, 0, 'not_supported'),
            (190000, 0.01, 'development_lead'),
        ]:
            sample.return_value = dict(valid=valid, interval=[lower, 0.05])
            self.assertEqual(summarize_weeks(weeks)['status'], status)
        weighted = sample.call_args.args[0]
        self.assertEqual(weighted, [(3, 0.09)] * 10)
        self.assertEqual(sample.call_args.kwargs, dict(attempts=200000, seed=20261110))

    def test_source_gate_precedes_row_access(self):
        with self.assertRaisesRegex(ValueError, 'Verify closing benchmark'):
            evaluate(None, source_scope_verified=False)
