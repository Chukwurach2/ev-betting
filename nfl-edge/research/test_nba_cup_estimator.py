import unittest
from unittest.mock import patch

from .nba_cup_totals import summarize_weeks
from .nhl_rematch_weekly import bootstrap


def week(cup=10, non_cup=13, contrast=0.04):
    return dict(cup=cup, non_cup=non_cup, contrast=contrast)


class CupEstimatorTests(unittest.TestCase):
    @patch('research.nba_cup_totals.bootstrap')
    def test_weights_by_cup_count_and_excludes_incomparable_weeks(self, sample):
        report = summarize_weeks([week(cup=10, contrast=0.5), week(cup=30, contrast=-0.5),
                                  week(cup=60, non_cup=0, contrast=None)])
        self.assertEqual(report['contrast'], -0.25)
        self.assertEqual(report['comparable_cup_games'], 40)
        self.assertEqual(report['comparable_weeks'], 2)
        self.assertEqual(report['status'], 'insufficient_coverage')
        sample.assert_not_called()

    @patch('research.nba_cup_totals.bootstrap')
    def test_each_coverage_gate_stops_bootstrap(self, sample):
        for weeks in ([week(cup=9)] * 4, [week(non_cup=12)] * 4,
                      [week(cup=20, non_cup=20)] * 3):
            with self.subTest(weeks=weeks):
                self.assertEqual(summarize_weeks(weeks)['status'], 'insufficient_coverage')
        sample.assert_not_called()

    @patch('research.nba_cup_totals.bootstrap')
    def test_fixed_parameters_and_screen(self, sample):
        for valid, lower, contrast, status in [
            (189999, 0.01, 0.04, 'insufficient_coverage'),
            (190000, 0, 0.04, 'not_supported'),
            (190000, 0.01, 0.029, 'not_supported'),
            (190000, 0.01, 0.03, 'development_lead'),
        ]:
            sample.return_value = dict(valid=valid, interval=[lower, 0.1])
            weeks = [week(contrast=contrast)] * 4 + [week(cup=0, contrast=None)]
            self.assertEqual(summarize_weeks(weeks)['status'], status)
        sample.assert_called_with([(10, 0.3)] * 4 + [(0, 0.0)],
                                  attempts=200000, seed=20261111, block_size=2)

    @patch('research.nhl_rematch_weekly.Random')
    def test_two_week_blocks_wrap_and_last_block_is_truncated(self, random):
        random.return_value.randrange.side_effect = [4, 1, 3]
        result = bootstrap([(1, float(i)) for i in range(5)], 1, 20261111, block_size=2)
        self.assertEqual(result['interval'], [2.0, 2.0])
        self.assertEqual(result['valid'], 1)

    def test_constant_fixture_reproducible_at_registered_draw_count(self):
        report = summarize_weeks([week()] * 5)
        self.assertEqual(report['bootstrap']['valid'], 200000)
        self.assertEqual(report['bootstrap']['skipped'], 0)
        self.assertEqual(report['bootstrap']['interval'], [0.04, 0.04])
