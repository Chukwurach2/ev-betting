import unittest

from .nba_cup_totals import over_residual, weekly_totals
from .test_nba_cup_schedule import fixture


def final(**changes):
    return dict(dict(home_score='114', away_score='108', close_total='220.5',
                     close_over_prob='0.52', total_result='over', total_margin='1.5'),
                **changes)


class CupTotalsTests(unittest.TestCase):
    def test_full_game_over_and_under_residuals(self):
        self.assertEqual(over_residual(final()), (0.48, None))
        self.assertEqual(over_residual(final(close_total='225.5', total_result='under',
                                            total_margin='-3.5')), (-0.52, None))

    def test_missing_and_unsupported_benchmarks_are_separate(self):
        self.assertEqual(over_residual(final(close_total='')), (None, 'missing_benchmark'))
        for changes in [dict(close_total='220'), dict(close_over_prob='NaN'),
                        dict(close_over_prob='1'), dict(close_total='-2.5')]:
            with self.subTest(changes=changes):
                self.assertEqual(over_residual(final(**changes)), (None, 'unsupported_benchmark'))

    def test_invalid_finals_and_contradictory_grading_fail(self):
        for changes, pattern in [
            (dict(home_score=True), 'integer final'),
            (dict(home_score='108'), 'Tied'),
            (dict(total_result='under'), 'contradicts'),
            (dict(total_margin='-1.5'), 'contradicts'),
        ]:
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, pattern):
                over_residual(final(**changes))

    def test_fixture_gate_runs_before_any_scores_are_read(self):
        with self.assertRaisesRegex(ValueError, 'Document'):
            weekly_totals(None, None, None, source_scope_verified=False)
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            weekly_totals([fixture(0)], [], [f'o{i}' for i in range(60)],
                          source_scope_verified=True)

    def test_fixed_calendar_and_benchmark_exclusion_counts(self):
        rows = [dict(fixture(i), **final()) for i in range(63)]
        rows[0]['close_total'] = ''
        rows[60]['close_total'] = '221'
        rows[61].update(final(close_total='225.5', total_result='under', total_margin='-3.5'))
        official = [dict(r, official_fixture_id=f'o{i}', publisher_game_id=r['game_id'])
                    for i, r in enumerate(rows)]
        report = weekly_totals(rows, official, [f'o{i}' for i in range(60)],
                               source_scope_verified=True)
        weeks = report['weeks']
        self.assertEqual([w['week'] for w in weeks],
                         ['2025-10-27', '2025-11-03', '2025-11-10', '2025-11-17', '2025-11-24'])
        self.assertEqual((weeks[0]['cup'], weeks[0]['non_cup']), (59, 2))
        self.assertAlmostEqual(weeks[0]['contrast'], 0.5)
        self.assertTrue(all(w['contrast'] is None for w in weeks[1:]))
        self.assertEqual(report['benchmark_exclusions'],
                         {'cup': {'missing_benchmark': 1}, 'non_cup': {'unsupported_benchmark': 1}})
