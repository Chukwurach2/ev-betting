import copy
import tempfile
import unittest
from pathlib import Path
from research.nfl_rematch import audit_schedule, bootstrap, evaluate, exclusive_json, percentile


def row(i, week, home, away, p=.7, hs=20, aws=10):
    day = 7 + week * 7
    # Fixtures stay in September; timestamps/ET dates agree.
    return dict(game_id=i, season='2025', season_type='regular', week=str(week),
                start_time_utc='2025-09-%02dT20:00:00Z' % day, date_et='2025-09-%02d' % day,
                home_team_id=home, away_team_id=away, home_team=home, away_team=away,
                close_home_win_prob=str(p), home_score=str(hs), away_score=str(aws))


def schedule():
    return [row('a', 1, 'A', 'B'), row('b', 1, 'C', 'D'),
            row('c', 2, 'B', 'A'), row('d', 2, 'D', 'C')]


def labels(rows, treated):
    return dict(labels={r['game_id']: dict(week=int(r['week']), treated=r['game_id'] in treated) for r in rows})


class RematchTests(unittest.TestCase):
    def test_schedule_is_outcome_blind_and_input_order_independent(self):
        rows = schedule()
        first = audit_schedule(rows, (4, 4, 2))
        for r in rows:
            r.update(home_score='invalid', away_score=None, close_home_win_prob='NaN')
        self.assertEqual(first, audit_schedule(list(reversed(rows)), (4, 4, 2)))
        self.assertEqual(first['treated_schedule'], 2)
        self.assertFalse(first['labels']['a']['treated'])
        self.assertTrue(first['labels']['c']['treated'])

    def test_complete_coverage_and_duplicate_guards(self):
        with self.assertRaisesRegex(ValueError, 'coverage'):
            audit_schedule(schedule())
        with self.assertRaisesRegex(ValueError, 'game ID'):
            audit_schedule(schedule() + [schedule()[0]], (5, 4, 2))
        rows = schedule(); rows[1]['home_team_id'] = 'A'; rows[1]['home_team'] = 'A'
        with self.assertRaisesRegex(ValueError, 'team start'):
            audit_schedule(rows, (4, 4, 2))

    def test_identity_timezone_and_date_guards(self):
        for field, value in [('home_team', 'Other'), ('start_time_utc', '2025-09-21T20:00:00'), ('date_et', '2025-09-20')]:
            rows = schedule(); rows[2][field] = value
            with self.assertRaises(ValueError):
                audit_schedule(rows, (4, 4, 2))

    def test_third_pair_meeting_is_rejected(self):
        rows = schedule() + [dict(schedule()[0], game_id='third', start_time_utc='2025-09-28T20:00:00Z', date_et='2025-09-28', week='3')]
        with self.assertRaisesRegex(ValueError, 'two pair'):
            audit_schedule(rows, (5, 4, 2))

    def test_frozen_exposed_even_and_tie_exclusions(self):
        rows = [row('IL4EwN7l', 1, 'A', 'B'), row('fH8rHjXi', 1, 'C', 'D'),
                row('even', 1, 'E', 'F', p=.5), row('tie', 1, 'G', 'H', hs=10, aws=10)]
        r = evaluate(rows, labels(rows, set()))
        self.assertEqual(r['exclusions'], {'search_exposed': 2, 'even_probability': 1, 'tied_final': 1})
        self.assertEqual(r['treated']['games'] + r['controls']['games'], 0)
        self.assertEqual(r['status'], 'insufficient_data')

    def test_away_favorite_residual_and_brier(self):
        rows = [row('t', 1, 'A', 'B', p=.2, hs=10, aws=20), row('c', 1, 'C', 'D', p=.7)]
        r = evaluate(rows, labels(rows, {'t'}), (1, 1, 1))
        self.assertAlmostEqual(r['treated']['mean_probability'], .8)
        self.assertEqual(r['treated']['wins'], 1)
        self.assertAlmostEqual(r['treated']['brier'], .04)
        self.assertAlmostEqual(r['primary_contrast'], -.1)

    def test_within_week_weighting_excludes_unmatched_week(self):
        rows = [row('t1', 1, 'A', 'B', p=.6), row('c1', 1, 'C', 'D', p=.8),
                row('t2', 2, 'A', 'C', p=.9, hs=0), row('t3', 2, 'B', 'D', p=.9, hs=0),
                row('c2', 2, 'E', 'F', p=.6), row('unmatched', 3, 'X', 'Y', p=.99)]
        r = evaluate(rows, labels(rows, {'t1', 't2', 't3'}), (1, 1, 1))
        self.assertEqual(r['comparable_weeks'], [1, 2])
        self.assertEqual(r['comparable_treated'], 3)
        self.assertEqual(r['weeks'][3]['weight'], 0)
        self.assertAlmostEqual(r['primary_contrast'], (.2 - 2 * 1.3) / 3)

    def test_invalid_required_outcomes_abort(self):
        for field, value in [('close_home_win_prob', 'NaN'), ('close_home_win_prob', '1'), ('home_score', '-1'), ('away_score', '2.5')]:
            rows = [row('t', 1, 'A', 'B')]; rows[0][field] = value
            with self.assertRaises(ValueError):
                evaluate(rows, labels(rows, {'t'}))

    def test_bootstrap_deterministic_full_week_grid_and_quantiles(self):
        weeks = {w: dict(weight=2, weighted_contrast=-.2) for w in range(1, 19)}
        a = bootstrap(weeks)
        self.assertEqual(a, bootstrap(weeks))
        self.assertEqual(a['valid_draws'], 20000)
        self.assertAlmostEqual(a['interval'][0], -.1)
        self.assertEqual(percentile([0, 10], .25), 2.5)
        empty = bootstrap({w: dict(weight=0, weighted_contrast=0) for w in range(1, 19)}, attempts=20)
        self.assertEqual(empty['skipped_zero_weight'], 20)
        self.assertIsNone(empty['interval'])

    def test_private_receipts_are_exclusive(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'receipt.json'
            exclusive_json(p, {'unchanged': True})
            before = p.read_bytes()
            with self.assertRaises(FileExistsError):
                exclusive_json(p, {'changed': True})
            self.assertEqual(before, p.read_bytes())
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)


if __name__ == '__main__':
    unittest.main()
