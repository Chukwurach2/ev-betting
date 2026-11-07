import copy
import json
import random
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch
from research.nfl_rematch import percentile, exclusive_json
from research.test_nfl_rematch import row, schedule
from research.nfl_longshot import audit_schedule, band, probability, label, evaluate, bootstrap, verify_source


def game(gid, p='.2', week=1, hs=10, aws=20):
    return row(gid, week, 'A', 'B', p, hs, aws)


def labels(rows):
    return dict(labels={r['game_id']: label(r) for r in rows if r['season_type'] == 'regular'})


class LongshotTests(unittest.TestCase):
    def test_exact_boundaries_and_decimal_complements(self):
        for p, expected in [(Fraction(1, 4), 'treated'), (Fraction(1, 3), 'controls'),
                            (Fraction(1, 2), 'even_probability'), (Fraction(3, 10), 'gap')]:
            self.assertEqual(band(p), expected)
        self.assertEqual(label(game('x', '.3333333333333333'))['band'], 'gap')
        self.assertEqual(label(game('x', '.3333333333333334'))['band'], 'controls')
        self.assertEqual(label(game('x', '.6666666666666667'))['band'], 'gap')
        self.assertEqual(label(game('x', '.75'))['selected_probability'], '1/4')
        self.assertEqual(label(game('x', '.74999999999999999'))['band'], 'gap')
        self.assertEqual(label(game('x', '.75000000000000001'))['band'], 'treated')

    def test_audit_score_blind_order_invariant_and_postseason_excluded(self):
        rows = schedule(); rows[0]['close_home_win_prob'] = '.2'
        prior = audit_schedule(rows, (4, 4, 2))
        for r in rows:
            r.update(home_score='invalid', away_score=None)
        self.assertEqual(prior, audit_schedule(list(reversed(rows)), (4, 4, 2)))
        rows.append(dict(rows[0], season_type='postseason', game_id='post'))
        self.assertEqual(audit_schedule(rows, (4, 4, 2))['postseason_excluded'], 1)

    def test_identity_coverage_timezone_and_required_fields(self):
        with self.assertRaises(ValueError): audit_schedule(schedule())
        with self.assertRaises(ValueError): audit_schedule(schedule() + [schedule()[0]], (5, 4, 2))
        for field, value in [('home_team', 'Other'), ('start_time_utc', '2025-09-21T20:00:00'),
                             ('date_et', '2025-09-20'), ('week', '19'), ('season_type', 'unknown'),
                             ('season', '2026'), ('close_home_win_prob', 'NaN')]:
            rows = schedule(); rows[2][field] = value
            with self.assertRaises(ValueError): audit_schedule(rows, (4, 4, 2))

    def test_one_selected_team_orientation_and_brier(self):
        rows = [game('h', '.2'), game('a', '.8', hs=10, aws=20), game('c', '.6')]
        r = evaluate(rows, labels(rows))
        self.assertEqual(r['treated']['games'], 2)
        self.assertEqual(r['treated']['wins'], 1)
        self.assertAlmostEqual(r['treated']['mean_probability'], .2)
        self.assertAlmostEqual(r['treated']['mean_residual'], .3)
        self.assertAlmostEqual(r['treated']['brier'], .34)
        self.assertEqual(r['controls']['games'], 1)
        self.assertEqual(r['controls']['wins'], 1)

    def test_nonexclusive_exposed_tie_even_and_gap_flags(self):
        rows = [game('IL4EwN7l', '.5', hs=10, aws=10),
                game('fH8rHjXi', '.3', hs=10, aws=10), game('tie', hs=10, aws=10)]
        r = evaluate(rows, labels(rows))
        self.assertEqual(r['exclusions_nonexclusive'],
                         dict(search_exposed=2, tied_final=3, even_probability=1, gap=1))
        self.assertEqual(r['treated']['games'] + r['controls']['games'], 0)

    def test_bad_required_outcomes_abort_even_excluded(self):
        for field, value in [('close_home_win_prob', 'NaN'), ('close_home_win_prob', '0'),
                             ('close_home_win_prob', '1'), ('home_score', '-1'),
                             ('away_score', '2.5'), ('away_score', 'Infinity')]:
            rows = [game('IL4EwN7l', '.3')]; audit = labels(rows); rows[0][field] = value
            with self.assertRaises(ValueError): evaluate(rows, audit)
        for value in ('NaN', 'Infinity', '0', '1', None, 'bogus'):
            with self.assertRaises(ValueError): probability(value)

    def test_audit_mutation_rejected(self):
        rows = [game('x')]; audit = labels(rows); rows[0]['close_home_win_prob'] = '.4'
        with self.assertRaisesRegex(ValueError, 'label mismatch'): evaluate(rows, audit)

    def test_same_week_treated_weighting_no_global_controls(self):
        rows = [game('t1'), game('c1', '.4'), game('t2', week=2), game('t3', week=2),
                game('c2', '.4', week=2, hs=20, aws=10), game('alone', week=3)]
        r = evaluate(rows, labels(rows))
        # Week1 contrast=.2, week2=-.8, weighted by1and2, not equal weeks.
        self.assertAlmostEqual(r['weeks'][1]['contrast'], .2)
        self.assertAlmostEqual(r['weeks'][2]['contrast'], -.8)
        self.assertAlmostEqual(sum(w['weighted_contrast'] for w in r['weeks'].values()) / 3, -1.4 / 3)
        self.assertEqual(r['weeks'][3]['weight'], 0)
        self.assertIsNone(r['weeks'][3]['contrast'])
        self.assertEqual(r['comparable_weeks'], [1, 2])
        self.assertEqual((r['comparable_treated'], r['comparable_controls']), (3, 2))
        self.assertEqual(r, evaluate(list(reversed(rows)), labels(rows)))

    def test_each_minimum_stops_bootstrap(self):
        rows = [game('t'), game('c', '.4')]
        with patch('research.nfl_longshot.bootstrap') as boot:
            for minima in ((2, 1, 1), (1, 2, 1), (1, 1, 2), (30, 80, 8)):
                r = evaluate(rows, labels(rows), minima)
                self.assertEqual(r['status'], 'insufficient_data')
                self.assertIsNone(r['primary_contrast']); self.assertIsNone(r['uncertainty'])
            boot.assert_not_called()

    def test_fixed_direction_upper_interval_and_draw_floor(self):
        rows = [game('t'), game('c', '.4', hs=20, aws=10)]
        for draws, interval, expected in [(19000, [-.9, -.1], 'descriptive_lead'),
                                          (19000, [-.9, 0], 'not_supported'),
                                          (18999, [-.9, -.1], 'insufficient_bootstrap_draws')]:
            with patch('research.nfl_longshot.bootstrap', return_value=dict(valid_draws=draws, interval=interval)):
                self.assertEqual(evaluate(rows, labels(rows), (1, 1, 1))['status'], expected)
        rows[0]['home_score'] = '30'  # Positive contrast cannot reverse the rule.
        with patch('research.nfl_longshot.bootstrap', return_value=dict(valid_draws=20000, interval=[.1, .5])):
            self.assertEqual(evaluate(rows, labels(rows), (1, 1, 1))['status'], 'not_supported')

    def test_bootstrap_matches_independent_circular_block_reconstruction(self):
        weeks = {w: dict(weight=w % 3, weighted_contrast=(w % 3) * w / 100) for w in range(1, 19)}
        rng, draws = random.Random(20261107), []
        for _ in range(100):
            sample = []
            for _ in range(6):
                first = rng.randint(1, 18)
                sample.extend(((first - 1 + j) % 18) + 1 for j in range(3))
            draws.append(sum(weeks[w]['weighted_contrast'] for w in sample) / sum(weeks[w]['weight'] for w in sample))
        draws.sort(); r = bootstrap(weeks, attempts=100)
        self.assertEqual(r['seed'], 20261107); self.assertEqual(r['valid_draws'], 100)
        self.assertEqual(r['interval'], [percentile(draws, .005), percentile(draws, .995)])
        self.assertEqual(r, bootstrap(weeks, attempts=100))

    def test_zero_weights_and_linear_99_quantiles(self):
        weeks = {w: dict(weight=0, weighted_contrast=0) for w in range(1, 19)}
        r = bootstrap(weeks, attempts=17)
        self.assertEqual(r['skipped_zero_weight'], 17); self.assertIsNone(r['interval'])
        self.assertEqual(percentile([0, 100], .005), .5)
        self.assertEqual(percentile([0, 100], .995), 99.5)

    def test_source_fingerprint_rejects_substitute(self):
        with self.assertRaisesRegex(ValueError, 'fingerprint'): verify_source(b'not registered')
        with self.assertRaisesRegex(ValueError, 'fingerprint'): verify_source(b'x' * 71573)

    def test_private_outputs_exclusive_and_json_safe(self):
        rows = [game('t'), game('c', '.4')]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'report.json'; report = evaluate(rows, labels(rows))
            exclusive_json(path, report)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(path.read_text())['id'], 'nfl-longshot-calibration-v1')
            with self.assertRaises(FileExistsError): exclusive_json(path, report)


if __name__ == '__main__':
    unittest.main()
