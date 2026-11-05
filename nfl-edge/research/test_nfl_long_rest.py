import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from research.nfl_long_rest import audit_schedule, rest_role, evaluate, bootstrap, verify_source
from research.nfl_rematch import percentile


def row(i, day=14, home='A', away='B', p=.6, hs=20, aws=10, week=3):
    at = datetime(2025, 9, 1, 20, tzinfo=timezone.utc) + timedelta(days=day)
    return dict(game_id=i, season='2025', season_type='regular', week=str(week),
        home_team_id=home, away_team_id=away, home_team=home, away_team=away,
        start_time_utc=at.isoformat(), date_et=at.astimezone(ZoneInfo('America/New_York')).date().isoformat(),
        close_home_win_prob=str(p), home_score=str(hs), away_score=str(aws))


def labels(rows, roles):
    return dict(labels={r['game_id']: dict(week=int(r['week']), role=roles.get(r['game_id'], 'control')) for r in rows})


def schedule():
    return [row('a', 0, 'A', 'C', week=1), row('b', 7, 'B', 'D', week=2), row('c', 14, 'A', 'B'), row('d', 14, 'C', 'D')]


class LongRestTests(unittest.TestCase):
    def test_schedule_blind_order_independent_previous_start(self):
        rows = schedule(); first = audit_schedule(rows, (4, 4, 2))
        for r in rows: r.update(home_score='bad', away_score=None, close_home_win_prob='NaN')
        self.assertEqual(first, audit_schedule(list(reversed(rows)), (4, 4, 2)))
        self.assertEqual(first['labels']['c'], dict(week=3, home_days=14, away_days=7, role='home'))
        self.assertEqual(first['schedule_counts'], {'missing_prior': 2, 'home': 2})

    def test_inclusive_bounds_and_roles(self):
        for long in (12, 18):
            for ordinary in (5, 9):
                self.assertEqual(rest_role(long, ordinary), 'home')
                self.assertEqual(rest_role(ordinary, long), 'away')
        for a, b in [(11.99999, 7), (18.00001, 7), (14, 4.99999), (14, 9.00001), (14, 14)]:
            self.assertEqual(rest_role(a, b), 'other_rest')
        self.assertEqual(rest_role(None, 7), 'missing_prior')
        self.assertEqual(rest_role(5, 9), 'control')

    def test_elapsed_not_calendar_days(self):
        rows = schedule(); rows[2] = row('c', 11.999, 'A', 'B')
        result = audit_schedule(rows, (4, 4, 2))
        self.assertAlmostEqual(result['labels']['c']['home_days'], 11.999)
        self.assertEqual(result['labels']['c']['role'], 'other_rest')

    def test_identity_coverage_start_guards(self):
        with self.assertRaises(ValueError): audit_schedule(schedule())
        with self.assertRaises(ValueError): audit_schedule(schedule() + [schedule()[0]], (5, 4, 2))
        for field, value in [('home_team', 'Other'), ('start_time_utc', '2025-09-15T20:00:00'), ('date_et', '2025-09-14')]:
            rows = schedule(); rows[2][field] = value
            with self.assertRaises(ValueError): audit_schedule(rows, (4, 4, 2))

    def test_orientation_shared_control_and_weighting(self):
        rows = [row('h', p=.8), row('a', p=.2, hs=10, aws=20), row('a2', p=.2, hs=10, aws=20), row('c', p=.7), row('unmatched', week=4)]
        r = evaluate(rows, labels(rows, {'h': 'home', 'a': 'away', 'a2': 'away', 'unmatched': 'home'}))
        h, a = r['strata'][4:6]
        self.assertEqual(a['treated']['wins'], 2)
        self.assertAlmostEqual(a['treated']['mean_probability'], .8)
        self.assertAlmostEqual(a['controls']['mean_probability'], .3)
        self.assertEqual(r['comparable_treated'], 3)
        self.assertEqual(r['comparable_control_appearances'], 2)
        self.assertAlmostEqual(h['contrast'], -.1)
        self.assertAlmostEqual(a['contrast'], .5)
        self.assertAlmostEqual(sum(w['weighted_contrast'] for w in r['weeks'].values()) / 3, (-.1 + 2 * .5) / 3)
        self.assertEqual(r['weeks'][4]['weight'], 0)
        self.assertEqual(r['comparable_weeks'], [3])

    def test_nonexclusive_exclusions_even_p_retained(self):
        rows = [row('missing'), row('other'), row('IL4EwN7l', hs=10, aws=10), row('tie', hs=10, aws=10), row('even', p=.5)]
        r = evaluate(rows, labels(rows, {'missing': 'missing_prior', 'other': 'other_rest', 'even': 'home'}))
        self.assertEqual(r['exclusions_nonexclusive'], {'missing_prior': 1, 'other_rest': 1, 'search_exposed': 1, 'tied_final': 2})
        self.assertEqual(r['strata'][4]['treated']['appearances'], 1)

    def test_invalid_outcome_even_if_ineligible(self):
        for field, value in [('close_home_win_prob', 'NaN'), ('close_home_win_prob', '1'), ('home_score', '-1'), ('away_score', '2.5')]:
            rows = [row('x')]; rows[0][field] = value
            with self.assertRaises(ValueError): evaluate(rows, labels(rows, {'x': 'missing_prior'}))

    def test_bootstrap_grid_seed_quantiles_and_skips(self):
        weeks = {w: dict(weight=1, weighted_contrast=w / 100) for w in range(1, 19)}
        a = bootstrap(weeks, attempts=100)
        self.assertEqual(a, bootstrap(weeks, attempts=100))
        self.assertEqual(a['valid_draws'], 100)
        self.assertEqual(a['seed'], 20261105)
        self.assertEqual(percentile([0, 100], .005), .5)
        self.assertEqual(percentile([0, 100], .995), 99.5)
        weeks = {w: dict(weight=0, weighted_contrast=0) for w in range(1, 19)}
        self.assertEqual(bootstrap(weeks, attempts=17)['skipped_zero_weight'], 17)
        self.assertIsNone(bootstrap(weeks, attempts=17)['interval'])

    def test_small_sample_stays_insufficient(self):
        rows = [row('t'), row('c')]; r = evaluate(rows, labels(rows, {'t': 'home'}))
        self.assertEqual(r['status'], 'insufficient_data')
        self.assertIsNone(r['primary_contrast']); self.assertIsNone(r['uncertainty'])

    def test_source_rejects_substitute_before_parse(self):
        with self.assertRaisesRegex(ValueError, 'fingerprint'): verify_source(b'not source')

    def test_decision_uses_registered_direction_and_valid_draw_gate(self):
        rows = [row('t', p=.6), row('c', p=.8)]
        audit = labels(rows, {'t': 'home'})
        for interval, valid, expected in [([.01, .3], 20000, 'descriptive_lead'),
                                        ([-.01, .3], 20000, 'not_supported'),
                                        ([.01, .3], 18999, 'insufficient_data')]:
            with patch('research.nfl_long_rest.bootstrap', return_value=dict(interval=interval, valid_draws=valid)):
                r = evaluate(rows, audit, (1, 1, 1))
                self.assertEqual(r['status'], expected)
                self.assertAlmostEqual(r['primary_contrast'], .2)
        with patch('research.nfl_long_rest.bootstrap', return_value=dict(interval=[.001, .1], valid_draws=20000)):
            r = evaluate([row('t', p=.79), row('c', p=.8)], audit, (1, 1, 1))
            self.assertEqual(r['status'], 'not_supported')

    def test_bootstrap_matches_independent_circular_block_reconstruction(self):
        import random
        weeks = {w: dict(weight=w % 3, weighted_contrast=(w % 3) * w / 100) for w in range(1, 19)}
        rng = random.Random(20261105)
        values = []
        for _ in range(100):
            sampled = []
            for _ in range(6):
                start = rng.randrange(18)
                sampled.extend([(start + k) % 18 + 1 for k in range(3)])
            values.append(sum(weeks[w]['weighted_contrast'] for w in sampled) / sum(weeks[w]['weight'] for w in sampled))
        values.sort()
        r = bootstrap(weeks, attempts=100)
        self.assertEqual(r['interval'], [percentile(values, .005), percentile(values, .995)])
