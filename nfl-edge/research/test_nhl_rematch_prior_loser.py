import unittest
from datetime import datetime, timedelta, timezone
from copy import deepcopy
from research.nhl_rematch_prior_loser import (
    EASTERN, assign_one, audit_schedule, classify_games, scores, start,
)


def game(gid='current', h='A', a='B', when='2025-10-03T23:00:00Z', hs=3, aws=2):
    t = datetime.fromisoformat(when.replace('Z', '+00:00'))
    return dict(game_id=gid, home_team_id=h, away_team_id=a, home_team=h, away_team=a,
                start_time_utc=t.isoformat(), date_et=t.astimezone(EASTERN).date().isoformat(),
                season_type='regular', close_home_win_prob='.6', home_score=str(hs),
                away_score=str(aws))


def fixture():
    rows = []
    for day in range(82):
        for pair in range(16):
            h, a = str(2*pair), str(2*pair+1)
            if (day//4) % 2:
                h, a = a, h
            t = datetime(2025, 10, 1, 23, tzinfo=timezone.utc) + timedelta(days=day*2)
            rows.append(game(f'{day}-{pair}', h, a, t.isoformat()))
    return rows


class PriorLoserTests(unittest.TestCase):
    def test_audit_does_not_read_outcomes(self):
        rows = fixture()
        for r in rows:
            del r['home_score'], r['away_score']
        self.assertEqual(len(audit_schedule(rows)), 1312)

    def test_true_shared_prior_and_home_away_reversal(self):
        previous = game('prior', 'B', 'A', '2025-10-01T23:00:00Z')
        r = assign_one(game(), previous, previous)
        self.assertTrue(r['eligible'])
        self.assertTrue(r['treated'])
        self.assertEqual(r['home_gap_hours'], 48)
        self.assertFalse(assign_one(game(h='B', a='A'), previous, previous)['eligible'])

    def test_current_outcome_not_used_for_assignment(self):
        previous = game('prior', 'B', 'A', '2025-10-01T23:00:00Z')
        current = game()
        before = assign_one(current, previous, previous)
        del current['home_score'], current['away_score']
        self.assertEqual(assign_one(current, previous, previous), before)

    def test_one_sided_or_older_head_to_head_is_control(self):
        home_prior = game('old-pair', 'B', 'A', '2025-10-01T23:00:00Z')
        away_prior = game('new-other', 'B', 'C', '2025-10-02T23:00:00Z')
        r = assign_one(game(), home_prior, away_prior)
        self.assertTrue(r['eligible'])
        self.assertFalse(r['treated'])
        self.assertEqual(r['reason'], 'different_previous_fixture')

    def test_exact_rest_boundaries_and_both_teams(self):
        current = game()
        t = start(current)
        for hours, eligible in [(18, True), (96, True), (17.99, False), (96.01, False), (-1, False)]:
            prior = game('prior', 'B', 'A', (t-timedelta(hours=hours)).isoformat())
            self.assertEqual(assign_one(current, prior, prior)['eligible'], eligible)
        hp = game('h', 'C', 'A', (t-timedelta(hours=48)).isoformat())
        ap = game('a', 'B', 'D', (t-timedelta(hours=97)).isoformat())
        self.assertEqual(assign_one(current, hp, ap)['reason'], 'rest_window')

    def test_missing_prior_and_prior_win_are_ineligible(self):
        previous = game('prior', 'A', 'B', '2025-10-01T23:00:00Z')
        self.assertEqual(assign_one(game(), None, previous)['reason'], 'missing_prior')
        self.assertEqual(assign_one(game(), previous, previous)['reason'], 'home_prior_not_loss')

    def test_conflicting_shared_fixture_and_wrong_team_rejected(self):
        previous = game('prior', 'B', 'A', '2025-10-01T23:00:00Z')
        conflict = dict(previous, home_score='4')
        with self.assertRaises(ValueError): assign_one(game(), previous, conflict)
        wrong = game('other', 'C', 'D', '2025-10-01T23:00:00Z')
        with self.assertRaises(ValueError): assign_one(game(), wrong, previous)

    def test_coverage_identity_probability_and_date_fail_closed(self):
        with self.assertRaises(ValueError): audit_schedule(fixture()[:-1])
        for field, value in [('game_id', '0-0'), ('close_home_win_prob', 'nan'),
                             ('date_et', '2024-10-01'), ('home_team', 'different')]:
            rows = fixture()
            rows[1 if field == 'game_id' else 32][field] = value
            with self.assertRaises(ValueError): audit_schedule(rows)

    def test_duplicate_team_start_and_timezone_fail_closed(self):
        rows = fixture()
        rows[16]['start_time_utc'] = rows[0]['start_time_utc']
        rows[16]['date_et'] = rows[0]['date_et']
        with self.assertRaises(ValueError): audit_schedule(rows)
        for t in ['2025-10-03T23:00:00', '2025-10-03T23:00:00+01:00']:
            with self.assertRaises(ValueError): start({'start_time_utc': t})

    def test_invalid_finals_and_winner_conflict_fail_closed(self):
        for value in ['-1', '2.5', 'NaN', True, '3']:
            r = game()
            r['away_score'] = value
            with self.assertRaises(ValueError): scores(r)
        with self.assertRaises(ValueError): scores(dict(game(), moneyline_winner='away'))
        rows = fixture()
        rows[-1]['away_score'] = rows[-1]['home_score']
        with self.assertRaises(ValueError): classify_games(rows)

    def test_order_invariance_and_prior_only_history(self):
        rows = fixture()
        original = classify_games(rows)
        self.assertEqual(original, classify_games(list(reversed(rows))))
        target = next(x for x in original if x['game_id'] == '4-0')
        self.assertTrue(target['treated'])
        changed = deepcopy(rows)
        changed[-1]['home_score'], changed[-1]['away_score'] = '1', '5'
        self.assertEqual(original, classify_games(changed))
        changed = deepcopy(rows)
        changed[64]['home_score'], changed[64]['away_score'] = '1', '5'
        current = next(x for x in classify_games(changed) if x['game_id'] == '4-0')
        self.assertEqual(target, current)


if __name__ == '__main__':
    unittest.main()
