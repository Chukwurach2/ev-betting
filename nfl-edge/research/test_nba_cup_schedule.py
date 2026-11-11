import unittest
from datetime import datetime, timedelta, timezone

from .nba_cup_schedule import audit_fixtures


def fixture(i):
    start = datetime(2025, 11, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i)
    return dict(game_id=f'p{i}', season='2025-26', season_type='regular',
                date_et='2025-10-31', start_time_utc=start.isoformat(),
                home_team_id='home', away_team_id='away')


class CupScheduleTests(unittest.TestCase):
    def setUp(self):
        self.publisher = [fixture(i) for i in range(61)]
        self.official = [
            dict(row, official_fixture_id=f'o{i}', publisher_game_id=row['game_id'])
            for i, row in enumerate(self.publisher)
        ]
        self.cup = [f'o{i}' for i in range(60)]

    def check(self):
        return audit_fixtures(self.publisher, self.official, self.cup)

    def test_exact_outcome_blind_group_assignment(self):
        self.publisher.extend([
            dict(fixture(0), season_type='postseason'),
            dict(fixture(0), season='2024-25'),
            dict(fixture(0), start_time_utc='2025-11-29T05:00:00Z', date_et='2025-11-29'),
        ])
        labels = self.check()
        self.assertEqual(sum(labels.values()), 60)
        self.assertIs(labels['p60'], False)

    def test_missing_regular_fixture_or_missing_official_cup_id(self):
        control = self.official.pop()
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            self.check()
        self.official.append(control)
        self.cup[-1] = 'missing'
        with self.assertRaisesRegex(ValueError, 'Incomplete'):
            self.check()

    def test_rejects_duplicate_cup_id(self):
        self.cup[-1] = self.cup[0]
        with self.assertRaisesRegex(ValueError, '60 distinct'):
            self.check()

    def test_duplicate_crosswalk_ids(self):
        for key in ('official_fixture_id', 'publisher_game_id'):
            original = self.official[-1][key]
            self.official[-1][key] = self.official[0][key]
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'one-to-one'):
                self.check()
            self.official[-1][key] = original

    def test_same_names_cannot_rescue_start_or_side_mismatch(self):
        control = self.official[-1]
        for key, value in [('home_team_id', 'another'), ('start_time_utc', '2025-11-01T02:01:00Z')]:
            original = control[key]
            control[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'identity disagree'):
                self.check()
            control[key] = original

    def test_rejects_duplicate_team_start_and_wrong_eastern_date(self):
        original = self.publisher[-1]
        self.publisher[-1] = dict(original, start_time_utc=self.publisher[0]['start_time_utc'])
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.check()
        self.publisher[-1] = dict(original, date_et='2025-11-01')
        with self.assertRaisesRegex(ValueError, 'Eastern date'):
            self.check()
