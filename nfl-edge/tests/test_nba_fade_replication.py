import unittest
from tests.test_nba_closing_screen import game
from research.nba_fade_replication import replicate


def g(gid, day, home, away, **kw):
    return dict(game(gid, day, home, away, **kw), season='2025-26')


class FadeReplicationTest(unittest.TestCase):
    def test_fixed_fade_and_input_order(self):
        rows = [g('a','2025-11-01','A','X'), g('b','2025-11-02','B','Y'),
                g('c','2025-11-03','A','B',line='-12.5')]
        r = replicate(rows)
        self.assertEqual(r['primary']['losses'], 1)
        self.assertNotIn('b2b_underdog', r)
        self.assertEqual(r, replicate(list(reversed(rows))))

    def test_december2025_exclusion_and_wrong_season(self):
        row = g('a','2025-12-01','A','B')
        self.assertEqual(replicate([row])['exclusions']['december_missing_cup_schedule'], 1)
        row['season']='2024-25'
        with self.assertRaises(ValueError): replicate([row])

    def test_january_gap_is_not_silently_repaired(self):
        rows = [g('a','2025-12-10','A','B'), g('b','2026-01-01','A','B')]
        with self.assertRaisesRegex(ValueError, 'January prior-game'):
            replicate(rows)

    def test_january_prior_after_cup_is_retained(self):
        rows = [g('a','2025-12-30','A','X'), g('b','2025-12-31','B','Y'),
                g('c','2026-01-01','A','B',line='-12.5')]
        self.assertEqual(replicate(rows)['primary']['decisions'], 1)


if __name__ == '__main__': unittest.main()
