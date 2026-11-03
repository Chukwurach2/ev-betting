import unittest
from research.nba_replication_uncertainty import (
    weekly_counts, circular_draw, replication_interval,
)


def record(gid, start, outcome='win'):
    return dict(game_id=gid, start_time_utc=start, outcome=outcome)


class FixedStarts:
    def __init__(self, starts): self.starts = iter(starts)
    def randrange(self, n): return next(self.starts)


class ReplicationUncertaintyTest(unittest.TestCase):
    def test_eastern_week_boundary_and_empty_week(self):
        rows = [record('a', '2025-10-27T00:30:00Z'),
                record('b', '2025-11-03T23:00:00Z', 'loss')]
        self.assertEqual(weekly_counts(rows), [
            ('2025-10-20', 1, 1), ('2025-10-27', 0, 0),
            ('2025-11-03', 0, 1)])

    def test_circular_blocks_wrap_truncate_and_weight_games(self):
        weeks = [('a', 0, 1), ('b', 0, 2), ('c', 0, 3),
                 ('d', 0, 4), ('e', 5, 5), ('f', 6, 6)]
        # Starts 4,1 -> indices 4,5,0,1,1,2 after truncation.
        self.assertEqual(circular_draw(weeks, FixedStarts([4, 1])), 11 / 19)

    def test_zero_decision_draw_is_skipped(self):
        weeks = [(str(i), 0, 0) for i in range(6)]
        weeks[0] = ('0', 1, 1)
        self.assertIsNone(circular_draw(weeks, FixedStarts([1, 1])))

    def test_no_rows_and_bad_rows_fail_closed(self):
        self.assertIsNone(replication_interval([])['interval95'])
        row = record('a', '2025-10-27T23:00:00Z')
        for rows in ([row, row], [dict(row, outcome='push')],
                     [dict(row, start_time_utc='2025-10-27T23:00:00')]):
            with self.assertRaises(ValueError): weekly_counts(rows)

    def test_constant_outcomes_do_not_bypass_sample_floor(self):
        result = replication_interval([record('a', '2025-10-27T23:00:00Z')])
        self.assertEqual(result['interval95'], [1, 1])
        self.assertEqual(result['valid_replicates'], 20_000)
        self.assertEqual(result['sample_gate'], 'insufficient_descriptive_inconclusive')

    def test_seeded_interval_is_order_invariant(self):
        rows = [record('a', '2025-10-27T23:00:00Z'),
                record('b', '2025-11-10T23:00:00Z', 'loss'),
                record('c', '2025-11-24T23:00:00Z')]
        self.assertEqual(replication_interval(rows),
                         replication_interval(list(reversed(rows))))


if __name__ == '__main__': unittest.main()
