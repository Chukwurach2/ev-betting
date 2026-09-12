"""Tests for shadow settlement math: spreads, totals, pushes, CLV sign."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ops"))
import settle


class SpreadSettlementTests(unittest.TestCase):
    def test_favorite_covers(self):
        # Home -3.5, wins 24-20: 4 + (-3.5) > 0
        self.assertEqual(settle.settle_spread("home", 24, 20, -3.5), "win")

    def test_favorite_fails_to_cover(self):
        self.assertEqual(settle.settle_spread("home", 23, 20, -3.5), "loss")

    def test_push_on_integer_line(self):
        # Home -3, wins 23-20: 3 + (-3) == 0
        self.assertEqual(settle.settle_spread("home", 23, 20, -3.0), "push")

    def test_underdog_side(self):
        # Away +3.5, loses 20-23: (20-23) + 3.5 > 0
        self.assertEqual(settle.settle_spread("away", 23, 20, 3.5), "win")
        self.assertEqual(settle.settle_spread("away", 27, 20, 3.5), "loss")

    def test_pick_routing_by_team_name(self):
        r = settle.settle_pick("FULL_GAME_SPREAD", "Kansas City Chiefs", -3.5,
                               "Kansas City Chiefs", "Buffalo Bills", 24, 20)
        self.assertEqual(r, "win")
        r = settle.settle_pick("FULL_GAME_SPREAD", "Buffalo Bills", 3.5,
                               "Kansas City Chiefs", "Buffalo Bills", 24, 20)
        self.assertEqual(r, "loss")

    def test_unresolvable_selection_returns_none(self):
        self.assertIsNone(settle.settle_pick("FULL_GAME_SPREAD", "Mystery FC", -3.5,
                                             "Kansas City Chiefs", "Buffalo Bills",
                                             24, 20))


class TotalSettlementTests(unittest.TestCase):
    def test_over_wins(self):
        self.assertEqual(settle.settle_total("Over", 24, 20, 43.5), "win")

    def test_over_loses(self):
        self.assertEqual(settle.settle_total("Over", 20, 20, 43.5), "loss")

    def test_under_wins(self):
        self.assertEqual(settle.settle_total("Under", 20, 20, 43.5), "win")

    def test_push_on_integer_total(self):
        self.assertEqual(settle.settle_total("Over", 24, 20, 44.0), "push")
        self.assertEqual(settle.settle_total("Under", 24, 20, 44.0), "push")

    def test_pick_routing_total(self):
        r = settle.settle_pick("FULL_GAME_TOTAL", "Over", 43.5,
                               "Kansas City Chiefs", "Buffalo Bills", 24, 20)
        self.assertEqual(r, "win")

    def test_unknown_market_returns_none(self):
        self.assertIsNone(settle.settle_pick("Q1_MONEYLINE", "Yes", 0,
                                             "Kansas City Chiefs", "Buffalo Bills",
                                             24, 20))


class ClvSignTests(unittest.TestCase):
    def test_beat_the_close_is_positive(self):
        # Took +120 (taken fair .4545), closed at fair .50 -> +4.55pp.
        self.assertAlmostEqual(settle.clv_prob_points(0.50, 0.4545), 0.0455)

    def test_worse_than_close_is_negative(self):
        # Took -150 (taken fair .60), closed at fair .55 -> -5pp.
        self.assertAlmostEqual(settle.clv_prob_points(0.55, 0.60), -0.05)

    def test_missing_inputs_yield_null(self):
        self.assertIsNone(settle.clv_prob_points(None, 0.50))
        self.assertIsNone(settle.clv_prob_points(0.50, None))

    def test_old_consensus_minus_close_convention_rejected(self):
        # Regression: the pre-2026-09-12 convention (pick-time consensus
        # minus close) has the wrong sign in the canonical slow-book case:
        # consensus .50, took +120, close .55. Old convention: .50-.55=-.05
        # (claims a bad beat); correct CLV: .55-.4545=+.0955 (beat the close).
        self.assertGreater(settle.clv_prob_points(0.55, 0.4545), 0)


class ClosingConsensusTests(unittest.TestCase):
    """closing_consensus must filter to the pick's line."""

    class FakeCursor:
        def __init__(self, probs):
            self._probs = probs
            self.last_query = None
            self.last_params = None

        def execute(self, query, params):
            self.last_query = query
            self.last_params = params

        def fetchall(self):
            return [(p,) for p in self._probs]

    class FakeConn:
        def __init__(self, cursor):
            self._cursor = cursor

        def cursor(self):
            return self._cursor

    def test_line_passed_to_query(self):
        cur = self.FakeCursor([0.50, 0.54])
        conn = self.FakeConn(cur)
        out = settle.closing_consensus(conn, "e1", "FULL_GAME_SPREAD",
                                       "Kansas City Chiefs", -3.5)
        self.assertAlmostEqual(out, 0.52)
        self.assertIn("line = %s", cur.last_query)
        self.assertEqual(cur.last_params[3], -3.5)

    def test_needs_two_books_at_line(self):
        cur = self.FakeCursor([0.50])
        conn = self.FakeConn(cur)
        self.assertIsNone(settle.closing_consensus(conn, "e1", "FULL_GAME_TOTAL",
                                                   "Over", 45.5))


if __name__ == "__main__":
    unittest.main()
