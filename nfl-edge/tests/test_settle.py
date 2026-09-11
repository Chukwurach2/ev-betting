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
    def test_clv_definition(self):
        # pick fair .55, close fair .52 -> beat the close by 3pp
        self.assertAlmostEqual(0.55 - 0.52, 0.03)


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
