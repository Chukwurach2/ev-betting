"""Tests for the market registry: contents, lookups, status transitions."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.markets import registry
from model.markets.registry import (MARKETS, get_market, markets_by_status,
                                    set_status, to_dict)


class ContentsTests(unittest.TestCase):
    def test_exactly_ten_markets(self):
        self.assertEqual(len(MARKETS), 10)
        self.assertEqual(
            sorted(MARKETS),
            ["first_drive_result", "first_half_spread", "first_half_total",
             "first_quarter_spread", "first_quarter_total", "full_game_spread",
             "full_game_total", "moneyline", "team_total_away",
             "team_total_home"])

    def test_required_fields(self):
        required = {"key", "label", "line_applies", "selection_ids",
                    "settlement", "push_handling", "min_sample_bets",
                    "risk_class", "status"}
        for key, m in MARKETS.items():
            self.assertTrue(required <= set(m), key)
            self.assertEqual(m["key"], key)

    def test_initial_statuses(self):
        self.assertEqual(MARKETS["full_game_spread"]["status"], "research")
        self.assertEqual(MARKETS["full_game_total"]["status"], "research")
        self.assertEqual(MARKETS["first_quarter_total"]["status"], "research")
        self.assertEqual(MARKETS["moneyline"]["status"], "unsupported")
        self.assertEqual(MARKETS["first_drive_result"]["status"],
                         "unsupported")

    def test_line_and_settlement_sanity(self):
        self.assertTrue(MARKETS["full_game_spread"]["line_applies"])
        self.assertFalse(MARKETS["moneyline"]["line_applies"])
        self.assertFalse(MARKETS["first_drive_result"]["line_applies"])
        self.assertEqual(MARKETS["full_game_spread"]["settlement"],
                         "score_margin_vs_line")
        self.assertEqual(MARKETS["full_game_total"]["push_handling"],
                         "push_excluded")

    def test_unknown_market_raises_value_error(self):
        with self.assertRaises(ValueError):
            get_market("nope")

    def test_markets_by_status(self):
        research = markets_by_status("research")
        self.assertEqual(research, ["first_quarter_total", "full_game_spread",
                                    "full_game_total"])
        with self.assertRaises(ValueError):
            markets_by_status("nope")

    def test_to_dict_returns_copies(self):
        d = to_dict()
        d["moneyline"]["status"] = "production"
        self.assertEqual(MARKETS["moneyline"]["status"], "unsupported")


class TransitionTests(unittest.TestCase):
    def setUp(self):
        self._saved = {k: m["status"] for k, m in MARKETS.items()}

    def tearDown(self):
        for k, s in self._saved.items():
            MARKETS[k]["status"] = s

    def test_forward_one_step_allowed(self):
        set_status("moneyline", "research")
        self.assertEqual(MARKETS["moneyline"]["status"], "research")
        set_status("moneyline", "candidate")
        self.assertEqual(MARKETS["moneyline"]["status"], "candidate")

    def test_forward_skip_forbidden(self):
        with self.assertRaises(ValueError):
            set_status("full_game_spread", "production")  # research -> prod
        with self.assertRaises(ValueError):
            set_status("full_game_spread", "challenger")
        # status unchanged after rejected moves
        self.assertEqual(MARKETS["full_game_spread"]["status"], "research")

    def test_backward_steps_always_allowed(self):
        set_status("full_game_spread", "candidate")
        set_status("full_game_spread", "challenger")
        set_status("full_game_spread", "production")
        set_status("full_game_spread", "challenger")  # demotion
        self.assertEqual(MARKETS["full_game_spread"]["status"], "challenger")
        set_status("full_game_spread", "research")  # multi-step demotion
        self.assertEqual(MARKETS["full_game_spread"]["status"], "research")

    def test_unknown_status_rejected(self):
        with self.assertRaises(ValueError):
            set_status("moneyline", "live")

    def test_unknown_market_rejected(self):
        with self.assertRaises(ValueError):
            set_status("nope", "research")


if __name__ == "__main__":
    unittest.main()
