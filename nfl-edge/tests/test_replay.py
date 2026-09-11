"""Tests for the historical market replay lab: economics math and the
no-future-leakage contract every model family must honor."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.families import EloV1, family_names, get_family
from model.research.replay import BREAKEVEN_110, simulate_bets

TEAMS = ["KC", "BUF", "PHI", "DAL"]


def game(home, away, hs, aws):
    return {"home": home, "away": away,
            "home_score": hs, "away_score": aws}


class EconomicsTests(unittest.TestCase):
    def test_wins_and_losses(self):
        b = simulate_bets([(0.6, 1), (0.6, 1)], edge_min=0.03)
        self.assertEqual(b["n_bets"], 2)
        self.assertAlmostEqual(b["profit_u"], 2 * 100 / 110, places=2)
        self.assertAlmostEqual(b["roi"], 100 / 110, places=4)
        b = simulate_bets([(0.6, 0)], edge_min=0.03)
        self.assertEqual(b["profit_u"], -1.0)
        self.assertEqual(b["roi"], -1.0)

    def test_away_side_normalized_before_scoring(self):
        # Regression: p_home=0.4 means "bet away at 0.6". y_home=0 means the
        # away side covered -> the bet WON, and edge is measured vs 0.6.
        b = simulate_bets([(0.4, 0)], edge_min=0.03)
        self.assertEqual(b["wins"], 1)
        self.assertGreater(b["profit_u"], 0)
        self.assertAlmostEqual(b["avg_edge_pp"],
                               (0.6 - BREAKEVEN_110) * 100, places=2)
        # And the mirror: p_home=0.4, y_home=1 -> away bet lost.
        b = simulate_bets([(0.4, 1)], edge_min=0.03)
        self.assertEqual(b["wins"], 0)
        self.assertEqual(b["profit_u"], -1.0)

    def test_edge_minimum_filters_marginals(self):
        b = simulate_bets([(0.51, 1), (0.49, 0)], edge_min=0.03)
        self.assertEqual(b["n_bets"], 0)
        b = simulate_bets([(0.51, 1)], edge_min=0.005)
        self.assertEqual(b["n_bets"], 1)

    def test_empty_is_valid(self):
        b = simulate_bets([], edge_min=0.03)
        self.assertEqual(b["n_bets"], 0)
        self.assertEqual(b["roi"], 0.0)


class NoLeakageTests(unittest.TestCase):
    def test_no_predictions_before_context_fit(self):
        fam = EloV1(TEAMS)
        self.assertIsNone(fam.predict(game("KC", "BUF", 0, 0)))
        fam.fit_context()  # no residuals yet -> still None
        self.assertIsNone(fam.predict(game("KC", "BUF", 0, 0)))

    def test_observe_only_affects_future_predictions(self):
        a, b = EloV1(TEAMS), EloV1(TEAMS)
        for fam in (a, b):
            for i in range(40):
                # Balanced history: ratings stay near zero, prob near 0.5.
                g = game("KC", "BUF", 24, 20) if i % 2 == 0 else \
                    game("BUF", "KC", 24, 20)
                fam.observe(g)
            fam.fit_context()
        upcoming = game("KC", "BUF", 0, 0)
        before_a = a.predict(upcoming)["pred_margin_home"]
        before_b = b.predict(upcoming)["pred_margin_home"]
        self.assertAlmostEqual(before_a, before_b, places=9)
        # Only family A sees a blowout; its next prediction must move.
        a.observe(game("KC", "BUF", 45, 3))
        after_a = a.predict(upcoming)["pred_margin_home"]
        self.assertNotAlmostEqual(before_a, after_a, places=3)
        # Family B is untouched.
        self.assertAlmostEqual(b.predict(upcoming)["pred_margin_home"],
                               before_b, places=9)

    def test_unknown_teams_refused(self):
        fam = EloV1(TEAMS)
        for _ in range(40):
            fam.observe(game("KC", "BUF", 24, 20))
        fam.fit_context()
        self.assertIsNone(fam.predict(game("KC", "XX", 0, 0)))


class RegistryTests(unittest.TestCase):
    def test_elo_registered(self):
        self.assertIn("elo-v1", family_names())
        self.assertIs(get_family("elo-v1"), EloV1)

    def test_unknown_family_raises(self):
        with self.assertRaises(ValueError):
            get_family("nope")


if __name__ == "__main__":
    unittest.main()
