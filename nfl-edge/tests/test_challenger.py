"""Tests for the challenger model package and its picks-engine annotation."""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "model"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "ops"))

from challenger import elo
from challenger import validate as vmod
from challenger.infer import load_challenger


class EloTests(unittest.TestCase):
    def test_phi_sanity(self):
        self.assertAlmostEqual(elo.Phi(0.0), 0.5)
        self.assertGreater(elo.Phi(3.0), 0.99)
        self.assertLess(elo.Phi(-3.0), 0.01)

    def test_expected_scores_home_edge(self):
        off, deff = elo.init_ratings(["H", "A"])
        eh, ea = elo.expected_scores(off, deff, "H", "A")
        self.assertGreater(eh, ea)  # home-field advantage

    def test_update_rewards_winner_and_recenters(self):
        off, deff = elo.init_ratings(["H", "A", "B", "C"])
        before = off["H"]
        elo.update_ratings(off, deff, "H", "A", 30, 10)
        self.assertGreater(off["H"], before)      # scored more than expected
        self.assertLess(deff["A"], 0.0)          # allowed more than expected
        self.assertAlmostEqual(sum(off.values()), 0.0, places=9)
        self.assertAlmostEqual(sum(deff.values()), 0.0, places=9)

    def test_deterministic(self):
        def run():
            off, deff = elo.init_ratings(["H", "A"])
            for hs, aws in [(24, 17), (10, 20), (28, 28)]:
                elo.update_ratings(off, deff, "H", "A", hs, aws)
            return dict(off), dict(deff)
        self.assertEqual(run(), run())

    def test_cover_prob_line_convention(self):
        # predicted home margin 7, home favored by 7 -> coin flip
        self.assertAlmostEqual(elo.cover_prob_home(7.0, 7.0, 14.0), 0.5)
        self.assertGreater(elo.cover_prob_home(21.0, 7.0, 14.0), 0.75)

    def test_predict_game_unknown_team(self):
        off, deff = elo.init_ratings(["H"])
        self.assertIsNone(elo.predict_game(off, deff, "H", "ZZZ", 14.0, 14.0))


class ValidateTests(unittest.TestCase):
    def test_metrics_perfect_predictions(self):
        preds = [{"task": "win", "p": 0.99 if y else 0.01, "y": y, "base": 0.5}
                 for y in [1, 0, 1, 1, 0] * 60]
        m = vmod.evaluate(preds)["win"]
        self.assertLess(m["log_loss"], m["base_log_loss"])
        self.assertLess(m["brier"], m["base_brier"])
        self.assertTrue(m["beats_baseline"])

    def test_gate_passes_on_winning_metrics(self):
        metrics = {
            "win": {"n": 800, "log_loss": 0.60, "base_log_loss": 0.68,
                    "brier": 0.22, "base_brier": 0.24, "beats_baseline": True,
                    "calibration_slope": 1.0},
            "spread": {"n": 800, "log_loss": 0.65, "base_log_loss": 0.6931,
                       "brier": 0.23, "base_brier": 0.25, "beats_baseline": True},
            "total": {"n": 800, "log_loss": 0.66, "base_log_loss": 0.6931,
                      "brier": 0.235, "base_brier": 0.25, "beats_baseline": True},
        }
        promoted, _ = vmod.gate_decision(metrics)
        self.assertTrue(promoted)

    def test_gate_rejects_when_spread_loses_to_close(self):
        metrics = {
            "win": {"n": 800, "log_loss": 0.60, "base_log_loss": 0.68,
                    "brier": 0.22, "base_brier": 0.24, "beats_baseline": True,
                    "calibration_slope": 1.0},
            "spread": {"n": 800, "log_loss": 0.73, "base_log_loss": 0.6931,
                       "brier": 0.27, "base_brier": 0.25, "beats_baseline": False},
            "total": {"n": 800, "log_loss": 0.66, "base_log_loss": 0.6931,
                      "brier": 0.235, "base_brier": 0.25, "beats_baseline": True},
        }
        promoted, reasons = vmod.gate_decision(metrics)
        self.assertFalse(promoted)
        self.assertTrue(any("spread" in r for r in reasons))

    def test_gate_rejects_small_sample(self):
        metrics = {
            "win": {"n": 50, "beats_baseline": True},
            "spread": {"n": 50, "beats_baseline": True},
            "total": {"n": 50, "beats_baseline": True},
        }
        promoted, _ = vmod.gate_decision(metrics)
        self.assertFalse(promoted)


class InferTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ch = load_challenger("v1")

    def test_loads_v1_artifact(self):
        self.assertEqual(self.ch.version, "v1")
        self.assertGreater(self.ch.sigma_margin, 10)
        # v1 was not promoted: role must be research, gate must say why
        self.assertEqual(self.ch.role, "research")
        self.assertFalse(self.ch.gate["promoted"])

    def test_predict_real_matchup(self):
        out = self.ch.predict("KC", "BUF")
        self.assertIsNotNone(out)
        self.assertTrue(0.05 < out["win_prob_home"] < 0.95)
        self.assertTrue(-21 < out["pred_margin_home"] < 21)
        self.assertTrue(30 < out["pred_total"] < 60)

    def test_predict_unknown_team_none(self):
        self.assertIsNone(self.ch.predict("KC", "ZZZ"))

    def test_alias_mapping(self):
        # DB canonical AZ/LAR map onto nflverse ARI/LA
        out = self.ch.predict("AZ", "LAR")
        self.assertIsNotNone(out)

    def test_spread_home_away_symmetry(self):
        p_home = self.ch.cover_probability("KC", "BUF", "spreads", "home", 3.5)
        p_away = self.ch.cover_probability("KC", "BUF", "spreads", "away", 3.5)
        self.assertIsNotNone(p_home)
        # away +3.5 (db convention) -> home_line -3.5; p_home(3.5)+p_away(3.5)=1
        self.assertAlmostEqual(p_home + p_away, 1.0, places=9)

    def test_total_over_under_symmetry(self):
        p_over = self.ch.cover_probability("KC", "BUF", "totals", "over", 45.5)
        p_under = self.ch.cover_probability("KC", "BUF", "totals", "under", 45.5)
        self.assertAlmostEqual(p_over + p_under, 1.0, places=9)


class AnnotateTests(unittest.TestCase):
    def test_annotate_with_challenger(self):
        import picks as picks_mod
        ch = load_challenger("v1")
        pick = {"home_team": "KC", "away_team": "BUF", "market": "FULL_GAME_SPREAD",
                "selection": "Kansas City Chiefs", "line": -3.5}
        out = picks_mod.annotate_challenger(ch, {}, pick)
        self.assertEqual(out["challenger_version"], "v1")
        self.assertTrue(0 < out["challenger_fair_prob"] < 1)
        self.assertIsNotNone(out["challenger_pred_margin"])
        self.assertIsNotNone(out["challenger_pred_total"])

    def test_annotate_total(self):
        import picks as picks_mod
        ch = load_challenger("v1")
        pick = {"home_team": "KC", "away_team": "BUF", "market": "FULL_GAME_TOTAL",
                "selection": "Over", "line": 45.5}
        out = picks_mod.annotate_challenger(ch, {}, pick)
        self.assertTrue(0 < out["challenger_fair_prob"] < 1)

    def test_annotate_without_challenger_never_raises(self):
        import picks as picks_mod
        out = picks_mod.annotate_challenger(None, {}, {"home_team": "X"})
        self.assertIsNone(out["challenger_fair_prob"])

    def test_annotate_garbage_never_raises(self):
        import picks as picks_mod
        ch = load_challenger("v1")
        out = picks_mod.annotate_challenger(ch, {}, {"home_team": "KC"})
        self.assertIsNone(out["challenger_fair_prob"])


if __name__ == "__main__":
    unittest.main()
