"""Tests for model governance: promotion, hold, reject, and demotion."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.governance import check_demotion, evaluate_promotion


def elo_v1_spread_evaluation():
    """Synthetic evaluation mirroring the real replay-elo-v1-2010-2024
    artifact for full_game_spread: roi -0.041 on 3,250 bets, no baseline
    beat, calibration slope -0.01, 2/15 profitable seasons."""
    per_season = {}
    for i, s in enumerate(range(2010, 2025)):
        roi = 0.085 if s == 2011 else (0.004 if s == 2024 else -0.05)
        per_season[str(s)] = {"roi": roi}
    return {
        "predictive": {
            "spread": {"brier": 0.2668, "log_loss": 0.7312,
                       "beats_baseline": False, "calibration_slope": -0.01},
        },
        "betting": {"n_bets": 3250, "win_rate": 0.502, "roi": -0.041,
                    "profit_u": -133.25, "avg_edge_pp": 10.13},
        "robustness": {"per_season": per_season},
        "uncertainty": {"p_roi_gt_0": 0.02, "p_clv_gt_0": 0.05},
    }


def passing_evaluation():
    return {
        "predictive": {
            "spread": {"brier": 0.24, "log_loss": 0.68,
                       "beats_baseline": True, "calibration_slope": 1.0},
        },
        "betting": {"n_bets": 600, "win_rate": 0.56, "roi": 0.05,
                    "profit_u": 30.0, "avg_edge_pp": 3.0},
        "robustness": {"per_season": {"2021": {"roi": 0.04},
                                      "2022": {"roi": 0.06},
                                      "2023": {"roi": -0.01},
                                      "2024": {"roi": 0.08}}},
        "uncertainty": {"p_roi_gt_0": 0.95, "p_clv_gt_0": 0.93},
    }


class PromotionTests(unittest.TestCase):
    def test_elo_v1_spread_is_rejected(self):
        r = evaluate_promotion("full_game_spread", "elo-v1", "v1",
                               elo_v1_spread_evaluation())
        self.assertEqual(r["decision"], "reject")
        self.assertFalse(r["checks"]["roi_positive"])
        self.assertFalse(r["checks"]["beats_baseline"])
        self.assertTrue(any("roi -0.041" in x for x in r["reasons"]))
        self.assertTrue(any("beats_baseline=False" in x
                            for x in r["reasons"]))

    def test_elo_v1_total_is_rejected(self):
        ev = elo_v1_spread_evaluation()
        ev["predictive"] = {"total": {"brier": 0.269, "log_loss": 0.7354,
                                      "beats_baseline": False,
                                      "calibration_slope": -0.05}}
        ev["betting"]["roi"] = -0.058
        r = evaluate_promotion("full_game_total", "elo-v1", "v1", ev)
        self.assertEqual(r["decision"], "reject")

    def test_all_checks_pass_promotes(self):
        r = evaluate_promotion("full_game_spread", "x", "v9",
                               passing_evaluation())
        self.assertEqual(r["decision"], "promote")
        self.assertTrue(all(r["checks"].values()))

    def test_positive_roi_without_baseline_beat_holds(self):
        ev = passing_evaluation()
        ev["predictive"]["spread"]["beats_baseline"] = False
        r = evaluate_promotion("full_game_spread", "x", "v9", ev)
        self.assertEqual(r["decision"], "hold")
        self.assertFalse(r["checks"]["beats_baseline"])

    def test_missing_sections_hold_with_named_reasons(self):
        r = evaluate_promotion("full_game_spread", "x", "v0", {})
        self.assertEqual(r["decision"], "hold")
        self.assertTrue(any("betting.n_bets" in x for x in r["reasons"]))
        self.assertTrue(any("uncertainty.p_roi_gt_0" in x
                            for x in r["reasons"]))

    def test_task_inferred_from_market_key(self):
        ev = passing_evaluation()
        ev["predictive"] = {"win": {"brier": 0.22, "log_loss": 0.64,
                                    "beats_baseline": True,
                                    "calibration_slope": 1.0}}
        r = evaluate_promotion("moneyline", "x", "v1", ev)
        self.assertEqual(r["decision"], "promote")

    def test_configurable_thresholds(self):
        ev = passing_evaluation()
        ev["betting"]["n_bets"] = 400
        r = evaluate_promotion("full_game_spread", "x", "v1", ev)
        self.assertEqual(r["decision"], "hold")
        r = evaluate_promotion("full_game_spread", "x", "v1", ev,
                               config={"min_bets": 300})
        self.assertEqual(r["decision"], "promote")


class DemotionTests(unittest.TestCase):
    def test_production_demotes_on_negative_rolling_roi(self):
        r = check_demotion("production", {"roi": -0.01, "p_roi_gt_0": 0.4})
        self.assertEqual(r["action"], "demote")
        self.assertEqual(r["to"], "challenger")

    def test_production_demotes_on_low_confidence(self):
        r = check_demotion("production", {"roi": 0.02, "p_roi_gt_0": 0.3})
        self.assertEqual((r["action"], r["to"]), ("demote", "challenger"))

    def test_production_keeps_on_healthy_evidence(self):
        r = check_demotion("production", {"roi": 0.03, "p_roi_gt_0": 0.9})
        self.assertEqual(r["action"], "keep")
        self.assertIsNone(r["to"])

    def test_challenger_demotes_when_baseline_lost(self):
        r = check_demotion("challenger", {"beats_baseline": False})
        self.assertEqual((r["action"], r["to"]), ("demote", "research"))

    def test_challenger_keeps_otherwise(self):
        r = check_demotion("challenger", {"beats_baseline": True})
        self.assertEqual(r["action"], "keep")

    def test_research_has_no_demotion(self):
        r = check_demotion("research", {"roi": -0.5})
        self.assertEqual(r["action"], "keep")


if __name__ == "__main__":
    unittest.main()


class ArtifactShapeTests(unittest.TestCase):
    def _evaluation(self, roi, beats, slope, season_rois, p_roi):
        task = "spread"
        return {
            "predictive": {task: {"beats_baseline": beats,
                                  "calibration_slope": slope}},
            "betting": {"by_task": {task: {"n_bets": 600, "roi": roi,
                                           "avg_edge_pp": 3.0}}},
            "robustness": {"per_season_by_task":
                           {task: season_rois}},
            "uncertainty": {"by_task":
                            {task: {"p_roi_gt_0": p_roi}}},
        }

    def test_prefers_per_task_slices(self):
        from model.governance import evaluate_promotion
        ev = self._evaluation(roi=0.05, beats=True, slope=1.0,
                              season_rois={2020: 0.1, 2021: 0.2, 2022: 0.05,
                                           2023: -0.01},
                              p_roi=0.95)
        v = evaluate_promotion("full_game_spread", "x", "v1", ev)
        self.assertEqual(v["decision"], "promote")
        self.assertTrue(all(v["checks"].values()))

    def test_reject_needs_both_negative_roi_and_no_beat(self):
        from model.governance import evaluate_promotion
        ev = self._evaluation(roi=-0.04, beats=False, slope=-0.01,
                              season_rois={2020: -0.1, 2021: -0.2,
                                           2022: 0.05},
                              p_roi=0.02)
        v = evaluate_promotion("full_game_spread", "elo-v1", "v1", ev)
        self.assertEqual(v["decision"], "reject")

    def test_hold_when_inconclusive(self):
        from model.governance import evaluate_promotion
        ev = self._evaluation(roi=0.02, beats=False, slope=0.9,
                              season_rois={2020: 0.1, 2021: -0.05,
                                           2022: 0.03},
                              p_roi=0.6)
        v = evaluate_promotion("full_game_spread", "x", "v1", ev)
        self.assertEqual(v["decision"], "hold")
