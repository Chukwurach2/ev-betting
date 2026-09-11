"""Tests for the standardized evaluation artifact (evaluate.py)."""
import pathlib
import random
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.evaluate import (ARTIFACTS_VERSION, bootstrap_roi,
                                     calibration_fit, reliability_buckets,
                                     standard_report)


def pred_rows(task, ps, ys, season=2020):
    rows = []
    for i, (p, y) in enumerate(zip(ps, ys)):
        rows.append({
            "task": task, "p": p, "y": y, "season": season,
            "favorite": True if task == "spread" and i % 2 == 0 else None,
            "home": True if task != "total" else None,
            "line_bucket": "-3..3" if task == "spread" else None,
            "total_bucket": "42-46" if task == "total" else None,
            "edge_bucket": "small", "early_season": i % 3 == 0,
            "side": "home" if p >= 0.5 else "away",
        })
    return rows


def bet_rows(n, win_prob=0.5, seed=1, season=2020, task="spread"):
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        y = 1 if rng.random() < win_prob else 0
        out.append({"p": 0.6, "y": y,
                    "profit_u": 100.0 / 110.0 if y == 1 else -1.0,
                    "edge_pp": (0.6 - 110.0 / 210.0) * 100.0,
                    "season": season, "task": task,
                    "edge_bucket": "med", "early_season": True,
                    "side": "home"})
    return out


def perfect_calibration_data():
    """900 points in 9 groups; every decile bin has obs_rate == mean_p,
    so slope is exactly 1 and intercept exactly 0. Tiny within-group jitter
    keeps the original order after the (p, y) sort used by the binning."""
    ps, ys = [], []
    for k in range(1, 10):
        p = k / 10.0
        for i in range(100):
            ps.append(p + (i - 50) * 1e-9)
            ys.append(1 if (i % 10) < k else 0)
    return ps, ys


class CalibrationTests(unittest.TestCase):
    def test_perfect_calibration(self):
        ps, ys = perfect_calibration_data()
        slope, intercept = calibration_fit(ps, ys)
        self.assertAlmostEqual(slope, 1.0, places=6)
        self.assertLess(abs(intercept), 1e-6)

    def test_small_sample_returns_none(self):
        slope, intercept = calibration_fit([0.6] * 10, [1] * 10)
        self.assertIsNone(slope)
        self.assertIsNone(intercept)

    def test_reliability_buckets_sum_to_n(self):
        rng = random.Random(3)
        ps = [rng.random() for _ in range(200)]
        ys = [1 if rng.random() < p else 0 for p in ps]
        buckets = reliability_buckets(ps, ys)
        self.assertEqual(sum(b["n"] for b in buckets), 200)
        self.assertEqual(len(buckets), 10)

    def test_reliability_empty(self):
        self.assertEqual(reliability_buckets([], []), [])


class BettingTests(unittest.TestCase):
    def test_max_drawdown_known_sequence(self):
        bets = bet_rows(0)
        for profit, y in ((1.0, 1), (1.0, 1), (-3.0, 0), (1.0, 1)):
            bets.append({"p": 0.6, "y": y, "profit_u": profit,
                         "edge_pp": 7.0, "season": 2020})
        rep = standard_report([], bets, {})
        # cum: 1, 2, -1, 0 -> peak 2, trough -1 -> drawdown 3.
        self.assertEqual(rep["betting"]["max_drawdown_u"], 3.0)

    def test_max_drawdown_no_drawdown(self):
        rep = standard_report([], bet_rows(10, win_prob=1.0), {})
        self.assertEqual(rep["betting"]["max_drawdown_u"], 0.0)

    def test_realized_edge_pp(self):
        # All wins: realized edge = (1 - 110/210) * 100.
        rep = standard_report([], bet_rows(10, win_prob=1.0), {})
        self.assertAlmostEqual(rep["betting"]["realized_edge_pp"],
                               (1 - 110.0 / 210.0) * 100.0, places=2)


class UncertaintyTests(unittest.TestCase):
    def test_bootstrap_determinism(self):
        bets = bet_rows(200, win_prob=0.52, seed=5)
        r1 = standard_report([], bets, {}, seed=7)
        r2 = standard_report([], bets, {}, seed=7)
        self.assertEqual(r1["uncertainty"]["roi_ci_95"],
                         r2["uncertainty"]["roi_ci_95"])
        self.assertEqual(r1["uncertainty"]["p_roi_gt_0"],
                         r2["uncertainty"]["p_roi_gt_0"])

    def test_p_roi_gt_0_all_wins(self):
        rep = standard_report([], bet_rows(20, win_prob=1.0), {}, seed=7)
        self.assertEqual(rep["uncertainty"]["p_roi_gt_0"], 1.0)

    def test_p_roi_gt_0_all_losses(self):
        rep = standard_report([], bet_rows(20, win_prob=0.0), {}, seed=7)
        self.assertEqual(rep["uncertainty"]["p_roi_gt_0"], 0.0)

    def test_ci_brackets_point_estimate(self):
        bets = bet_rows(300, win_prob=0.55, seed=9)
        rep = standard_report([], bets, {}, seed=7)
        lo, hi = rep["uncertainty"]["roi_ci_95"]
        roi = rep["betting"]["roi"]
        self.assertLessEqual(lo, hi)
        self.assertLessEqual(lo, roi + 0.05)
        self.assertGreaterEqual(hi, roi - 0.05)

    def test_no_bets_uncertainty_none(self):
        rep = standard_report([], [], {}, seed=7)
        self.assertIsNone(rep["uncertainty"]["roi_ci_95"])
        self.assertIsNone(rep["uncertainty"]["p_roi_gt_0"])


class ReportStructureTests(unittest.TestCase):
    def test_sections_and_version(self):
        preds = pred_rows("spread", [0.6] * 120, [1] * 60 + [0] * 60)
        bets = bet_rows(50, win_prob=0.55, seed=2)
        rep = standard_report(preds, bets, {"family": "elo-v1"}, seed=7)
        self.assertEqual(rep["artifacts_version"], ARTIFACTS_VERSION)
        self.assertEqual(rep["meta"]["family"], "elo-v1")
        for section in ("predictive", "betting", "robustness", "uncertainty"):
            self.assertIn(section, rep)
        ptask = rep["predictive"]["spread"]
        for key in ("n", "brier", "log_loss", "calibration_slope",
                    "calibration_intercept", "reliability"):
            self.assertIn(key, ptask)
        self.assertEqual(ptask["n"], 120)
        self.assertIn("by_task", rep["betting"])
        self.assertIn("per_season", rep["robustness"])
        self.assertIn("slices", rep["robustness"])

    def test_slices_present(self):
        rng = random.Random(4)
        ps = [0.3 + 0.4 * rng.random() for _ in range(300)]
        ys = [1 if rng.random() < p else 0 for p in ps]
        preds = pred_rows("spread", ps, ys)
        bets = bet_rows(120, win_prob=0.5, seed=6)
        rep = standard_report(preds, bets, {}, seed=7)
        slices = rep["robustness"]["slices"]
        for name in ("fav_vs_dog", "preferred_side", "spread_bucket",
                     "edge_bucket", "early_vs_late"):
            self.assertIn(name, slices)
        self.assertIn("favorite", slices["fav_vs_dog"])
        self.assertIn("underdog", slices["fav_vs_dog"])

    def test_empty_inputs_do_not_crash(self):
        rep = standard_report([], [], {})
        self.assertEqual(rep["betting"]["n_bets"], 0)
        self.assertEqual(rep["predictive"], {})
        self.assertEqual(rep["robustness"]["per_season"], {})

    def test_win_task_no_bets_needed(self):
        preds = pred_rows("win", [0.55] * 100, [1] * 55 + [0] * 45)
        rep = standard_report(preds, [], {"family": "x"}, seed=7)
        self.assertEqual(rep["predictive"]["win"]["n"], 100)
        self.assertNotIn("by_task", rep["betting"])


if __name__ == "__main__":
    unittest.main()


class IntegrationWiringTests(unittest.TestCase):
    def test_predictive_carries_beats_baseline(self):
        from model.research.evaluate import standard_report
        preds = [{"task": "spread", "p": 0.9 if y else 0.1, "y": y,
                  "base": 0.5, "season": 2020}
                 for y in ([1] * 60 + [0] * 60)]
        rep = standard_report(preds, [], {"t": 1}, seed=7)
        m = rep["predictive"]["spread"]
        self.assertTrue(m["beats_baseline"])
        self.assertLess(m["base_brier"], 1.0)

    def test_predictive_without_base_omits_beats_baseline(self):
        from model.research.evaluate import standard_report
        preds = [{"task": "spread", "p": 0.6, "y": 1, "season": 2020}
                 for _ in range(60)]
        rep = standard_report(preds, [], {"t": 1}, seed=7)
        self.assertNotIn("beats_baseline", rep["predictive"]["spread"])

    def test_per_season_by_task_present(self):
        from model.research.evaluate import standard_report
        bets = [{"task": "spread", "p": 0.6, "y": 1, "profit_u": 0.909,
                 "edge_pp": 5.0, "season": s} for s in (2020, 2021)]
        rep = standard_report([], bets, {"t": 1}, seed=7)
        self.assertIn("spread", rep["robustness"]["per_season_by_task"])


class HoldoutTests(unittest.TestCase):
    def _bets(self):
        return [{"task": "spread", "p": 0.6, "y": 1, "profit_u": 0.909,
                 "edge_pp": 5.0, "season": s}
                for s in (2020, 2021, 2022, 2023, 2024)]

    def test_no_holdout_declared_gives_none(self):
        rep = standard_report([], self._bets(), {})
        self.assertIsNone(rep["holdout"])

    def test_holdout_section_restricted_to_declared_seasons(self):
        rep = standard_report([], self._bets(),
                              {"holdout_seasons": [2023, 2024]})
        h = rep["holdout"]
        self.assertEqual(h["seasons"], [2023, 2024])
        self.assertEqual(h["n_bets"], 2)
        self.assertGreater(h["roi"], 0)

    def test_holdout_empty_when_no_bets_in_window(self):
        rep = standard_report([], self._bets(),
                              {"holdout_seasons": [1999]})
        self.assertEqual(rep["holdout"]["n_bets"], 0)
        self.assertIsNone(rep["holdout"]["roi"])
