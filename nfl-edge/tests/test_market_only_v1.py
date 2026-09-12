"""Tests for market-only-v1: slots, consensus, model, and test calibration."""
import datetime as dt
import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from research.market_only_v1 import (
    _consensus,
    _modal_line,
    build_units,
    null_simulate,
    one_sided_paired_greater_neg,
    ridge_fit_predict,
    run_market_only,
)

import numpy as np

T_WED = dt.datetime(2024, 9, 4, 12, 0, tzinfo=dt.timezone.utc)
T_SAT = dt.datetime(2024, 9, 7, 12, 0, tzinfo=dt.timezone.utc)
T_SUN = dt.datetime(2024, 9, 8, 15, 30, tzinfo=dt.timezone.utc)
KICKOFF = dt.datetime(2024, 9, 8, 17, 0, tzinfo=dt.timezone.utc)


def _q(book, snap, line, odds, selection="HOME", market="FULL_GAME_SPREAD"):
    return {
        "quote_id": "%s-%s" % (book, snap.isoformat()),
        "provider_event_id": "evt1",
        "home_team": "HOME", "away_team": "AWAY",
        "kickoff": KICKOFF.isoformat(),
        "book_key": book, "sportsbook": book,
        "market": market, "selection": selection,
        "line": line, "american_odds": odds,
        "observed_at": snap.isoformat(),
    }


def _game_quotes(f_wed=0.55, f_sat=0.57, f_close=0.60, line=3.0):
    # NOTE: the historical table stores spread lines as |line| for BOTH
    # selections (see backfill_history pairing by abs(line)).
    def odds_for(fair, r=1.045):
        p = fair * r
        return int(round(-100 * p / (1 - p))) if p > 0.5 else int(round(100 * (1 - p) / p))
    rows = []
    for snap, f in ((T_WED, f_wed), (T_SAT, f_sat), (T_SUN, f_close)):
        for book in ("draftkings", "fanduel", "betmgm"):
            rows.append(_q(book, snap, line, odds_for(f), "HOME"))
            rows.append(_q(book, snap, line, odds_for(1 - f), "AWAY"))
    return rows


class SlotTests(unittest.TestCase):
    def test_builds_one_unit_with_three_slots(self):
        units = build_units(_game_quotes())
        self.assertEqual(len(units), 1)
        u = units[0]
        self.assertEqual(u["season"], 2024)
        self.assertAlmostEqual(u["line"], 3.0)
        self.assertTrue(0.5 < u["f_wed"] < 0.6)
        self.assertGreater(u["f_sat"], u["f_wed"])
        self.assertGreater(u["f_close"], u["f_sat"])

    def test_missing_wed_slot_excludes_unit(self):
        rows = [q for q in _game_quotes()
                if q["observed_at"] != T_WED.isoformat()]
        self.assertEqual(build_units(rows), [])

    def test_close_too_far_from_kickoff_excluded(self):
        rows = _game_quotes()
        for q in rows:  # move every quote a week earlier
            t = dt.datetime.fromisoformat(q["observed_at"])
            q["observed_at"] = (t - dt.timedelta(days=7)).isoformat()
        self.assertEqual(build_units(rows), [])


class ConsensusTests(unittest.TestCase):
    def test_modal_line_tiebreak_deterministic(self):
        self.assertEqual(_modal_line([3.0, 3.0, 7.0, 7.0]), 3.0)

    def test_consensus_needs_two_books(self):
        bp = {"a": [(3.0, 0.6)], "b": [(7.0, 0.5)]}
        self.assertIsNone(_consensus(bp, 3.0))
        bp2 = {"a": [(3.0, 0.6)], "b": [(3.0, 0.5)]}
        self.assertAlmostEqual(_consensus(bp2, 3.0), 0.55)


class ModelTests(unittest.TestCase):
    def test_ridge_recovers_linear_signal(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(300, 3))
        y = 2 * X[:, 0] - X[:, 1] + rng.normal(0, 0.1, size=300)
        pred = ridge_fit_predict(X[:200], y[:200], X[200:])
        rmse = math.sqrt(np.mean((pred - y[200:]) ** 2))
        self.assertLess(rmse, 0.2)

    def test_paired_test_direction(self):
        # diffs negative on average -> small p for H1: mean < 0
        _, _, _, p = one_sided_paired_greater_neg([-0.01] * 500)
        self.assertLess(p, 1e-6)
        _, _, _, p2 = one_sided_paired_greater_neg([0.01] * 500)
        self.assertGreater(p2, 0.99)

    def test_planted_signal_detected(self):
        # f_close = f_sat + 0.5*move + noise: momentum is real -> model
        # should beat carry-forward decisively.
        rng = np.random.default_rng(1)
        units = []
        for g in range(600):
            f_wed = float(rng.uniform(0.35, 0.65))
            move = float(rng.normal(0, 0.03))
            f_sat = min(0.95, max(0.05, f_wed + move))
            f_close = min(0.95, max(0.05,
                                    f_sat + 0.5 * move + float(rng.normal(0, 0.005))))
            units.append({"game": "g%d" % g, "market": "FULL_GAME_SPREAD",
                          "season": 2022 + (g % 3), "line": -3.0,
                          "f_wed": f_wed, "f_sat": f_sat, "f_close": f_close,
                          "move": move, "pinn_dev": 0.0, "disp_wed": 0.01,
                          "n_books_wed": 4, "is_total": 0.0})
        res = run_market_only(units)
        self.assertLess(res["primary_test"]["p_one_sided"], 0.05)
        self.assertLess(res["rmse_model"], res["rmse_bsat"])


class CalibrationTests(unittest.TestCase):
    def test_null_simulation_nominal(self):
        # Martingale evolution: f_sat is optimal; the test must not
        # systematically reject. Smoke with 40 seeds (full 200-seed check
        # runs via --null-sim before real-data execution).
        rate = null_simulate(n_games=400, n_seeds=40)
        self.assertLessEqual(rate, 0.15)


if __name__ == "__main__":
    unittest.main()
