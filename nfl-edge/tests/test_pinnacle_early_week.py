"""Tests for pinnacle-early-week v1 pure core.

A1-template calibration: noise-only data must not reject; a planted
Pinnacle-predicts-close world must reject (power). Also locks in the
Wednesday-window selection, signal threshold, and Pinnacle exclusion.
"""
import pathlib
import random
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

for _mod in [m for m in sys.modules if m == "research" or m.startswith("research.")]:
    del sys.modules[_mod]

from research.market_alpha import build_game_series  # noqa: E402
from research.pinnacle_early_week import (  # noqa: E402
    analyze_early_week,
    decide_early_week,
    early_week_units,
    run_pinnacle_early_week,
)

UTC = timezone.utc
KICKOFF = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)  # Sunday
WED_EARLY = datetime(2024, 9, 4, 11, 55, tzinfo=UTC)  # Wednesday ~11:55 UTC
# (provider returns snapshots at ~11:55; the stored observed_at keeps it)
SAT_LATE = datetime(2024, 9, 7, 12, 0, tzinfo=UTC)  # close proxy


def odds_from_prob(p):
    if p >= 0.5:
        return -round(100.0 * p / (1.0 - p))
    return round(100.0 * (1.0 - p) / p)


def pair_quotes(home, away, kickoff, snap, book, f_ref, line=-3.0,
                overround=1.05):
    p_ref = f_ref * overround
    p_other = (1.0 - f_ref) * overround
    base = {"provider_event_id": "pid", "home_team": home, "away_team": away,
            "kickoff": kickoff, "book_key": book,
            "market": "FULL_GAME_SPREAD", "line": line}
    return [dict(base, selection=home,
                 american_odds=odds_from_prob(p_ref), observed_at=snap),
            dict(base, selection=away,
                 american_odds=odds_from_prob(p_other), observed_at=snap)]


BOOKS = ["pinnacle", "draftkings", "fanduel", "betmgm", "betrivers"]


def make_world(n_games, pin_early_fn, other_fn, close_fn):
    """pin_early_fn(i)->f_pin at WED; other_fn(i,b)->f; close_fn(i,b)->f."""
    quotes = []
    for i in range(n_games):
        home, away = "Home%d" % i, "Away%d" % i
        quotes.extend(pair_quotes(home, away, KICKOFF, WED_EARLY, "pinnacle",
                                  pin_early_fn(i)))
        for b in BOOKS[1:]:
            quotes.extend(pair_quotes(home, away, KICKOFF, WED_EARLY, b,
                                      other_fn(i, b)))
        for b in BOOKS:
            quotes.extend(pair_quotes(home, away, KICKOFF, SAT_LATE, b,
                                      close_fn(i, b)))
    return quotes


class TestMechanics(unittest.TestCase):
    def test_signal_and_agreement(self):
        # Pinnacle leans home (+0.02 vs consensus 0.50); close moves home
        # (+0.03). -> agree, positive CLV.
        q = make_world(40,
                       pin_early_fn=lambda i: 0.52,
                       other_fn=lambda i, b: 0.50,
                       close_fn=lambda i, b: 0.53)
        results, _ = run_pinnacle_early_week(q)
        a = results["analysis"]
        self.assertTrue(a["feasible"])
        self.assertEqual(a["n_games"], 40)
        self.assertAlmostEqual(a["direction"]["agree_rate"], 1.0, places=2)
        self.assertGreater(a["clv"]["mean"], 0.02)
        self.assertEqual(results["verdict"], "pinnacle_early_signal")

    def test_weak_signal_excluded(self):
        # |f_pin - C_wed| = 0.002 < 0.005 -> no signal -> infeasible.
        q = make_world(40,
                       pin_early_fn=lambda i: 0.502,
                       other_fn=lambda i, b: 0.50,
                       close_fn=lambda i, b: 0.53)
        results, _ = run_pinnacle_early_week(q)
        self.assertEqual(results["diagnostics"]["n_no_signal"], 40)
        self.assertEqual(results["verdict"], "infeasible")

    def test_pinnacle_excluded_from_consensus(self):
        # If Pinnacle were IN its own consensus, the signal would vanish.
        q = make_world(40,
                       pin_early_fn=lambda i: 0.55,
                       other_fn=lambda i, b: 0.50,
                       close_fn=lambda i, b: 0.50)
        units, _ = early_week_units(build_game_series(q))
        for u in units:
            self.assertAlmostEqual(u["c_wed"], 0.50, places=2)
            self.assertEqual(u["signal"], 1)


class TestCalibrationA1Template(unittest.TestCase):
    def test_noise_world_does_not_reject(self):
        # Pure noise: Pinnacle, others, and close all iid around 0.50.
        # Neither confirmatory test should fire (beyond ~alpha).
        rng = random.Random(21)
        sig_count = 0
        reps = 120
        for r in range(reps):
            rr = random.Random(5000 + r)
            q = make_world(40,
                           pin_early_fn=lambda i: 0.50 + rr.gauss(0, 0.01),
                           other_fn=lambda i, b: 0.50 + rr.gauss(0, 0.01),
                           close_fn=lambda i, b: 0.50 + rr.gauss(0, 0.01))
            results, _ = run_pinnacle_early_week(q)
            if results["verdict"] == "pinnacle_early_signal":
                sig_count += 1
        rate = sig_count / reps
        self.assertLessEqual(rate, 0.10,
                             "noise rejection rate %f too high" % rate)

    def test_planted_predictive_pinnacle_detected(self):
        # Pinnacle's early deviation = 0.7 * close move + small noise:
        # genuine predictive signal -> must reject (power).
        rr = random.Random(99)
        moves = [rr.uniform(-0.04, 0.04) for _ in range(60)]
        q = make_world(
            60,
            pin_early_fn=lambda i: 0.50 + 0.7 * moves[i] + rr.gauss(0, 0.003),
            other_fn=lambda i, b: 0.50 + rr.gauss(0, 0.004),
            close_fn=lambda i, b: 0.50 + moves[i] + rr.gauss(0, 0.004))
        results, _ = run_pinnacle_early_week(q)
        self.assertEqual(results["verdict"], "pinnacle_early_signal")
        self.assertGreater(results["analysis"]["direction"]["agree_rate"], 0.6)


if __name__ == "__main__":
    unittest.main()
