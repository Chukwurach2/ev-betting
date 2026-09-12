"""Tests for execution-alpha v1 pure core.

Includes the Amendment-A1 template calibration check: data generated FROM
the null DGP must not reject more often than ~alpha, and a planted
systematic edge must be detected (power).
"""
import math
import pathlib
import random
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

for _mod in [m for m in sys.modules if m == "research" or m.startswith("research.")]:
    del sys.modules[_mod]

from research.execution_alpha import (  # noqa: E402
    analyze_execution,
    decide_execution,
    execution_cells,
    null_replicate_means,
    run_execution_alpha,
)
from research.market_alpha import build_game_series  # noqa: E402

UTC = timezone.utc


def odds_from_prob(p):
    if p >= 0.5:
        return -round(100.0 * p / (1.0 - p))
    return round(100.0 * (1.0 - p) / p)


def pair_quotes(home, away, kickoff, market, snap, book, f_ref, line=0.0,
                overround=1.05):
    p_ref = f_ref * overround
    p_other = (1.0 - f_ref) * overround
    if market == "FULL_GAME_SPREAD":
        ref_sel, other_sel = home, away
    elif market == "FULL_GAME_TOTAL":
        ref_sel, other_sel = "Over", "Under"
    else:  # FULL_GAME_MONEYLINE
        ref_sel, other_sel = home, away
    base = {"provider_event_id": "pid", "home_team": home, "away_team": away,
            "kickoff": kickoff, "book_key": book, "market": market,
            "line": line}
    return [dict(base, selection=ref_sel,
                 american_odds=odds_from_prob(p_ref), observed_at=snap),
            dict(base, selection=other_sel,
                 american_odds=odds_from_prob(p_other), observed_at=snap)]


def make_world(n_games, books, fair_fn, n_snaps=4,
               markets=("FULL_GAME_SPREAD",)):
    """fair_fn(game_idx, snap_idx, book) -> f_ref for the reference side."""
    quotes = []
    kickoff = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
    snaps = [datetime(2024, 9, 4, 12, 0, tzinfo=UTC) + timedelta(days=d)
             for d in (0, 1, 2, 3)][:n_snaps]
    for i in range(n_games):
        home, away = "Home%d" % i, "Away%d" % i
        for market in markets:
            for b in books:
                for j, s in enumerate(snaps):
                    f = fair_fn(i, j, b)
                    f = min(0.97, max(0.03, f))
                    quotes.extend(pair_quotes(home, away, kickoff, market,
                                              s, b, f))
    return quotes


BOOKS = ["pinnacle", "draftkings", "fanduel", "betmgm"]


class TestCells(unittest.TestCase):
    def test_best_book_and_lobo_exclusion(self):
        # DK always best at snap 0..2; close = snap 3.
        def fair(i, j, b):
            base = 0.50
            return base + (0.03 if b == "draftkings" else 0.0)
        q = make_world(35, BOOKS, fair)
        gs = build_game_series(q)
        cells = execution_cells(gs)
        self.assertTrue(cells)
        for c in cells:
            self.assertEqual(c["best_book"], "draftkings")
            # strict LOBO: evaluated book not in its own closing benchmark
            close_excl = [b for b in c["close_books_all"]
                          if b != c["best_book"]]
            self.assertGreaterEqual(len(close_excl), 2)
            self.assertNotIn("draftkings", close_excl)
        # every cell edge ~ +0.03 (minus rounding noise)
        self.assertGreater(sum(c["edge"] for c in cells) / len(cells), 0.02)

    def test_infeasible_few_games(self):
        q = make_world(5, BOOKS, lambda i, j, b: 0.5)
        gs = build_game_series(q)
        cells = execution_cells(gs)
        analysis = analyze_execution(cells)
        self.assertFalse(analysis["feasible"])
        verdict, _ = decide_execution(analysis, [])
        self.assertEqual(verdict, "infeasible")


class TestCalibrationA1Template(unittest.TestCase):
    def test_null_dgp_rejection_rate_near_alpha(self):
        # Pure noise world: books = consensus + iid N(0, 0.01) at every
        # snapshot including the close. The observed-vs-null test must
        # reject at ~5%, NOT ~100% (the failure mode of the naive
        # mean-vs-zero test that Amendment A1 killed).
        rng = random.Random(7)
        rejects = 0
        reps = 100
        for r in range(reps):
            rr = random.Random(1000 + r)
            q = make_world(40, BOOKS,
                           lambda i, j, b: 0.50 + rr.gauss(0, 0.01))
            gs = build_game_series(q)
            cells = execution_cells(gs)
            analysis = analyze_execution(cells)
            self.assertTrue(analysis["feasible"])
            null_means = null_replicate_means(cells, random.Random(r),
                                              n_rep=199)
            verdict, _ = decide_execution(analysis, null_means)
            rejects += (verdict == "execution_edge")
        rate = rejects / reps
        self.assertGreaterEqual(rate, 0.0)
        self.assertLessEqual(rate, 0.12,
                             "null rejection rate %f far above alpha" % rate)

    def test_naive_mean_vs_zero_would_be_miscalibrated(self):
        # Documents WHY the preregistration uses observed-vs-null: the
        # naive one-sided test of mean edge > 0 rejects on pure noise.
        from research.market_alpha import one_sample_one_sided
        rr = random.Random(11)
        q = make_world(40, BOOKS, lambda i, j, b: 0.50 + rr.gauss(0, 0.01))
        gs = build_game_series(q)
        cells = execution_cells(gs)
        analysis = analyze_execution(cells)
        vals = list(analysis["game_edges"].values())
        res = one_sample_one_sided(vals)
        self.assertIsNotNone(res)
        # naive test sees "significant" positive edge on pure noise
        self.assertLess(res[4], 0.05)

    def test_planted_systematic_edge_detected(self):
        # One book systematically +0.025: genuine, persistent shopping edge
        # beyond the mechanical null bias -> must reject (power).
        def fair(i, j, b):
            return 0.50 + (0.025 if b == "betmgm" else 0.0)
        q = make_world(40, BOOKS, fair)
        results, _ = run_execution_alpha(q, n_null=499, seed=3)
        self.assertEqual(results["verdict"], "execution_edge")
        self.assertGreaterEqual(results["analysis"]["mean"], 0.01)


class TestMoneylinePath(unittest.TestCase):
    def test_moneyline_cells_build(self):
        def fair(i, j, b):
            return 0.60 + (0.02 if b == "pinnacle" else 0.0)
        q = make_world(35, BOOKS, fair, markets=("FULL_GAME_MONEYLINE",))
        results, gs = run_execution_alpha(q, markets=("FULL_GAME_MONEYLINE",),
                                          n_null=99, seed=5)
        self.assertTrue(results["n_cells"] > 0)
        self.assertIn(results["verdict"], ("execution_edge", "no_edge"))


if __name__ == "__main__":
    unittest.main()
