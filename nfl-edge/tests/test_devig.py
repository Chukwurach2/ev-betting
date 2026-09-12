"""Tests for the de-vig methods and the tournament's pure core.

The Shin/additive identity test below implements Shin's method per the
published formula (penaltyblog/implied: q_i(z) =
(sqrt(z^2 + 4(1-z) p_i^2 / R) - z) / (2 - 2z), z solved from sum q = 1)
and verifies it coincides with additive on two-outcome markets — the
justification for excluding Shin from the preregistered candidate set.
"""
import math
import pathlib
import random
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

# Landmine: nfl-edge/model/research is a *separate* package that some test
# modules (e.g. test_challenger) expose as top-level ``research`` by putting
# nfl-edge/model on sys.path. If it was already imported and cached, evict it
# so the bare ``research`` name below resolves to this directory (nfl-edge is
# first on sys.path here). ``model.research`` itself is untouched.
for _mod in [m for m in sys.modules if m == "research" or m.startswith("research.")]:
    del sys.modules[_mod]

from research.devig import (
    DEVIG_METHODS,
    devig_additive,
    devig_method_names,
    devig_multiplicative,
    devig_power,
    get_devig_method,
)
from research.devig_tournament import (
    _holm_bonferroni,
    _paired_test,
    run_tournament,
)


def _shin_reference(p1, p2):
    """Shin's method per the published formula (two outcomes)."""
    inv = [p1, p2]
    r = sum(inv)

    def implied(z):
        return [((z * z + 4 * (1 - z) * pi * pi / r) ** 0.5 - z)
                / (2 - 2 * z) for pi in inv]

    def err(z):
        return 1.0 - sum(implied(z))

    lo, hi = 0.0, 1.0 - 1e-12
    if not (err(lo) < 0 < err(hi)):
        raise AssertionError("no Shin root bracket")
    for _ in range(300):
        mid = 0.5 * (lo + hi)
        if err(mid) < 0:
            lo = mid
        else:
            hi = mid
    return implied(0.5 * (lo + hi))


class TestDevigMethods(unittest.TestCase):
    def test_registry(self):
        self.assertEqual(devig_method_names(),
                         ["additive", "multiplicative", "power"])
        for name in devig_method_names():
            self.assertIs(get_devig_method(name), DEVIG_METHODS[name])
        with self.assertRaises(ValueError):
            get_devig_method("shin")

    def test_multiplicative_known(self):
        q1, q2 = devig_multiplicative(0.55, 0.50)
        self.assertAlmostEqual(q1, 0.55 / 1.05, places=12)
        self.assertAlmostEqual(q2, 0.50 / 1.05, places=12)

    def test_additive_known(self):
        q1, q2 = devig_additive(0.55, 0.50)
        self.assertAlmostEqual(q1, 0.525, places=12)
        self.assertAlmostEqual(q2, 0.475, places=12)

    def test_power_symmetric(self):
        q1, q2 = devig_power(0.55, 0.55)
        self.assertAlmostEqual(q1, 0.5, places=9)
        self.assertAlmostEqual(q2, 0.5, places=9)

    def test_power_sums_to_one_and_favors_favorite(self):
        # favorite at 0.70/0.40 implied (R = 1.10)
        qm = devig_multiplicative(0.70, 0.40)
        qp = devig_power(0.70, 0.40)
        self.assertAlmostEqual(qp[0] + qp[1], 1.0, places=9)
        # power reallocates vig away from the longshot vs multiplicative
        self.assertGreater(qp[0], qm[0])
        self.assertLess(qp[1], qm[1])

    def test_all_methods_agree_on_fair_book(self):
        for fn in DEVIG_METHODS.values():
            q1, q2 = fn(0.6, 0.4)
            self.assertAlmostEqual(q1, 0.6, places=9)
            self.assertAlmostEqual(q2, 0.4, places=9)

    def test_invalid_inputs_return_none(self):
        for fn in DEVIG_METHODS.values():
            self.assertIsNone(fn(0.0, 0.5))
            self.assertIsNone(fn(0.5, 1.0))
            self.assertIsNone(fn(-0.1, 0.5))
            self.assertIsNone(fn(None, 0.5))
            self.assertIsNone(fn("x", 0.5))

    def test_shin_equals_additive_two_outcome(self):
        """Preregistered justification for excluding Shin as a candidate."""
        rng = random.Random(42)
        worst = 0.0
        for _ in range(100):
            p1 = rng.uniform(0.15, 0.85)
            p2 = rng.uniform(0.15, 0.85)
            if p1 + p2 <= 1.0:
                continue
            qs = _shin_reference(p1, p2)
            qa = devig_additive(p1, p2)
            worst = max(worst, abs(qs[0] - qa[0]), abs(qs[1] - qa[1]))
        self.assertLess(worst, 1e-9)


T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)


def _q(book, snap, market, selection, line, odds, home="KC", away="BUF",
       evt="evt-1"):
    return {
        "quote_id": "%s-%s-%s-%s" % (book, snap.isoformat(), market,
                                     selection),
        "provider_event_id": evt,
        "home_team": home,
        "away_team": away,
        "kickoff": snap + timedelta(days=2),
        "book_key": book,
        "market": market,
        "selection": selection,
        "line": line,
        "american_odds": odds,
        "observed_at": snap,
    }


def _two_sided(quotes, book, snap, market, s1, o1, s2, o2, line= -3.0,
               home="KC", away="BUF", evt="evt-1"):
    quotes.append(_q(book, snap, market, s1, line, o1, home, away, evt))
    quotes.append(_q(book, snap, market, s2, line, o2, home, away, evt))


class TestTournamentCore(unittest.TestCase):
    def _three_book_quotes(self):
        """Two snapshots x three books, all two-sided at one line."""
        s1 = T0
        s2 = T0 + timedelta(days=3)
        quotes = []
        # snapshot 1: books slightly off each other
        _two_sided(quotes, "pinnacle", s1, "FULL_GAME_SPREAD",
                   "KC", -110, "BUF", -110)
        _two_sided(quotes, "draftkings", s1, "FULL_GAME_SPREAD",
                   "KC", -105, "BUF", -115)
        _two_sided(quotes, "fanduel", s1, "FULL_GAME_SPREAD",
                   "KC", -115, "BUF", -105)
        # closing snapshot: all at -110
        for b in ("pinnacle", "draftkings", "fanduel"):
            _two_sided(quotes, b, s2, "FULL_GAME_SPREAD",
                       "KC", -110, "BUF", -110)
        return quotes

    def test_lobo_cells_and_exclusion(self):
        quotes = self._three_book_quotes()
        results, cells = run_tournament(quotes)
        # 3 held-out books x 1 prediction snapshot = 3 cells
        self.assertEqual(len(cells), 3)
        self.assertEqual(results["n_cells"], 3)
        self.assertEqual(results["n_events"], 1)
        books = sorted(c["book"] for c in cells)
        self.assertEqual(books, ["draftkings", "fanduel", "pinnacle"])
        for c in cells:
            # closing consensus from the OTHER two books only
            self.assertEqual(c["n_consensus_books"], 2)
            self.assertIn(c["book"], ("pinnacle", "draftkings", "fanduel"))
        # symmetric books: multiplicative fair prob is 0.5 everywhere,
        # so SEs are 0 for the -110 books
        pin = [c for c in cells if c["book"] == "pinnacle"][0]
        self.assertAlmostEqual(pin["se"]["multiplicative"], 0.0, places=12)

    def test_target_book_excluded_from_own_consensus(self):
        """A book with a wildly off quote must not validate itself: its
        closing pair must not enter its own consensus."""
        s1, s2 = T0, T0 + timedelta(days=3)
        quotes = []
        _two_sided(quotes, "pinnacle", s1, "FULL_GAME_SPREAD",
                   "KC", -110, "BUF", -110)
        _two_sided(quotes, "draftkings", s1, "FULL_GAME_SPREAD",
                   "KC", -110, "BUF", -110)
        # fanduel off-market at s1 too, so its SE is nonzero iff its own
        # off-market close does NOT leak into its consensus
        _two_sided(quotes, "fanduel", s1, "FULL_GAME_SPREAD",
                   "KC", -300, "BUF", 200)
        # close: two books at -110, fanduel wildly off at -300/+200
        for b in ("pinnacle", "draftkings"):
            _two_sided(quotes, b, s2, "FULL_GAME_SPREAD",
                       "KC", -110, "BUF", -110)
        _two_sided(quotes, "fanduel", s2, "FULL_GAME_SPREAD",
                   "KC", -300, "BUF", 200)
        _, cells = run_tournament(quotes)
        fd = [c for c in cells if c["book"] == "fanduel"][0]
        # fanduel's own off-market close must not be in its consensus:
        # consensus from pinnacle+draftkings is exactly 0.5
        self.assertAlmostEqual(fd["consensus"]["multiplicative"], 0.5,
                               places=9)
        self.assertGreater(fd["se"]["multiplicative"], 0.0)

    def test_single_sided_book_excluded(self):
        """A book with only one side at a snapshot forms no pair and is
        excluded from cells (and from others' consensus at the close)."""
        s1, s2 = T0, T0 + timedelta(days=3)
        quotes = []
        _two_sided(quotes, "pinnacle", s1, "FULL_GAME_SPREAD",
                   "KC", -110, "BUF", -110)
        _two_sided(quotes, "draftkings", s1, "FULL_GAME_SPREAD",
                   "KC", -110, "BUF", -110)
        # fanduel single-sided at s1: no pair -> no cell as held-out book
        quotes.append(_q("fanduel", s1, "FULL_GAME_SPREAD", "KC", -3.0, -110))
        for b in ("pinnacle", "draftkings", "fanduel"):
            _two_sided(quotes, b, s2, "FULL_GAME_SPREAD",
                       "KC", -110, "BUF", -110)
        results, cells = run_tournament(quotes)
        self.assertEqual(sorted(c["book"] for c in cells),
                         ["draftkings", "pinnacle"])
        for c in cells:
            self.assertEqual(c["n_consensus_books"], 2)

    def test_totals_use_over_as_reference(self):
        s1, s2 = T0, T0 + timedelta(days=3)
        quotes = []
        for b in ("pinnacle", "draftkings", "fanduel"):
            _two_sided(quotes, b, s1, "FULL_GAME_TOTAL",
                       "Over", -110, "Under", -110, line=47.5)
            _two_sided(quotes, b, s2, "FULL_GAME_TOTAL",
                       "Over", -110, "Under", -110, line=47.5)
        _, cells = run_tournament(quotes)
        self.assertEqual(len(cells), 3)
        for c in cells:
            self.assertAlmostEqual(c["f"]["multiplicative"], 0.5, places=9)

    def test_decision_no_significant_difference(self):
        quotes = self._three_book_quotes()
        results, _ = run_tournament(quotes)
        # only one event -> paired test has n=1 -> p_raw None -> no winner
        self.assertEqual(results["decision"], "no method significantly better")
        for p in results["pairwise_tests"]:
            self.assertIsNone(p["p_raw"])

    def test_holm_bonferroni(self):
        adj = _holm_bonferroni([("a", 0.01), ("b", 0.02), ("c", 0.03)])
        self.assertAlmostEqual(adj["a"], 0.03)
        self.assertAlmostEqual(adj["b"], 0.04)
        self.assertAlmostEqual(adj["c"], 0.04)
        # monotonicity: adjusted p-values are non-decreasing in raw p
        self.assertLessEqual(adj["a"], adj["b"])
        self.assertLessEqual(adj["b"], adj["c"])

    def test_paired_test_known(self):
        mean, z, p = _paired_test([1.0] * 50 + [3.0] * 50)
        self.assertAlmostEqual(mean, 2.0)
        self.assertGreater(z, 10)
        self.assertLess(p, 1e-10)
        self.assertIsNone(_paired_test([1.0]))


if __name__ == "__main__":
    unittest.main()
