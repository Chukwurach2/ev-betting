"""Tests for the market-alpha v1 pure core (lead/lag + stale tracks).

Follows the import pattern of test_devig.py: evict any cached top-level
``research`` module (test_challenger puts nfl-edge/model on sys.path)
so the bare name resolves to nfl-edge/research.
"""
import math
import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

for _mod in [m for m in sys.modules if m == "research" or m.startswith("research.")]:
    del sys.modules[_mod]

from research.market_alpha import (  # noqa: E402
    FOLLOWERS,
    PINNACLE,
    analyze_lead_lag,
    analyze_stale,
    binomial_one_sided_ge,
    build_game_series,
    decide_track_a,
    decide_track_b,
    directional_lag_units,
    lead_lag_events,
    one_sample_one_sided,
    stale_cells,
)

UTC = timezone.utc


def odds_from_prob(p):
    """American odds with implied probability p (exact inverse of implied_prob)."""
    if p >= 0.5:
        return -round(100.0 * p / (1.0 - p))
    return round(100.0 * (1.0 - p) / p)


def pair_quotes(game, market, snap, book, f_ref, line=0.0, overround=1.05):
    """Two-sided quotes giving multiplicative no-vig fair prob ~f_ref."""
    home, away, _kickoff = game
    p_ref = f_ref * overround
    p_other = (1.0 - f_ref) * overround
    if market == "FULL_GAME_SPREAD":
        ref_sel, other_sel = home, away
    else:
        ref_sel, other_sel = "Over", "Under"
    base = {
        "provider_event_id": "pid",
        "home_team": home,
        "away_team": away,
        "kickoff": game[2],
        "book_key": book,
        "market": market,
        "line": line,
    }
    q1 = dict(base, selection=ref_sel,
              american_odds=odds_from_prob(p_ref), observed_at=snap)
    q2 = dict(base, selection=other_sel,
              american_odds=odds_from_prob(p_other), observed_at=snap)
    return [q1, q2]


def make_game(idx, markets=("FULL_GAME_SPREAD",),
              book_probs=None, n_snaps=4):
    """Synthetic game. book_probs: {book: [f per snapshot]} (pad with last)."""
    home, away = "Home%d" % idx, "Away%d" % idx
    kickoff = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
    snaps = [datetime(2024, 9, 4, 12, 0, tzinfo=UTC) + timedelta(days=d)
             for d in (0, 3, 4, 6)][:n_snaps]
    quotes = []
    for market in markets:
        for book, probs in (book_probs or {}).items():
            for i, s in enumerate(snaps):
                f = probs[min(i, len(probs) - 1)]
                quotes.extend(pair_quotes((home, away, kickoff), market, s,
                                          book, f))
    return quotes


class TestBinomial(unittest.TestCase):
    def test_exact_values(self):
        self.assertAlmostEqual(binomial_one_sided_ge(10, 10), 1.0 / 1024)
        self.assertAlmostEqual(binomial_one_sided_ge(0, 10), 1.0)
        # P(X>=5) for n=10: (252+210+120+45+10+1)/1024
        self.assertAlmostEqual(binomial_one_sided_ge(5, 10),
                               638.0 / 1024, places=9)

    def test_degenerate(self):
        self.assertIsNone(binomial_one_sided_ge(0, 0))
        self.assertAlmostEqual(binomial_one_sided_ge(11, 10), 0.0)


class TestOneSample(unittest.TestCase):
    def test_significant_positive(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0] * 20
        mean, sd, n, z, p = one_sample_one_sided(vals)
        self.assertAlmostEqual(mean, 3.0)
        self.assertEqual(n, 100)
        self.assertLess(p, 1e-10)

    def test_null_centered(self):
        vals = [1.0, -1.0] * 50
        mean, sd, n, z, p = one_sample_one_sided(vals)
        self.assertAlmostEqual(mean, 0.0)
        self.assertAlmostEqual(p, 0.5, places=6)

    def test_too_few(self):
        self.assertIsNone(one_sample_one_sided([1.0]))


class TestLeadLagCore(unittest.TestCase):
    def _following_world(self, n_games=40):
        # Pinnacle jumps +0.05 at t1; DK follows at t2. FD never responds.
        quotes = []
        for i in range(n_games):
            quotes += make_game(i, book_probs={
                PINNACLE: [0.50, 0.55, 0.55, 0.55],
                "draftkings": [0.50, 0.50, 0.54, 0.54],
                "fanduel": [0.50, 0.50, 0.50, 0.50],
            })
        return build_game_series(quotes)

    def test_move_detected_and_agreement(self):
        gs = self._following_world(n_games=5)
        events = lead_lag_events(gs, followers=("draftkings", "fanduel"))
        # one Pinnacle move per game (t0->t1); needs follower data t1,t2
        self.assertEqual(len(events), 5)
        for e in events:
            self.assertAlmostEqual(e["leader_move"], 0.05, places=2)
            self.assertEqual(e["direction"], 1)
            self.assertTrue(e["followers"]["draftkings"]["agree"])
            # FD delta = 0 < resp_min -> no response, excluded
            self.assertIsNone(e["followers"]["fanduel"]["agree"])

    def test_catchup_fraction(self):
        gs = self._following_world(n_games=3)
        events = lead_lag_events(gs, followers=("draftkings",))
        c = events[0]["followers"]["draftkings"]["catchup"]
        # 1 - |0.54-0.55| / |0.50-0.55| = 1 - 0.2 = 0.8
        self.assertAlmostEqual(c, 0.8, places=1)

    def test_analysis_significant(self):
        gs = self._following_world(n_games=40)
        events = lead_lag_events(gs, followers=("draftkings",))
        an = analyze_lead_lag(events, followers=("draftkings",))
        dk = an["books"]["draftkings"]
        self.assertTrue(dk["feasible"])
        self.assertEqual(dk["n_events"], 40)
        self.assertEqual(dk["n_agree"], 40)
        self.assertLess(dk["p_raw"], 1e-9)
        self.assertTrue(dk["significant"])  # single-test Holm = raw
        self.assertTrue(an["pooled"]["significant"])

    def test_infeasible_when_too_few_events(self):
        gs = self._following_world(n_games=5)
        events = lead_lag_events(gs, followers=("draftkings",))
        an = analyze_lead_lag(events, followers=("draftkings",))
        dk = an["books"]["draftkings"]
        self.assertFalse(dk["feasible"])
        self.assertIsNone(dk["p_raw"])

    def test_random_follower_not_significant(self):
        # Follower moves randomly relative to Pinnacle moves.
        import random
        random.seed(7)
        quotes = []
        for i in range(60):
            dk = [0.50]
            for _ in range(3):
                dk.append(dk[-1] + random.choice([-0.03, 0.03]))
            quotes += make_game(i, book_probs={
                PINNACLE: [0.50, 0.55, 0.55, 0.55],
                "draftkings": dk,
            })
        gs = build_game_series(quotes)
        events = lead_lag_events(gs, followers=("draftkings",))
        an = analyze_lead_lag(events, followers=("draftkings",))
        dk = an["books"]["draftkings"]
        self.assertTrue(dk["feasible"])
        self.assertFalse(dk["significant"])


class TestDecideTrackA(unittest.TestCase):
    def _analysis(self, sig_books, pooled_sig=True, rev_sig=()):
        books = {b: {"significant": b in sig_books} for b in FOLLOWERS}
        analysis = {"books": books, "pooled": {"significant": pooled_sig}}
        reverse = {"books": {b: {"significant": b in rev_sig,
                                 "p_raw": 0.01 if b in rev_sig else 0.9}
                             for b in FOLLOWERS}}
        return analysis, reverse

    def test_edge_requires_all_conditions(self):
        a, r = self._analysis(set(FOLLOWERS[:3]))
        v, d = decide_track_a(a, r)
        self.assertEqual(v, "lead_lag_edge")

    def test_failed_falsification_blocks(self):
        a, r = self._analysis(set(FOLLOWERS[:4]), rev_sig=(FOLLOWERS[0],))
        v, d = decide_track_a(a, r)
        self.assertEqual(v, "mixed_inconclusive")
        self.assertEqual(d["reverse_significant_books"], [FOLLOWERS[0]])

    def test_nothing_significant(self):
        a, r = self._analysis(set(), pooled_sig=False)
        v, _ = decide_track_a(a, r)
        self.assertEqual(v, "no_lead_lag_signal")

    def test_partial_is_inconclusive(self):
        a, r = self._analysis({FOLLOWERS[0]})
        v, _ = decide_track_a(a, r)
        self.assertEqual(v, "mixed_inconclusive")


class TestStaleCore(unittest.TestCase):
    def _stale_world(self, n_games=50, dk_f=0.44, close_f=0.52):
        # DK stale low at s0/s1 (bets ref); everyone converges to close_f.
        quotes = []
        for i in range(n_games):
            quotes += make_game(i, book_probs={
                PINNACLE: [0.50, 0.50, close_f],
                "draftkings": [dk_f, dk_f, close_f],
                "fanduel": [0.50, 0.50, close_f],
            }, n_snaps=3)
        return build_game_series(quotes)

    def test_stale_flag_and_edge(self):
        gs = self._stale_world(n_games=3)
        cells = stale_cells(gs)
        # DK stale at s0 and s1 per game (s2 is the close, excluded)
        dk_cells = [c for c in cells if c["book"] == "draftkings"]
        self.assertEqual(len(dk_cells), 6)
        for c in dk_cells:
            self.assertEqual(c["bet"], "ref")
            self.assertGreater(c["edge"], 0.05)
        # closing snapshot cells are excluded by construction
        latest = max(s for g in gs.values()
                     for s in g["series"]["FULL_GAME_SPREAD"]).isoformat()
        self.assertFalse(any(c["snapshot"] == latest for c in cells))

    def test_no_stale_when_aligned(self):
        quotes = []
        for i in range(3):
            quotes += make_game(i, book_probs={
                PINNACLE: [0.50, 0.50, 0.52],
                "draftkings": [0.50, 0.50, 0.52],
                "fanduel": [0.50, 0.50, 0.52],
            }, n_snaps=3)
        gs = build_game_series(quotes)
        self.assertEqual(stale_cells(gs), [])

    def test_exact_line_consensus(self):
        # Books at different lines must not enter each other's consensus.
        import random
        random.seed(3)
        quotes = []
        for i in range(4):
            home, away = "Home%d" % i, "Away%d" % i
            kickoff = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
            snaps = [datetime(2024, 9, 4, 12, 0, tzinfo=UTC),
                     datetime(2024, 9, 7, 12, 0, tzinfo=UTC),
                     datetime(2024, 9, 8, 12, 0, tzinfo=UTC)]
            # line 0.0: A, B at 0.50; E (evaluated) at 0.44 (deviant)
            # line -2.5: C, D at 0.30 (must not pollute the line-0.0 consensus)
            plan = {"bookA": (0.0, 0.50), "bookB": (0.0, 0.50),
                    "bookC": (-2.5, 0.30), "bookD": (-2.5, 0.30),
                    "bookE": (0.0, 0.44)}
            for s in snaps:
                for b, (line, f) in plan.items():
                    quotes.extend(pair_quotes((home, away, kickoff),
                                              "FULL_GAME_SPREAD", s, b, f,
                                              line=line))
        gs = build_game_series(quotes)
        cells = stale_cells(gs)
        e_cells = [c for c in cells if c["book"] == "bookE"]
        self.assertTrue(e_cells)  # flagged despite the -2.5 books
        for c in e_cells:
            # consensus formed at line 0.0 only: median(0.50, 0.50) = 0.50
            self.assertAlmostEqual(c["consensus_s"], 0.50, places=2)
            self.assertEqual(c["line"], 0.0)

    def test_naive_ttest_rejects_under_pure_noise(self):
        # Locks in the Amendment A1 diagnosis: the preregistered t-test is
        # miscalibrated -- it rejects when every deviation is iid noise
        # (line-shopping selection bias).
        import random
        random.seed(11)
        quotes = []
        for i in range(80):
            probs = {}
            for b in [PINNACLE, "draftkings", "fanduel", "betmgm",
                      "betrivers", "williamhill_us"]:
                probs[b] = [min(0.9, max(0.1, 0.50 + random.gauss(0, 0.01)))
                            for _ in range(3)]
            quotes += make_game(5000 + i, book_probs=probs, n_snaps=3)
        gs = build_game_series(quotes)
        an = analyze_stale(gs)
        self.assertIsNotNone(an["naive_t_p_superseded"])
        self.assertLess(an["naive_t_p_superseded"], 0.05)

    def test_directional_lag_calibrated_under_noise(self):
        # Same noise world: the directional lag test must NOT reject.
        import random
        random.seed(11)
        quotes = []
        for i in range(150):
            base = [0.50, 0.54, 0.52]  # market trends; books track it + noise
            probs = {}
            for b in [PINNACLE, "draftkings", "fanduel", "betmgm",
                      "betrivers", "williamhill_us"]:
                probs[b] = [min(0.9, max(0.1, base[j] + random.gauss(0, 0.01)))
                            for j in range(3)]
            quotes += make_game(5000 + i, book_probs=probs, n_snaps=3)
        gs = build_game_series(quotes)
        an = analyze_stale(gs)
        self.assertGreaterEqual(an["n_units"], 30)
        self.assertGreaterEqual(an["p_lag"], 0.05)
        v, _ = decide_track_b(an)
        self.assertEqual(v, "no_stale_price_signal")

    def test_directional_lag_detects_planted_stale(self):
        # slowbook lags one snapshot behind a trending market.
        quotes = []
        for i in range(40):
            quotes += make_game(6000 + i, book_probs={
                PINNACLE: [0.50, 0.56, 0.58],
                "fanduel": [0.50, 0.56, 0.58],
                "betmgm": [0.50, 0.56, 0.58],
                "betrivers": [0.50, 0.56, 0.58],
                "williamhill_us": [0.50, 0.56, 0.58],
                "slowbook": [0.50, 0.50, 0.56],
            }, n_snaps=3)
        gs = build_game_series(quotes)
        units = directional_lag_units(gs)
        self.assertGreaterEqual(len(units), 30)
        # every flagged unit is slowbook, lagging
        self.assertTrue(all(u["book"] == "slowbook" for u in units))
        self.assertTrue(all(u["lagging"] for u in units))
        an = analyze_stale(gs)
        self.assertLess(an["p_lag"], 1e-6)
        self.assertGreater(an["mean_edge_lagging"], 0.05)
        v, detail = decide_track_b(an)
        self.assertEqual(v, "stale_price_edge")

    def test_evaluated_book_excluded_from_both_consensuses(self):
        # The split-consensus dev uses A (2 books) and the move uses B
        # (2 disjoint books): 4 distinct others, none is the evaluated book.
        quotes = []
        for i in range(2):
            quotes += make_game(7000 + i, book_probs={
                PINNACLE: [0.50, 0.56, 0.58],
                "fanduel": [0.50, 0.56, 0.58],
                "betmgm": [0.50, 0.56, 0.58],
                "betrivers": [0.50, 0.56, 0.58],
                "williamhill_us": [0.50, 0.56, 0.58],
                "slowbook": [0.50, 0.50, 0.56],
            }, n_snaps=3)
        gs = build_game_series(quotes)
        units = directional_lag_units(gs)
        self.assertTrue(units)
        for u in units:
            # dev/D are computed without the evaluated book by construction;
            # spot-check the arithmetic on the first unit (slowbook):
            # A = [betmgm, betrivers] -> median 0.56; dev = 0.50-0.56
            if u["book"] == "slowbook":
                self.assertAlmostEqual(u["dev"], -0.06, places=2)
                self.assertAlmostEqual(u["D"], 0.06, places=2)

    def test_decide_rules(self):
        base = {"n_units": 40, "n_lagging": 30, "lagging_fraction": 0.75,
                "n_games_lagging": 30, "ci95_lagging": [0.02, 0.06],
                "naive_t_p_superseded": 1e-9,
                "naive_t_mean_superseded": 0.04}
        v, _ = decide_track_b(dict(base, p_lag=0.01,
                                   mean_edge_lagging=0.04))
        self.assertEqual(v, "stale_price_edge")
        v, _ = decide_track_b(dict(base, p_lag=0.01,
                                   mean_edge_lagging=0.004))
        self.assertEqual(v, "significant_but_negligible")
        v, _ = decide_track_b(dict(base, p_lag=0.40,
                                   mean_edge_lagging=0.04))
        self.assertEqual(v, "no_stale_price_signal")
        v, d = decide_track_b(dict(base, p_lag=0.40,
                                   mean_edge_lagging=0.04, n_units=12))
        self.assertEqual(v, "infeasible")


if __name__ == "__main__":
    unittest.main()
