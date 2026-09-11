"""Tests for the leave-one-book-out fair-pricing engine."""
import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.lobo import best_price_across_books, lobo_signal
from model.research.market_features import implied_prob

T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
KICKOFF = T0 + timedelta(days=3)
EVT = "evt-1"


def q(book, mins_after_t0, line, odds, selection="HOME"):
    return {
        "quote_id": "%s-%d-%s" % (book, mins_after_t0, selection),
        "provider_event_id": EVT,
        "home_team": "BAL",
        "away_team": "IND",
        "kickoff": KICKOFF,
        "sportsbook": book,
        "book_key": book,
        "market": "FULL_GAME_SPREAD",
        "selection": selection,
        "line": line,
        "american_odds": odds,
        "fair_probability": 0.5,
        "observed_at": T0 + timedelta(minutes=mins_after_t0),
        "collected_at": T0 + timedelta(minutes=mins_after_t0),
    }


def three_book():
    # FD and MGM both -110/-110 (no-vig HOME = 0.5). DK is off-market at
    # -105 on HOME -- and must NOT contaminate its own consensus.
    return [
        q("fanduel", 0, -3.0, -110, "HOME"),
        q("fanduel", 0, 3.0, -110, "AWAY"),
        q("betmgm", 0, -3.0, -110, "HOME"),
        q("betmgm", 0, 3.0, -110, "AWAY"),
        q("draftkings", 0, -3.0, -105, "HOME"),
        q("draftkings", 0, 3.0, -115, "AWAY"),
    ]


def sig(quotes, as_of, target):
    return lobo_signal(quotes, as_of, EVT, "FULL_GAME_SPREAD", "HOME", -3.0,
                       target)


class ExclusionTests(unittest.TestCase):
    def test_target_excluded_from_own_consensus(self):
        s = sig(three_book(), T0 + timedelta(minutes=10), "draftkings")
        # Consensus from FD + MGM only: exactly 0.5.
        self.assertAlmostEqual(s["consensus_prob"], 0.5)
        self.assertAlmostEqual(s["fair_prob"], 0.5)
        self.assertEqual(s["n_books_used"], 2)
        self.assertEqual(s["books"], ["betmgm", "fanduel"])
        self.assertNotIn("draftkings", s["books"])
        self.assertFalse(s["insufficient_data"])

    def test_lobo_differs_from_naive_include_target(self):
        # Naive consensus including DK's shaded price would sit above 0.5
        # for DK's own evaluation; LOBO must not.
        s = sig(three_book(), T0 + timedelta(minutes=10), "draftkings")
        self.assertLess(s["consensus_prob"], 0.51)

    def test_point_in_time(self):
        quotes = three_book() + [q("fanduel", 60, -3.0, -120, "HOME"),
                                 q("fanduel", 60, 3.0, 100, "AWAY")]
        s = sig(quotes, T0 + timedelta(minutes=10), "draftkings")
        self.assertAlmostEqual(s["consensus_prob"], 0.5)


class EdgeSignTests(unittest.TestCase):
    def test_generous_offer_positive_edge(self):
        # DK offers +120 (implied 0.4545) vs fair 0.5 -> positive edge.
        quotes = [qq for qq in three_book()
                  if not (qq["book_key"] == "draftkings"
                          and qq["selection"] == "HOME")]
        quotes.append(q("draftkings", 0, -3.0, 120, "HOME"))
        s = sig(quotes, T0 + timedelta(minutes=10), "draftkings")
        self.assertAlmostEqual(s["offered_prob"], implied_prob(120))
        self.assertGreater(s["edge_pp"], 0)
        self.assertAlmostEqual(s["edge_pp"],
                               (0.5 - implied_prob(120)) * 100.0)

    def test_shaded_offer_negative_edge(self):
        # DK -105 (implied 0.5122) vs fair 0.5 -> negative edge.
        s = sig(three_book(), T0 + timedelta(minutes=10), "draftkings")
        self.assertLess(s["edge_pp"], 0)
        self.assertAlmostEqual(s["edge_pp"],
                               (0.5 - implied_prob(-105)) * 100.0)


class InsufficientDataTests(unittest.TestCase):
    def test_fewer_than_two_other_books(self):
        quotes = [qq for qq in three_book()
                  if qq["book_key"] in ("draftkings", "fanduel")]
        s = sig(quotes, T0 + timedelta(minutes=10), "draftkings")
        self.assertTrue(s["insufficient_data"])
        self.assertIsNone(s["edge_pp"])
        self.assertIsNone(s["consensus_prob"])

    def test_one_sided_book_does_not_count(self):
        # MGM only posts HOME (no AWAY) -> cannot form no-vig.
        quotes = [qq for qq in three_book()
                  if not (qq["book_key"] == "betmgm"
                          and qq["selection"] == "AWAY")]
        s = sig(quotes, T0 + timedelta(minutes=10), "draftkings")
        self.assertTrue(s["insufficient_data"])


class FreshnessTests(unittest.TestCase):
    def test_fresh(self):
        s = sig(three_book(), T0 + timedelta(minutes=10), "draftkings")
        self.assertTrue(s["fresh"])
        self.assertAlmostEqual(s["quote_age_seconds"], 600.0)

    def test_stale_contributor(self):
        s = sig(three_book(), T0 + timedelta(minutes=45), "draftkings")
        self.assertFalse(s["fresh"])

    def test_signal_at_iso(self):
        as_of = T0 + timedelta(minutes=10)
        s = sig(three_book(), as_of, "draftkings")
        self.assertEqual(s["signal_at"], as_of.isoformat())


class BestPriceTests(unittest.TestCase):
    def test_picks_lowest_implied(self):
        b = best_price_across_books(three_book(), T0 + timedelta(minutes=10),
                                    EVT, "FULL_GAME_SPREAD", "HOME", -3.0)
        # -105 (0.5122) beats -110 (0.5238).
        self.assertEqual(b["book_key"], "draftkings")
        self.assertEqual(b["american_odds"], -105)
        self.assertAlmostEqual(b["implied_prob"], implied_prob(-105))

    def test_none_when_empty(self):
        self.assertIsNone(best_price_across_books(
            [], T0, EVT, "FULL_GAME_SPREAD", "HOME", -3.0))


if __name__ == "__main__":
    unittest.main()
