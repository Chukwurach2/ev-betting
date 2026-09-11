"""Tests for the point-in-time market feature builder."""
import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.market_features import (FEATURE_VERSION, build_features,
                                            feature_names, implied_prob,
                                            novig_prob)

T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
KICKOFF = T0 + timedelta(days=3)
EVT = "evt-1"


def q(book, mins_after_t0, line, odds, selection="HOME", t0=T0):
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
        "observed_at": t0 + timedelta(minutes=mins_after_t0),
        "collected_at": t0 + timedelta(minutes=mins_after_t0),
    }


def base_quotes():
    # Two-sided DK/FD market, DK moves at +60, FD moves at +90.
    return [
        q("draftkings", 0, -3.0, -110, "HOME"),
        q("draftkings", 0, 3.0, -110, "AWAY"),
        q("fanduel", 0, -3.0, -110, "HOME"),
        q("fanduel", 0, 3.0, -110, "AWAY"),
        q("draftkings", 60, -3.5, -110, "HOME"),
        q("draftkings", 60, 3.5, -110, "AWAY"),
        q("fanduel", 90, -3.5, -105, "HOME"),
        q("fanduel", 90, 3.5, -115, "AWAY"),
    ]


def feats(quotes, as_of, **kw):
    return build_features(quotes, as_of, EVT, "FULL_GAME_SPREAD", "HOME",
                          -3.0, **kw)


class ConversionTests(unittest.TestCase):
    def test_implied(self):
        self.assertAlmostEqual(implied_prob(-110), 110 / 210)
        self.assertAlmostEqual(implied_prob(150), 100 / 250)
        self.assertIsNone(implied_prob(0))
        self.assertIsNone(implied_prob(None))

    def test_novig(self):
        self.assertAlmostEqual(novig_prob(110 / 210, 110 / 210), 0.5)
        # -110/-110 hold: raw sum 1.0476 -> no-vig 0.5 each.
        self.assertAlmostEqual(novig_prob(0.5238, 0.5238), 0.5)
        self.assertIsNone(novig_prob(None, 0.5))


class PointInTimeTests(unittest.TestCase):
    def test_later_quote_never_changes_earlier_features(self):
        as_of = T0 + timedelta(minutes=30)
        f_before = feats(base_quotes(), as_of)
        # Extra quotes observed AFTER as_of must not leak in.
        extra = base_quotes() + [q("draftkings", 120, -4.0, -110, "HOME")]
        f_after = feats(extra, as_of)
        self.assertEqual(f_before, f_after)

    def test_as_of_string_parses(self):
        f = feats(base_quotes(), (T0 + timedelta(minutes=30)).isoformat())
        self.assertEqual(f["opener_line"], -3.0)

    def test_empty_quotes_graceful(self):
        f = feats([], T0 + timedelta(minutes=30))
        for name in feature_names():
            self.assertIsNone(f[name])
        self.assertEqual(f["feature_version"], FEATURE_VERSION)


class MovementTests(unittest.TestCase):
    def test_opener_current_move(self):
        f = feats(base_quotes(), T0 + timedelta(minutes=150))
        self.assertEqual(f["opener_line"], -3.0)
        self.assertEqual(f["opener_price"], -110)
        self.assertEqual(f["current_line"], -3.5)
        self.assertEqual(f["current_price"], -105)
        self.assertAlmostEqual(f["line_move"], -0.5)
        self.assertEqual(f["price_move"], 5)
        self.assertEqual(f["move_direction"], -1)
        # -0.5 over 1.5h
        self.assertAlmostEqual(f["move_velocity_per_hour"], -0.5 / 1.5)
        self.assertAlmostEqual(f["minutes_since_opener"], 150.0)
        self.assertAlmostEqual(f["minutes_to_kickoff"],
                               (KICKOFF - (T0 + timedelta(minutes=150)))
                               .total_seconds() / 60.0)

    def test_static_move_flags(self):
        # Line moved >= 0.5 with price static on DK's series.
        dk_only = [qq for qq in base_quotes()
                   if qq["book_key"] == "draftkings"
                   and qq["selection"] == "HOME"]
        f = feats(dk_only, T0 + timedelta(minutes=150),
                  target_book="draftkings")
        self.assertTrue(f["line_moved_price_static"])
        self.assertFalse(f["price_moved_line_static"])
        # Price-only move on FD: line static, price moves.
        fd_only = [q("fanduel", 0, -3.0, -110, "HOME"),
                   q("fanduel", 90, -3.0, -105, "HOME")]
        f2 = feats(fd_only, T0 + timedelta(minutes=150),
                   target_book="fanduel")
        self.assertFalse(f2["line_moved_price_static"])
        self.assertTrue(f2["price_moved_line_static"])


class MarketStructureTests(unittest.TestCase):
    def test_books_dispersion_consensus(self):
        f = feats(base_quotes(), T0 + timedelta(minutes=150))
        self.assertEqual(f["n_books"], 2)
        # Latest lines: DK -3.5, FD -3.5 -> zero dispersion.
        self.assertAlmostEqual(f["line_dispersion"], 0.0)
        # No-vig consensus: median of DK 0.5 and FD no-vig(-105/-115).
        fd_nv = novig_prob(implied_prob(-105), implied_prob(-115))
        self.assertAlmostEqual(f["novig_consensus_prob"],
                               (0.5 + fd_nv) / 2.0)
        self.assertAlmostEqual(f["dk_fd_line_gap"], 0.0)
        # FD -105 vs DK -110.
        self.assertAlmostEqual(
            f["dk_fd_prob_gap"],
            implied_prob(-110) - implied_prob(-105))
        # Best price = FD -105 (lowest implied).
        self.assertEqual(f["best_price"], -105)
        # No-vig fair (0.4946) sits below even the best vigged price, so the
        # best available price is still short of fair: negative.
        self.assertLess(f["best_price_vs_consensus_pp"], 0)
        # Hold from two-sided FD (target book): 105/205 + 115/215 - 1.
        f_tgt = feats(base_quotes(), T0 + timedelta(minutes=150),
                      target_book="fanduel")
        self.assertAlmostEqual(
            f_tgt["hold_pct"],
            (implied_prob(-105) + implied_prob(-115) - 1.0) * 100.0)

    def test_single_book_graceful(self):
        one = [qq for qq in base_quotes()
               if qq["book_key"] == "draftkings"]
        f = feats(one, T0 + timedelta(minutes=150))
        self.assertEqual(f["n_books"], 1)
        self.assertIsNone(f["line_dispersion"])
        self.assertIsNone(f["dk_fd_line_gap"])
        # Only one two-sided book -> median of one still works.
        self.assertAlmostEqual(f["novig_consensus_prob"], 0.5)


class TargetBookTests(unittest.TestCase):
    def test_stale_flag(self):
        # FD quote is old; DK moved within the last 15 minutes.
        quotes = [
            q("draftkings", 0, -3.0, -110, "HOME"),
            q("draftkings", 0, 3.0, -110, "AWAY"),
            q("fanduel", 0, -3.0, -110, "HOME"),
            q("fanduel", 0, 3.0, -110, "AWAY"),
            q("draftkings", 50, -3.5, -110, "HOME"),
            q("draftkings", 50, 3.5, -110, "AWAY"),
        ]
        as_of = T0 + timedelta(minutes=60)
        f = feats(quotes, as_of, target_book="fanduel")
        self.assertTrue(f["stale_flag"])
        self.assertAlmostEqual(f["quote_age_seconds"], 3600.0)
        # DK is fresh -> not stale.
        f2 = feats(quotes, as_of, target_book="draftkings")
        self.assertFalse(f2["stale_flag"])

    def test_not_stale_when_nothing_moved(self):
        quotes = [q("draftkings", 0, -3.0, -110, "HOME"),
                  q("fanduel", 0, -3.0, -110, "HOME")]
        f = feats(quotes, T0 + timedelta(minutes=60), target_book="fanduel")
        self.assertFalse(f["stale_flag"])

    def test_lead_lag(self):
        f = feats(base_quotes(), T0 + timedelta(minutes=150),
                  target_book="draftkings")
        # DK moved at +60, FD at +90 -> DK led.
        self.assertTrue(f["is_leading"])
        self.assertFalse(f["is_lagging"])
        f2 = feats(base_quotes(), T0 + timedelta(minutes=150),
                   target_book="fanduel")
        self.assertFalse(f2["is_leading"])
        self.assertTrue(f2["is_lagging"])

    def test_missing_target_book_graceful(self):
        f = feats(base_quotes(), T0 + timedelta(minutes=150),
                  target_book="betmgm")
        self.assertIsNone(f["quote_age_seconds"])
        self.assertIsNone(f["stale_flag"])
        self.assertIsNone(f["is_leading"])


if __name__ == "__main__":
    unittest.main()
