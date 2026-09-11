"""Tests for book-universe and region wiring in the odds provider."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model import provider_oddsapi as pov


def _event(books):
    return {
        "id": "evt-1",
        "bookmakers": [
            {"key": key, "title": key.title(),
             "markets": [{"key": "spreads", "last_update": "2026-09-11T12:00:00Z",
                          "outcomes": [
                              {"name": "Baltimore Ravens", "price": -110,
                               "point": -3.0},
                              {"name": "Indianapolis Colts", "price": -110,
                               "point": 3.0}]}]}
            for key in books
        ],
    }


class NormalizeBooksTests(unittest.TestCase):
    def test_default_keeps_ny_books_only(self):
        q = pov.normalize(_event(["draftkings", "pinnacle", "betmgm"]))
        keys = {x.sportsbook_key for x in q}
        self.assertIn("draftkings", keys)
        self.assertIn("betmgm", keys)
        self.assertNotIn("pinnacle", keys)

    def test_explicit_allowlist_includes_signal_books(self):
        q = pov.normalize(_event(["draftkings", "pinnacle"]),
                          allowed_books={"draftkings", "pinnacle"})
        keys = {x.sportsbook_key for x in q}
        self.assertEqual(keys, {"draftkings", "pinnacle"})

    def test_paid_ny_books_in_default_set(self):
        self.assertIn("williamhill_us", pov.NY_BOOK_KEYS)
        self.assertIn("fanatics", pov.NY_BOOK_KEYS)
        self.assertIn("pinnacle", pov.SIGNAL_ONLY_BOOKS)
        self.assertNotIn("pinnacle", pov.NY_BOOK_KEYS)


class RegionsTests(unittest.TestCase):
    def test_fetch_event_odds_sends_regions(self):
        captured = {}

        def fake_get(url, params, timeout):
            captured.update(params)
            return {}, {}

        old = pov._get
        pov._get = fake_get
        try:
            pov.fetch_event_odds("e1", ["spreads", "totals"],
                                 api_key="k", bookmakers=["draftkings"],
                                 regions="us,eu")
        finally:
            pov._get = old
        self.assertEqual(captured["regions"], "us,eu")
        self.assertEqual(captured["bookmakers"], "draftkings")
        self.assertEqual(captured["markets"], "spreads,totals")

    def test_regions_default_is_us(self):
        captured = {}

        def fake_get(url, params, timeout):
            captured.update(params)
            return {}, {}

        old = pov._get
        pov._get = fake_get
        try:
            pov.fetch_event_odds("e1", ["spreads"], api_key="k")
        finally:
            pov._get = old
        self.assertEqual(captured["regions"], "us")


if __name__ == "__main__":
    unittest.main()
