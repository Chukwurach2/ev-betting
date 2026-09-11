"""Tests for the historical backfill: schedule math and snapshot normalization."""
import datetime as dt
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import backfill_history as bh


def _envelope():
    return {
        "timestamp": "2024-09-11T12:00:00Z",
        "data": [
            {"id": "g1", "home_team": "Baltimore Ravens",
             "away_team": "Kansas City Chiefs",
             "commence_time": "2024-09-15T17:00:00Z",
             "bookmakers": [
                 {"key": "pinnacle", "title": "Pinnacle",
                  "markets": [
                      {"key": "spreads",
                       "outcomes": [
                           {"name": "Baltimore Ravens", "price": -105,
                            "point": -2.5},
                           {"name": "Kansas City Chiefs", "price": -115,
                            "point": 2.5}]},
                      {"key": "totals",
                       "outcomes": [
                           {"name": "Over", "price": -110, "point": 47.5},
                           {"name": "Under", "price": -110, "point": 47.5}]}]},
                 {"key": "draftkings", "title": "DraftKings",
                  "markets": [
                      {"key": "spreads",
                       "outcomes": [
                           {"name": "Baltimore Ravens", "price": -110,
                            "point": -3.0},
                           {"name": "Kansas City Chiefs", "price": -110,
                            "point": 3.0}]}]},
             ]},
        ],
    }


class ScheduleTests(unittest.TestCase):
    def test_three_snapshots_per_week(self):
        times = bh.snapshot_times(2024, 1)
        self.assertEqual(len(times), 3)
        # Week 1 2024: Sunday Sep 8 -> Wed Sep 4, Sat Sep 7, Sun Sep 8.
        self.assertEqual(
            [t.strftime("%Y-%m-%d %H:%M") for t in times],
            ["2024-09-04 12:00", "2024-09-07 12:00", "2024-09-08 15:30"])
        self.assertTrue(all(t.tzinfo is not None for t in times))

    def test_close_snapshot_before_kickoff(self):
        # Sunday 15:30 UTC is before the 17:00 UTC (1pm ET) slate.
        times = bh.snapshot_times(2023, 5)
        self.assertLess(times[2].hour * 60 + times[2].minute, 17 * 60)

    def test_parse_weeks(self):
        self.assertEqual(bh.parse_weeks("1-18"), list(range(1, 19)))
        self.assertEqual(bh.parse_weeks("5"), [5])

    def test_unsupported_season_rejected(self):
        with self.assertRaises(SystemExit):
            bh.main(["--seasons", "1999", "--dry-run"])


class NormalizeTests(unittest.TestCase):
    def test_quotes_paired_with_novig(self):
        qs = bh.normalize_snapshot(_envelope(), "us,eu", "spreads,totals")
        # pinnacle spread pair (2) + pinnacle total pair (2) + DK spread (2)
        self.assertEqual(len(qs), 6)
        by_key = {(q["book_key"], q["market"], q["selection"]): q for q in qs}
        q = by_key[("pinnacle", "FULL_GAME_SPREAD", "Baltimore Ravens")]
        self.assertEqual(q["line"], 2.5)
        self.assertEqual(q["observed_at"], "2024-09-11T12:00:00Z")
        self.assertFalse(q["ny_licensed"])  # pinnacle is signal-only
        dk = by_key[("draftkings", "FULL_GAME_SPREAD", "Kansas City Chiefs")]
        self.assertTrue(dk["ny_licensed"])
        # No-vig: the two sides of a pair sum to 1.
        pair = [by_key[("pinnacle", "FULL_GAME_SPREAD", s)]["fair_probability"]
                for s in ("Baltimore Ravens", "Kansas City Chiefs")]
        self.assertAlmostEqual(sum(pair), 1.0, places=9)
        tot = by_key[("pinnacle", "FULL_GAME_TOTAL", "Over")]
        self.assertEqual(tot["line"], 47.5)

    def test_quote_ids_deterministic(self):
        a = bh.normalize_snapshot(_envelope(), "us,eu", "spreads,totals")
        b = bh.normalize_snapshot(_envelope(), "us,eu", "spreads,totals")
        self.assertEqual([q["quote_id"] for q in a],
                         [q["quote_id"] for q in b])

    def test_empty_envelope(self):
        self.assertEqual(bh.normalize_snapshot({"data": []}, "us", "spreads"),
                         [])


if __name__ == "__main__":
    unittest.main()
