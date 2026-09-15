"""Unit tests for ops/market_outcomes.py (pure logic; no DB, no API)."""
import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ops"))
import market_outcomes as mo


def _game(**kw):
    g = {"season": 2024, "homeTeam": "Ohio State", "awayTeam": "Michigan",
         "startDate": "2024-11-30T17:00:00.000Z", "completed": True,
         "homePoints": 24, "awayPoints": 20,
         "homeConference": "Big Ten", "awayConference": "Big Ten",
         "homeClassification": "fbs", "awayClassification": "fbs"}
    g.update(kw)
    return g


class TestMatching(unittest.TestCase):
    def setUp(self):
        games = [_game()]
        self.idx = {}
        for g in games:
            key = (g["season"], mo.norm_name(g["homeTeam"]),
                   mo.norm_name(g["awayTeam"]))
            self.idx.setdefault(key, []).append(g)

    def test_exact_match(self):
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, self.idx)
        self.assertIn(("e1", 2024), matched)
        self.assertEqual(integ["matched"], 1)

    def test_name_normalization(self):
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("  OHIO-state!! ", "michigan", ko)}
        matched, _ = mo.match_events(events, self.idx)
        self.assertIn(("e1", 2024), matched)

    def test_kickoff_outside_tolerance(self):
        ko = dt.datetime(2024, 12, 5, 17, 0, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, self.idx)
        self.assertNotIn(("e1", 2024), matched)
        self.assertEqual(integ["no_candidate"], 1)

    def test_incomplete_game_excluded(self):
        g = _game(completed=False)
        idx = {(2024, "ohiostate", "michigan"): [g]}
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertEqual(integ["not_completed"], 1)
        self.assertNotIn(("e1", 2024), matched)

    def test_null_scores_excluded(self):
        g = _game(homePoints=None, awayPoints=None)
        idx = {(2024, "ohiostate", "michigan"): [g]}
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertEqual(integ["null_scores"], 1)

    def test_load_games_filters_2025(self):
        games = [_game(season=2024), _game(season=2025)]
        import json, tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as f:
            json.dump(games, f)
            path = f.name
        idx = mo.load_games(path)
        seasons = {k[0] for k in idx}
        self.assertEqual(seasons, {2024})
        os.unlink(path)


class TestOutcomes(unittest.TestCase):
    def _selected(self, line, prob, market="FULL_GAME_SPREAD"):
        return {("e1", 2024, 1, "early", "draftkings", market):
                {"line": line, "fair_prob": prob, "home": "Ohio State",
                 "away": "Michigan", "kickoff": "2024-11-30 17:00:00+00:00"}}

    def _matched(self, hp=24, ap=20):
        return {("e1", 2024): _game(homePoints=hp, awayPoints=ap)}

    def test_spread_cover(self):
        # home -3.5, wins by 4 -> cover
        out = mo.analyze(self._selected(-3.5, 0.6), self._matched(24, 20), [2024])
        self.assertEqual(out["n_played"], 1)
        self.assertEqual(out["by_window"]["early"]["rate"], 1.0)

    def test_spread_push(self):
        out = mo.analyze(self._selected(-4.0, 0.6), self._matched(24, 20), [2024])
        self.assertEqual(out["n_pushes"], 1)
        self.assertEqual(out["n_played"], 0)

    def test_total_over(self):
        out = mo.analyze(self._selected(40.5, 0.55, "FULL_GAME_TOTAL"),
                         self._matched(24, 20), [2024])
        self.assertEqual(out["by_window"]["early"]["rate"], 1.0)

    def test_total_push(self):
        out = mo.analyze(self._selected(44.0, 0.5, "FULL_GAME_TOTAL"),
                         self._matched(24, 20), [2024])
        self.assertEqual(out["n_pushes"], 1)

    def test_matchup_and_conf_splits(self):
        out = mo.analyze(self._selected(-3.5, 0.6), self._matched(), [2024])
        self.assertIn("FBSvFBS", out["by_matchup"])
        self.assertIn("P4", out["by_conference"])

    def test_fbs_fcs_class(self):
        sel = self._selected(-21.5, 0.7)
        m = {("e1", 2024): _game(homeClassification="fbs",
                                 awayClassification="fcs",
                                 awayConference="Missouri Valley",
                                 homePoints=45, awayPoints=10)}
        out = mo.analyze(sel, m, [2024])
        self.assertIn("FBSvFCS", out["by_matchup"])
        # conf_group uses the home team: FBS Big Ten home -> P4
        self.assertIn("P4", out["by_conference"])


class TestSummarize(unittest.TestCase):
    def test_perfect(self):
        s = mo.summarize([(0.9, 1), (0.1, 0)])
        self.assertEqual(s["n"], 2)
        self.assertEqual(s["rate"], 0.5)
        self.assertLess(s["log_loss"], 0.2)

    def test_empty(self):
        self.assertEqual(mo.summarize([])["n"], 0)


if __name__ == "__main__":
    unittest.main()
