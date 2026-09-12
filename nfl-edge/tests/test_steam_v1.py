"""Tests for steam-v1."""
import pathlib
import sys
import unittest
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

for _mod in [m for m in sys.modules if m == "research" or m.startswith("research.")]:
    del sys.modules[_mod]

from research.steam_v1 import (  # noqa: E402
    analyze_steam,
    decide_steam,
    steam_events,
)

UTC = timezone.utc
SPREAD = "FULL_GAME_SPREAD"


def make_gs(moves_s1_s2, moves_s2_close, books=6, line=-3.0):
    """One game, three snapshots. moves: {book: delta} for each leg."""
    s1 = datetime(2024, 9, 4, 11, 55, tzinfo=UTC)
    s2 = datetime(2024, 9, 7, 11, 55, tzinfo=UTC)
    s3 = datetime(2024, 9, 8, 15, 25, tzinfo=UTC)
    ko = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
    base = 0.55
    lp = {}
    for snap, mv in ((s1, {b: 0.0 for b in range(books)}),
                     (s2, moves_s1_s2), (s3, moves_s2_close)):
        d = {}
        for b in range(books):
            f = base + mv.get(b, 0.0)
            d["b%d" % b] = [(line, f)]
        lp[snap] = d
    series = {SPREAD: {s1: 1, s2: 1, s3: 1}}
    return {("Home", "Away"): {
        "kickoff": ko, "series": series, "line_pairs": {SPREAD: lp}}}


class TestSteamDetection(unittest.TestCase):
    def test_detects_coordinated_move(self):
        # 6 books all move +0.02 from s1 to s2 -> steam event.
        # s3 at base+0.03 = 0.58, so the close continues +0.01 from s2.
        gs = make_gs({b: 0.02 for b in range(6)},
                     {b: 0.03 for b in range(6)})
        evs = steam_events(gs)
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["direction"], 1)
        # Close continued +0.01 -> agree, positive clv.
        self.assertTrue(evs[0]["agree"])
        self.assertAlmostEqual(evs[0]["clv"], 0.01, places=9)

    def test_no_steam_when_divided(self):
        # 3 up, 3 down -> no steam.
        gs = make_gs({0: 0.02, 1: 0.02, 2: 0.02,
                      3: -0.02, 4: -0.02, 5: -0.02},
                     {b: 0.0 for b in range(6)})
        self.assertEqual(steam_events(gs), [])

    def test_no_steam_when_too_small(self):
        # All move, but below the per-book threshold.
        gs = make_gs({b: 0.005 for b in range(6)},
                     {b: 0.0 for b in range(6)})
        self.assertEqual(steam_events(gs), [])

    def test_disagree_scores_negative(self):
        # Steam up (+0.02 to 0.57), close reverses to 0.54 (-0.03 from s2).
        gs = make_gs({b: 0.02 for b in range(6)},
                     {b: -0.01 for b in range(6)})
        evs = steam_events(gs)
        self.assertEqual(len(evs), 1)
        self.assertFalse(evs[0]["agree"])
        self.assertLess(evs[0]["clv"], 0)


class TestSteamAnalysis(unittest.TestCase):
    def test_infeasible_when_few_games(self):
        out = analyze_steam([])
        self.assertFalse(out["feasible"])
        verdict, _ = decide_steam(out)
        self.assertEqual(verdict, "infeasible")

    def test_decide_no_edge_without_significance(self):
        evs = [{"game": ("H%d" % i, "A"), "agree": i % 2 == 0,
                "clv": 0.001 if i % 2 == 0 else -0.001}
               for i in range(40)]
        out = analyze_steam(evs)
        self.assertTrue(out["feasible"])
        verdict, _ = decide_steam(out)
        self.assertEqual(verdict, "no_edge")


if __name__ == "__main__":
    unittest.main()
