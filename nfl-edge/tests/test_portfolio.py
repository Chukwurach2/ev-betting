"""Tests for the correlation-aware portfolio selector."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.portfolio import (american_to_decimal, kelly_units,
                                      market_quality, select_opportunities)


def cand(**over):
    c = {"event_id": "ev1", "market": "full_game_spread",
         "selection": "home", "line": "-3.0", "book": "draftkings",
         "american_odds": -110, "fair_prob": 0.60, "edge_pp": 5.0,
         "confidence": 0.8, "quote_age_s": 60, "model": "elo-v1",
         "data_quality": "ok", "teams": ["KC", "BUF"]}
    c.update(over)
    return c


class FilterTests(unittest.TestCase):
    def test_stale_quote_filtered(self):
        out = select_opportunities([cand(quote_age_s=3600)])
        self.assertEqual(out, [])

    def test_thin_data_quality_filtered(self):
        out = select_opportunities([cand(data_quality="thin")])
        self.assertEqual(out, [])

    def test_below_min_edge_filtered(self):
        out = select_opportunities([cand(edge_pp=1.0)])
        self.assertEqual(out, [])

    def test_below_min_confidence_filtered(self):
        out = select_opportunities([cand(confidence=0.3)])
        self.assertEqual(out, [])

    def test_no_edge_gets_no_units(self):
        out = select_opportunities([cand(fair_prob=0.50, edge_pp=3.0)])
        self.assertEqual(out, [])


class SizingTests(unittest.TestCase):
    def test_american_to_decimal(self):
        self.assertAlmostEqual(american_to_decimal(-110), 1.0 + 100 / 110)
        self.assertAlmostEqual(american_to_decimal(150), 2.5)

    def test_kelly_sanity_quarter_kelly_capped(self):
        # fair 0.6 at -110: edge = 0.6*1.9091-1 = 0.1455,
        # f = 0.1455/0.9091 = 0.16; quarter kelly on 100u = 4.0 -> cap 1.0
        u = kelly_units(0.60, -110, 100.0, 0.25, 1.0)
        self.assertAlmostEqual(u, 1.0, places=6)

    def test_kelly_uncapped_value(self):
        u = kelly_units(0.60, -110, 100.0, 0.25, 100.0)
        self.assertAlmostEqual(u, 4.0, places=2)

    def test_kelly_zero_without_edge(self):
        self.assertEqual(kelly_units(0.50, -110, 100.0, 0.25, 1.0), 0.0)

    def test_market_quality_weights(self):
        self.assertEqual(market_quality("full_game_spread"), 1.0)
        self.assertEqual(market_quality("moneyline"), 1.0)
        self.assertEqual(market_quality("first_quarter_total"), 0.7)
        self.assertEqual(market_quality("nope"), 0.5)


class RankAndCapTests(unittest.TestCase):
    def test_rank_by_edge_x_confidence(self):
        low = cand(event_id="ev1", edge_pp=3.0, confidence=0.9)
        high = cand(event_id="ev2", edge_pp=8.0, confidence=0.9,
                    teams=["KC", "DEN"])
        out = select_opportunities([low, high])
        self.assertEqual([o["event_id"] for o in out], ["ev2", "ev1"])
        self.assertEqual([o["rank"] for o in out], [1, 2])

    def test_per_event_unit_cap(self):
        # Each candidate wants 4u uncapped -> 1.0u capped; event cap 1.5u
        # admits only the first.
        a = cand(event_id="ev1", selection="home")
        b = cand(event_id="ev1", selection="away", line="+3.0",
                 teams=["KC", "BUF"])
        out = select_opportunities([a, b])
        self.assertEqual(len(out), 1)
        self.assertAlmostEqual(out[0]["units"], 1.0)

    def test_per_event_count_cap(self):
        cfg = {"max_units_per_event": 8.0, "max_bets_per_event": 2,
               "max_units_per_team": 8.0, "max_units_per_day": 8.0,
               "max_units_per_bet": 0.4}
        cands = [cand(event_id="ev1", selection="s%d" % i) for i in range(3)]
        out = select_opportunities(cands, config=cfg)
        self.assertEqual(len(out), 2)

    def test_per_team_cap(self):
        cfg = {"max_units_per_team": 1.5}
        a = cand(event_id="ev1", teams=["KC", "BUF"])
        b = cand(event_id="ev2", teams=["KC", "DEN"])
        out = select_opportunities([a, b], config=cfg)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["event_id"], "ev1")

    def test_per_day_cap(self):
        cfg = {"max_units_per_day": 1.0}
        a = cand(event_id="ev1", teams=["KC", "BUF"])
        b = cand(event_id="ev2", teams=["SF", "SEA"])
        out = select_opportunities([a, b], config=cfg)
        self.assertEqual(len(out), 1)

    def test_deterministic(self):
        cands = [cand(event_id="ev%d" % i, edge_pp=3.0 + i) for i in range(5)]
        first = select_opportunities(cands)
        second = select_opportunities(list(reversed(cands)))
        self.assertEqual([o["event_id"] for o in first],
                         [o["event_id"] for o in second])

    def test_missing_teams_falls_back_to_event_cap(self):
        c = cand()
        del c["teams"]
        out = select_opportunities([c])
        self.assertEqual(len(out), 1)


if __name__ == "__main__":
    unittest.main()
