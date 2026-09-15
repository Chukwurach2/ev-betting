"""Tests for ops/market_structure.py (pure logic with synthetic rows)."""
import datetime as dt
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import market_structure as ms

UTC = dt.timezone.utc


def _req(season, month, day, hour, minute):
    return dt.datetime(season, month, day, hour, minute, tzinfo=UTC)


def _row(eid, home, away, kickoff, book, market, sel, line, price, fp,
         observed):
    return {
        "provider_event_id": eid, "home_team": home, "away_team": away,
        "kickoff": kickoff, "book_key": book, "market": market,
        "selection": sel, "line": line, "american_odds": price,
        "fair_probability": fp, "observed_at": observed,
    }


def _pair_rows(eid, home, away, kickoff, book, market, line, fp_home,
               observed, price_h=-110, price_a=-110):
    if market == "FULL_GAME_TOTAL":
        sels = [("Over", fp_home, price_h), ("Under", 1 - fp_home, price_a)]
    else:
        sels = [(home, fp_home, price_h), (away, 1 - fp_home, price_a)]
    return [_row(eid, home, away, kickoff, book, market, s, line, p, f,
                 observed) for s, f, p in sels]


class MarketStructureTest(unittest.TestCase):
    def setUp(self):
        # one planned instant: Wed 2024-09-04 12:00 UTC (weekday=2) -> early
        self.t = _req(2024, 9, 4, 12, 0)
        self.plan = {self.t: (2024, 1, "early")}
        self.ko = _req(2024, 9, 7, 16, 0)
        obs = self.t - dt.timedelta(seconds=262)

        def ev(eid, book, line, fp):
            return _pair_rows(eid, "Home", "Away", self.ko, book,
                              "FULL_GAME_SPREAD", line, fp, obs)

        self.rows = (ev("e1", "draftkings", -3.0, 0.55) +
                     ev("e1", "pinnacle", -3.5, 0.57) +
                     ev("e1", "fanduel", -3.0, 0.54))

    def test_select_pairs_modal_and_reference(self):
        sel, integ, matched = ms.select_pairs(self.rows, self.plan)
        self.assertEqual(matched, {self.t})
        self.assertEqual(integ["unmatched_quotes"], 0)
        self.assertEqual(integ["identity_violations"], 0)
        # dk modal line -3.0, reference = home side
        dk = sel[("e1", 2024, 1, "early", "draftkings", "FULL_GAME_SPREAD")]
        self.assertEqual(dk["line"], -3.0)
        self.assertAlmostEqual(dk["fair_prob"], 0.55)

    def test_identity_violation_drops_event(self):
        bad = _row("e1", "Other", "Away", self.ko, "draftkings",
                   "FULL_GAME_SPREAD", "Other", -3.0, -110, 0.55,
                   self.t - dt.timedelta(seconds=262))
        sel, integ, _ = ms.select_pairs(self.rows + [bad], self.plan)
        self.assertEqual(integ["identity_violations"], 1)
        self.assertFalse(any(k[0] == "e1" for k in sel))

    def test_unmatched_quotes_counted(self):
        far = _row("e9", "H", "A", self.ko, "draftkings", "FULL_GAME_SPREAD",
                   "H", -3.0, -110, 0.55, _req(2023, 1, 1, 0, 0))
        far2 = dict(far, selection="A", fair_probability=0.45)
        sel, integ, _ = ms.select_pairs(self.rows + [far, far2], self.plan)
        self.assertEqual(integ["unmatched_quotes"], 2)

    def test_analyze_disagreement_and_pinnacle(self):
        sel, integ, _ = ms.select_pairs(self.rows, self.plan)
        out = ms.analyze(sel, integ, [2024])
        key = "FULL_GAME_SPREAD/early/s2024/[3,7)"
        self.assertIn(key, out["disagreement"])
        d = out["disagreement"][key]
        self.assertEqual(d["std_fair_prob"]["n"], 1)
        self.assertGreater(d["range_line"]["mean"], 0)
        pv = out["pinnacle_vs_consensus"]
        self.assertEqual(pv["n_event_windows"], 1)
        # pinnacle -3.5 vs median -3.0 -> deviation 0.5
        self.assertAlmostEqual(pv["abs_pinnacle_deviation"]["mean"], 0.5)

    def test_analyze_movement_needs_all_windows(self):
        sel, integ, _ = ms.select_pairs(self.rows, self.plan)
        out = ms.analyze(sel, integ, [2024])
        m = out["movement"]["FULL_GAME_SPREAD"]
        self.assertEqual(m["book_level_abs_late_minus_early"]["n"], 0)

    def test_match_window_tolerance(self):
        self.assertIsNone(ms.match_window(_req(2024, 9, 4, 13, 0), self.plan))
        self.assertEqual(
            ms.match_window(self.t - dt.timedelta(seconds=262), self.plan),
            self.t)


if __name__ == "__main__":
    unittest.main()
