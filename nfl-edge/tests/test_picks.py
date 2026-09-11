"""Tests for the shadow picks engine: math, gating, and idempotency shape."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ops"))
import picks


class PicksMathTests(unittest.TestCase):
    def test_american_to_decimal(self):
        self.assertAlmostEqual(picks.american_to_decimal(100), 2.0)
        self.assertAlmostEqual(picks.american_to_decimal(-110), 1.0 + 100 / 110)
        self.assertAlmostEqual(picks.american_to_decimal(150), 2.5)

    def test_consensus_is_median(self):
        self.assertAlmostEqual(
            picks.consensus_prob([0.50, 0.52, 0.90]), 0.52)

    def test_edge_positive_when_odds_beat_fair(self):
        # fair 50%, offered at +110 (2.0909): edge = .5*2.0909-1 > 0
        edge = picks.edge_for_quote(picks.american_to_decimal(110), 0.5)
        self.assertGreater(edge, 0.04)

    def test_edge_negative_at_vigged_price(self):
        # fair 50%, offered at -110: edge < 0
        edge = picks.edge_for_quote(picks.american_to_decimal(-110), 0.5)
        self.assertLess(edge, 0)

    def test_kelly_zero_without_edge(self):
        self.assertEqual(picks.kelly_fraction(0.0, 2.0), 0.0)
        self.assertEqual(picks.kelly_fraction(-0.01, 2.0), 0.0)

    def test_kelly_fractional_and_capped(self):
        kf = picks.kelly_fraction(0.05, 2.0)  # full kelly .05 -> quarter .0125
        self.assertAlmostEqual(kf, 0.0125)
        self.assertLessEqual(picks.stake_units(kf), picks.MAX_STAKE_UNITS)

    def test_stake_cap(self):
        self.assertEqual(picks.stake_units(10.0), picks.MAX_STAKE_UNITS)

    def test_pick_id_deterministic(self):
        a = picks.pick_id_for("v1", "e", "M", "s", "-3.5", "b", "2026-09-12T00:00:00+00:00")
        b = picks.pick_id_for("v1", "e", "M", "s", "-3.5", "b", "2026-09-12T00:00:00+00:00")
        c = picks.pick_id_for("v1", "e", "M", "s", "-3.5", "b2", "2026-09-12T00:00:00+00:00")
        d = picks.pick_id_for("v1", "e", "M", "s", "-4.5", "b", "2026-09-12T00:00:00+00:00")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)  # line is part of the identity

    def test_line_key_canonical(self):
        self.assertEqual(picks.line_key(-3.5), "-3.5")
        self.assertEqual(picks.line_key(45.0), "45")
        self.assertEqual(picks.line_key("47.5"), "47.5")

    def test_shadow_gate_rejects_other_modes(self):
        with self.assertRaises(ValueError):
            picks.assert_shadow("production")
        with self.assertRaises(ValueError):
            picks.assert_shadow("challenger")
        picks.assert_shadow("shadow")  # no raise

    def test_engine_constants_sane(self):
        self.assertEqual(picks.MODE, "shadow")
        self.assertGreaterEqual(picks.MIN_EDGE, 0.01)
        self.assertGreaterEqual(picks.MIN_CONSENSUS_BOOKS, 2)
        self.assertGreater(picks.FRESHNESS_MINUTES, 0)


class PicksBuildTests(unittest.TestCase):
    """build_picks against a fake connection: no DB required."""

    class FakeCursor:
        def __init__(self, rows, cols):
            self._rows, self.description = rows, [(c,) for c in cols]
            self.rowcount = 0

        def execute(self, *a, **k):
            return None

        def fetchall(self):
            return self._rows

    class FakeConn:
        def __init__(self, rows, cols):
            self._rows, self._cols = rows, cols

        def cursor(self):
            return PicksBuildTests.FakeCursor(self._rows, self._cols)

    COLS = ["provider_event_id", "home_team", "away_team", "kickoff", "market",
            "selection", "line", "sportsbook", "book_key", "american_odds",
            "fair_probability", "observed_at", "game_id"]

    def run_build(self, rows):
        import datetime as dt
        conn = self.FakeConn(rows, self.COLS)
        return picks.build_picks(conn)

    def test_emits_pick_when_book_beats_consensus(self):
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "DraftKings", "draftkings", -110, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "FanDuel", "fanduel", -105, 0.50, now, "g1"),
            # third book way off market: +120 on a 50%-fair selection
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "Circa", "circa", 120, 0.40, now, "g1"),
        ]
        out = self.run_build(rows)
        # consensus median of [.50,.50,.40] = .50; circa at +120: edge=.50*2.2-1=.10
        circa = [p for p in out if p["book_key"] == "circa"]
        self.assertEqual(len(circa), 1)
        self.assertGreaterEqual(circa[0]["edge"], picks.MIN_EDGE)
        self.assertEqual(circa[0]["mode"], "shadow")
        # the -110/-105 books have negative edge vs consensus: no picks
        self.assertEqual(len([p for p in out if p["book_key"] != "circa"]), 0)

    def test_no_pick_below_threshold(self):
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 47.5,
             "DraftKings", "draftkings", -110, 0.52, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 47.5,
             "FanDuel", "fanduel", -105, 0.50, now, "g1"),
        ]
        self.assertEqual(self.run_build(rows), [])

    def test_single_book_never_emits(self):
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "DraftKings", "draftkings", 200, 0.60, now, "g1"),
        ]
        self.assertEqual(self.run_build(rows), [])

    def test_empty_quotes_empty_picks(self):
        self.assertEqual(self.run_build([]), [])

    def test_no_consensus_across_different_lines(self):
        # Two books, same selection, DIFFERENT lines: no shared consensus,
        # so no pick even though one book's price looks generous.
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "DraftKings", "draftkings", 120, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -4.5,
             "FanDuel", "fanduel", -110, 0.50, now, "g1"),
        ]
        self.assertEqual(self.run_build(rows), [])

    def test_consensus_only_within_same_line(self):
        # Two books at -3.5 form a consensus; a lone book at -4.5 is ignored.
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "DraftKings", "draftkings", -110, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -3.5,
             "FanDuel", "fanduel", -105, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_SPREAD", "A", -4.5,
             "Circa", "circa", 200, 0.60, now, "g1"),
        ]
        out = self.run_build(rows)
        # -3.5 group: consensus .50, both books -110/-105 -> negative edge
        # -4.5 group: single book -> no consensus. Nothing emitted.
        self.assertEqual(out, [])

    def test_line_move_splits_groups(self):        # Same book re-quoted at a moved line: each line needs its own
        # two-book consensus.
        import datetime as dt
        now = dt.datetime.now(dt.timezone.utc)
        rows = [
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 45.5,
             "DraftKings", "draftkings", -110, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 45.5,
             "FanDuel", "fanduel", -110, 0.50, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 46.5,
             "DraftKings", "draftkings", 130, 0.45, now, "g1"),
            ("e1", "H", "A", now, "FULL_GAME_TOTAL", "Over", 46.5,
             "FanDuel", "fanduel", -110, 0.45, now, "g1"),
        ]
        out = self.run_build(rows)
        # 45.5 group: consensus .50, -110 both -> edge .50*1.909-1 < 0
        # 46.5 group: consensus .45, DK +130 -> edge .45*2.3-1 = .035 -> pick
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["book_key"], "draftkings")
        self.assertEqual(float(out[0]["line"]), 46.5)


def _mkpick(event="e1", market="FULL_GAME_SPREAD", selection="A",
            edge=0.05, stake=0.5, line=-3.5):
    return {"pick_id": f"{event}-{market}-{selection}-{line}",
            "provider_event_id": event, "market": market,
            "selection": selection, "line": line,
            "edge": edge, "stake_units": stake}


class RiskLimitsTests(unittest.TestCase):
    def test_empty(self):
        kept, stats = picks.apply_risk_limits([])
        self.assertEqual(kept, [])
        self.assertEqual(stats["kept"], 0)

    def test_dedupe_keeps_best_edge_per_side(self):
        ps = [_mkpick(edge=0.03, line=-3.5), _mkpick(edge=0.09, line=-4.5),
              _mkpick(market="FULL_GAME_TOTAL", selection="Over", edge=0.04)]
        kept, stats = picks.apply_risk_limits(ps)
        self.assertEqual(len(kept), 2)
        self.assertEqual(stats["deduped"], 1)
        spread = [p for p in kept if p["market"] == "FULL_GAME_SPREAD"][0]
        self.assertEqual(spread["line"], -4.5)  # higher edge wins

    def test_per_event_stake_cap(self):
        ps = [_mkpick(market="M1", selection="s1", edge=0.10, stake=1.0),
              _mkpick(market="M2", selection="s2", edge=0.09, stake=1.0),
              _mkpick(market="M3", selection="s3", edge=0.08, stake=1.0)]
        kept, stats = picks.apply_risk_limits(ps)
        # greedy by edge: 1.0u fits, second 1.0u would exceed 1.5u cap
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["edge"], 0.10)
        self.assertEqual(stats["event_capped"], 2)

    def test_per_event_pick_count_cap(self):
        ps = [_mkpick(market=f"M{i}", selection=f"s{i}", edge=0.05 + i * 0.001,
                      stake=0.1) for i in range(6)]
        kept, stats = picks.apply_risk_limits(ps)
        self.assertEqual(len(kept), picks.MAX_PICKS_PER_EVENT)
        self.assertEqual(stats["event_capped"], 2)

    def test_per_run_stake_cap(self):
        ps = [_mkpick(event=f"e{i}", edge=0.05, stake=1.0) for i in range(12)]
        kept, stats = picks.apply_risk_limits(ps)
        total = sum(p["stake_units"] for p in kept)
        self.assertLessEqual(total, picks.MAX_STAKE_PER_RUN + 1e-9)
        self.assertGreater(stats["run_capped"], 0)
        self.assertEqual(stats["total_stake_units"], round(total, 4))

    def test_sorted_by_edge_desc(self):
        ps = [_mkpick(event=f"e{i}", edge=0.05 + (i % 3) * 0.01, stake=0.2)
              for i in range(5)]
        kept, _ = picks.apply_risk_limits(ps)
        edges = [p["edge"] for p in kept]
        self.assertEqual(edges, sorted(edges, reverse=True))


if __name__ == "__main__":
    unittest.main()
