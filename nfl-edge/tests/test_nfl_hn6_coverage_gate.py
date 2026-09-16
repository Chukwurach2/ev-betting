"""Tests for the H-N6 read-only coverage gate (nfl_hn6_coverage_gate.py).

Guards the frozen-rule mechanics without touching the database:
  1. Snapshot slots are the frozen cadence (Wed 12:00 / Sat 12:00 /
     Sun 15:30 UTC of the game week).
  2. Provider observed_at values map to cadence slots within tolerance;
     nothing outside tolerance is claimed.
  3. Consensus = median Over line across books; >=3 books incl. Pinnacle
     within 0.01 of the consensus line. Per-book medians first (1:n
     Odds event-id multiplicity must not double-count a book).
  4. Fallback usage is missingness at the decision snapshot, never
     coverage: decision > fallback > none.
"""
import pathlib
import sys
import unittest
from datetime import datetime, timezone

OPS = pathlib.Path(__file__).parents[1] / "ops"
sys.path.insert(0, str(OPS))

import nfl_hn6_coverage_gate as gate  # noqa: E402


def utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


class TestSnapshotSlots(unittest.TestCase):
    def test_week1_2022_instants(self):
        slots = {(s, w, slot): inst
                 for (s, w, slot, inst) in gate.snapshot_slots()}
        # anchor 2022-09-11 (Sun); Wed = -4d, Sat = -1d, Sun = +0d
        self.assertEqual(slots[(2022, 1, "wed")], utc(2022, 9, 7, 12, 0))
        self.assertEqual(slots[(2022, 1, "sat")], utc(2022, 9, 10, 12, 0))
        self.assertEqual(slots[(2022, 1, "sun")], utc(2022, 9, 11, 15, 30))

    def test_all_seasons_54_slots_each(self):
        slots = gate.snapshot_slots()
        self.assertEqual(len(slots), 3 * 18 * 3)
        for s in (2022, 2023, 2024):
            self.assertEqual(sum(1 for x in slots if x[0] == s), 54)

    def test_decision_slot_is_wednesday(self):
        for (s, w, slot, inst) in gate.snapshot_slots():
            if slot == "wed":
                self.assertEqual(inst.weekday(), 2)  # Monday=0
                self.assertEqual((inst.hour, inst.minute), (12, 0))


class TestObservedToSlot(unittest.TestCase):
    def test_exact_match(self):
        slots = gate.snapshot_slots()
        oas = [inst for (_, _, _, inst) in slots]
        claimed, unmatched = gate.match_observed_to_slots(oas, slots)
        self.assertEqual(len(claimed), len(slots))
        self.assertEqual(unmatched, [])

    def test_drift_within_tolerance_claimed(self):
        slots = gate.snapshot_slots()
        # shift one instant by 60s (within the 1800s audit tolerance)
        oas = [inst for (_, _, _, inst) in slots]
        oas[0] = datetime.fromtimestamp(oas[0].timestamp() + 60,
                                        tz=timezone.utc)
        claimed, unmatched = gate.match_observed_to_slots(oas, slots)
        self.assertEqual(len(claimed), len(slots))
        self.assertEqual(unmatched, [])

    def test_outside_tolerance_unmatched(self):
        slots = gate.snapshot_slots()
        oas = [inst for (_, _, _, inst) in slots]
        oas[0] = datetime.fromtimestamp(oas[0].timestamp() + 3600,
                                        tz=timezone.utc)
        claimed, unmatched = gate.match_observed_to_slots(oas, slots)
        self.assertEqual(len(claimed), len(slots) - 1)
        self.assertEqual(len(unmatched), 1)


class TestBuildConsensus(unittest.TestCase):
    def test_eligible_basic(self):
        over = [("pinnacle", 45.0), ("draftkings", 45.0),
                ("fanduel", 45.0), ("betmgm", 45.5)]
        c = gate.build_consensus(over)
        self.assertEqual(c["consensus_line"], 45.0)
        self.assertTrue(c["eligible"])
        self.assertTrue(c["pinnacle_present"])
        self.assertEqual(c["n_books_at_line"], 3)

    def test_fewer_than_three_books(self):
        c = gate.build_consensus([("pinnacle", 45.0), ("draftkings", 45.0)])
        self.assertFalse(c["eligible"])

    def test_pinnacle_off_line(self):
        over = [("pinnacle", 46.0), ("draftkings", 45.0),
                ("fanduel", 45.0), ("betmgm", 45.0)]
        c = gate.build_consensus(over)
        self.assertEqual(c["consensus_line"], 45.0)
        self.assertFalse(c["pinnacle_present"])
        self.assertFalse(c["eligible"])

    def test_tolerance_boundary_within(self):
        # exactly 0.01 away counts as "within 0.01"
        over = [("pinnacle", 45.0), ("draftkings", 45.01),
                ("fanduel", 44.99)]
        c = gate.build_consensus(over)
        self.assertTrue(c["eligible"])
        self.assertEqual(c["n_books_at_line"], 3)

    def test_tolerance_boundary_outside(self):
        over = [("pinnacle", 45.0), ("draftkings", 45.02),
                ("fanduel", 44.98)]
        c = gate.build_consensus(over)
        self.assertFalse(c["eligible"])

    def test_per_book_median_not_double_count(self):
        # same book quoting two event ids (1:n multiplicity): one vote
        over = [("pinnacle", 45.0), ("pinnacle", 45.0),
                ("draftkings", 45.0), ("fanduel", 45.0)]
        c = gate.build_consensus(over)
        self.assertEqual(c["n_books"], 3)
        self.assertTrue(c["eligible"])

    def test_empty(self):
        c = gate.build_consensus([])
        self.assertFalse(c["eligible"])
        self.assertIsNone(c["consensus_line"])


class TestClassifyGame(unittest.TestCase):
    def test_decision(self):
        self.assertEqual(gate.classify_game(True, False, False), "decision")
        self.assertEqual(gate.classify_game(True, True, True), "decision")

    def test_fallback_is_missingness_not_coverage(self):
        self.assertEqual(gate.classify_game(False, True, False), "fallback")
        self.assertEqual(gate.classify_game(False, False, True), "fallback")

    def test_none(self):
        self.assertEqual(gate.classify_game(False, False, False), "none")


if __name__ == "__main__":
    unittest.main()
