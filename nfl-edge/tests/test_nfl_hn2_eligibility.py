"""Tests for ops/nfl_hn2_eligibility.py (H-N2 eligible-N gate, DRAFT).

All pure functions: venue resolution/classification, decision-slot
selection, MOS cycle/ftime selection, MDE/feasibility math. No DB, no
HTTP, no outcomes.
"""
import pathlib
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import nfl_hn2_eligibility as hn2

UTC = timezone.utc


def _entry(roof="outdoor", status="verified", station="KBUF",
           unavailable_reason=None):
    return {"roof": roof, "status": status, "mos_station": station,
            "unavailable_reason": unavailable_reason}


class VenueTest(unittest.TestCase):
    def setUp(self):
        self.mapping = {
            "Highmark Stadium": _entry("outdoor"),
            "SoFi Stadium": _entry("canopy_open_air", station="KLAX"),
            "Ford Field": _entry("dome", station="KDTW"),
            "AT&T Stadium": _entry("retractable", station="KDFW"),
        }
        self.aliases = {
            "aliases": {"New Era Field": {"venue_key": "Highmark Stadium"}},
            "unmatched": {"Deutsche Bank Park": {
                "reason": "Frankfurt; no NWS MOS coverage"}},
        }

    def test_resolve_direct(self):
        key, entry = hn2.resolve_venue("Highmark Stadium", self.mapping,
                                       self.aliases)
        self.assertEqual(key, "Highmark Stadium")
        self.assertEqual(entry["mos_station"], "KBUF")

    def test_resolve_alias(self):
        key, entry = hn2.resolve_venue("New Era Field", self.mapping,
                                       self.aliases)
        self.assertEqual(key, "Highmark Stadium")

    def test_resolve_unmatched_intl(self):
        key, reason = hn2.resolve_venue("Deutsche Bank Park", self.mapping,
                                        self.aliases)
        self.assertIsNone(key)
        self.assertTrue(reason.startswith("unmatched:"))

    def test_resolve_unmapped_fails_loud(self):
        key, reason = hn2.resolve_venue("Mystery Dome", self.mapping,
                                        self.aliases)
        self.assertIsNone(key)
        self.assertTrue(reason.startswith("unmapped_stadium:"))

    def test_classify_outdoor_included(self):
        ok, reason = hn2.classify_venue(_entry("outdoor"))
        self.assertTrue(ok)
        self.assertTrue(reason.startswith("include:"))

    def test_classify_canopy_included(self):
        ok, _ = hn2.classify_venue(_entry("canopy_open_air"))
        self.assertTrue(ok)

    def test_classify_dome_excluded(self):
        ok, reason = hn2.classify_venue(_entry("dome"))
        self.assertFalse(ok)
        self.assertEqual(reason, "exclude:dome")

    def test_classify_retractable_excluded_by_rule(self):
        # Excluded structurally -- never by gameday roof position.
        ok, reason = hn2.classify_venue(_entry("retractable"))
        self.assertFalse(ok)
        self.assertEqual(reason, "exclude:retractable")

    def test_classify_unmatched_excluded(self):
        ok, reason = hn2.classify_venue(
            _entry(roof=None, status="unmatched",
                   unavailable_reason="no NWS MOS coverage"))
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("unmatched:"))

    def test_classify_unknown_roof_raises(self):
        with self.assertRaises(ValueError):
            hn2.classify_venue(_entry(roof="bubble"))


class DecisionSlotTest(unittest.TestCase):
    def setUp(self):
        # Wed 12:00Z / Sat 12:00Z / Sun 15:30Z of week 1, 2024 season.
        self.slots = [
            (2024, 1, "wed", datetime(2024, 9, 4, 12, tzinfo=UTC)),
            (2024, 1, "sat", datetime(2024, 9, 7, 12, tzinfo=UTC)),
            (2024, 1, "sun", datetime(2024, 9, 8, 15, 30, tzinfo=UTC)),
        ]

    def test_sunday_game_gets_sat(self):
        # Sun 13:00 ET kickoff -> cutoff Sat 17:00Z; latest slot < cutoff = sat.
        ko = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
        slot, inst = hn2.decision_slot_for_game(2024, 1, ko, self.slots)
        self.assertEqual(slot, "sat")

    def test_thursday_game_gets_wed(self):
        # Thu 20:15 ET kickoff -> cutoff Fri 00:15Z; only wed < cutoff.
        ko = datetime(2024, 9, 6, 0, 15, tzinfo=UTC)
        slot, _ = hn2.decision_slot_for_game(2024, 1, ko, self.slots)
        self.assertEqual(slot, "wed")

    def test_monday_game_gets_sun(self):
        # Mon 20:15 ET kickoff -> cutoff Tue 00:15Z; sun 15:30Z < cutoff.
        ko = datetime(2024, 9, 10, 0, 15, tzinfo=UTC)
        slot, _ = hn2.decision_slot_for_game(2024, 1, ko, self.slots)
        self.assertEqual(slot, "sun")

    def test_no_pre_cutoff_slot(self):
        # Kickoff before any slot + 24h -> no decision snapshot.
        ko = datetime(2024, 9, 4, 13, 0, tzinfo=UTC)
        slot, inst = hn2.decision_slot_for_game(2024, 1, ko, self.slots)
        self.assertIsNone(slot)
        self.assertIsNone(inst)

    def test_strictly_before_cutoff(self):
        # Slot exactly AT the cutoff instant is not eligible (strict <).
        slots = [(2024, 1, "wed", datetime(2024, 9, 4, 12, tzinfo=UTC))]
        ko = datetime(2024, 9, 5, 12, tzinfo=UTC)  # cutoff == wed instant
        slot, _ = hn2.decision_slot_for_game(2024, 1, ko, slots)
        self.assertIsNone(slot)


class MosSelectionTest(unittest.TestCase):
    def test_eligible_runtimes_respect_dissemination_lag(self):
        cutoff = datetime(2024, 9, 7, 17, tzinfo=UTC)
        rts = hn2.eligible_mos_runtimes(cutoff)
        self.assertTrue(len(rts) > 0)
        for rt in rts:
            self.assertLessEqual(rt + timedelta(hours=4), cutoff)
        # grid is 6-hourly
        for rt in rts:
            self.assertEqual(rt.hour % 6, 0)

    def test_selected_is_max_eligible(self):
        import mos_forecast_archive as mos
        cutoff = datetime(2024, 9, 7, 17, tzinfo=UTC)
        rts = hn2.eligible_mos_runtimes(cutoff)
        sel = mos.select_cycle(rts, cutoff)
        self.assertEqual(sel, max(rts))
        # the 12Z cycle on cutoff day: 12+4=16 <= 17 eligible; 18Z not.
        self.assertEqual(sel, datetime(2024, 9, 7, 12, tzinfo=UTC))

    def test_pick_nearest_ftime(self):
        ko = datetime(2024, 9, 8, 17, 0, tzinfo=UTC)
        rows = [
            {"ftime": datetime(2024, 9, 8, 15, tzinfo=UTC), "wsp_knots": 8.0},
            {"ftime": datetime(2024, 9, 8, 18, tzinfo=UTC), "wsp_knots": 12.0},
        ]
        chosen = hn2.pick_nearest_ftime(rows, ko)
        self.assertEqual(chosen["wsp_knots"], 12.0)  # 18Z is 1h away vs 2h

    def test_pick_nearest_ftime_tie_goes_earlier(self):
        ko = datetime(2024, 9, 8, 16, 30, tzinfo=UTC)
        rows = [
            {"ftime": datetime(2024, 9, 8, 18, tzinfo=UTC), "wsp_knots": 12.0},
            {"ftime": datetime(2024, 9, 8, 15, tzinfo=UTC), "wsp_knots": 8.0},
        ]
        chosen = hn2.pick_nearest_ftime(rows, ko)
        self.assertEqual(chosen["ftime"].hour, 15)

    def test_pick_nearest_ftime_empty(self):
        ko = datetime(2024, 9, 8, 17, tzinfo=UTC)
        self.assertIsNone(hn2.pick_nearest_ftime([], ko))

    def test_knots_to_mph(self):
        self.assertAlmostEqual(hn2.knots_to_mph(10.0), 11.5078, places=4)


class MdeTest(unittest.TestCase):
    def test_mde_formula(self):
        # 2.4865 * 13.5 / (5 * sqrt(425)) ~= 0.326
        self.assertAlmostEqual(hn2.mde_slope(425), 0.326, places=2)

    def test_feasibility_proceed(self):
        verdict, mde = hn2.feasibility_verdict(425)
        self.assertEqual(verdict, "PROCEED-TO-FREEZE")
        self.assertLessEqual(mde, 0.40)

    def test_feasibility_infeasible_n(self):
        verdict, mde = hn2.feasibility_verdict(200)
        self.assertEqual(verdict, "INFEASIBLE")
        self.assertIsNone(mde)

    def test_feasibility_retire_on_power(self):
        # N=250 -> MDE ~0.427 > 0.40 cap
        verdict, mde = hn2.feasibility_verdict(250)
        self.assertEqual(verdict, "RETIRE-ON-POWER")
        self.assertGreater(mde, 0.40)

    def test_feasibility_boundary(self):
        # N=300 -> MDE ~0.39 <= cap
        verdict, _ = hn2.feasibility_verdict(300)
        self.assertEqual(verdict, "PROCEED-TO-FREEZE")


if __name__ == "__main__":
    unittest.main()
