import datetime as dt
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
from provider_identity_diagnostic import diagnose_provider_identity


class ProviderIdentityDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.home = "Kansas City Chiefs"
        self.away = "Buffalo Bills"
        self.kickoff = "2026-09-20T20:25:00Z"

    def event(self, event_id="event-1", home=None, away=None, kickoff=None):
        return {
            "id": event_id,
            "home_team": home or self.home,
            "away_team": away or self.away,
            "commence_time": kickoff or self.kickoff,
        }

    def test_reports_one_exact_match(self):
        result = diagnose_provider_identity(
            self.home, self.away, self.kickoff, [self.event()])
        self.assertEqual(result["status"], "exact_match")
        self.assertEqual(result["exact_event_id"], "event-1")
        self.assertEqual(result["closest_kickoff_delta_seconds"], 0)

    def test_kickoff_drift_is_diagnostic_only(self):
        result = diagnose_provider_identity(self.home, self.away, self.kickoff, [
            self.event(kickoff="2026-09-20T20:26:30Z")])
        self.assertEqual(result["status"], "matchup_found_kickoff_mismatch")
        self.assertEqual(result["exact_event_id"], None)
        self.assertEqual(result["closest_kickoff_delta_seconds"], 90)

    def test_reports_reversed_teams_without_accepting_them(self):
        result = diagnose_provider_identity(self.home, self.away, self.kickoff, [
            self.event(home=self.away, away=self.home)])
        self.assertEqual(result["status"], "teams_reversed")
        self.assertEqual(result["exact_match_count"], 0)
        self.assertEqual(result["reverse_event_ids"], ["event-1"])

    def test_duplicate_exact_rows_remain_ambiguous(self):
        result = diagnose_provider_identity(self.home, self.away, self.kickoff, [
            self.event("a"), self.event("b")])
        self.assertEqual(result["status"], "ambiguous_exact_duplicates")
        self.assertIsNone(result["exact_event_id"])
        self.assertEqual(result["candidate_event_ids"], ["a", "b"])

    def test_absence_and_malformed_rows_are_explicit(self):
        result = diagnose_provider_identity(self.home, self.away, self.kickoff, [
            None, {}, self.event(home="Other", away="Opponent")])
        self.assertEqual(result["status"], "matchup_absent")
        self.assertEqual(result["malformed_event_count"], 2)

    def test_closest_drift_is_absolute_nearest_with_signed_value(self):
        result = diagnose_provider_identity(self.home, self.away, self.kickoff, [
            self.event("later", kickoff="2026-09-20T20:27:00Z"),
            self.event("earlier", kickoff="2026-09-20T20:24:30Z")])
        self.assertEqual(result["closest_kickoff_delta_seconds"], -30)

    def test_malformed_ids_preserve_exact_match_cardinality(self):
        for bad_id in [None, "", 42, "   "]:
            with self.subTest(bad_id=bad_id):
                result = diagnose_provider_identity(
                    self.home, self.away, self.kickoff,
                    [self.event(), self.event(event_id=bad_id)])
                self.assertEqual(result["status"], "ambiguous_exact_duplicates")
                self.assertEqual(result["exact_match_count"], 2)
                self.assertEqual(result["malformed_event_count"], 1)
                self.assertIsNone(result["exact_event_id"])
                self.assertEqual(result["candidate_event_ids"], ["event-1"])

    def test_single_malformed_exact_id_fails_closed(self):
        result = diagnose_provider_identity(
            self.home, self.away, self.kickoff, [self.event(event_id=None)])
        self.assertEqual(result["status"], "malformed_exact_match")
        self.assertEqual(result["exact_match_count"], 1)
        self.assertIsNone(result["exact_event_id"])

    def test_invalid_expected_identity_fails_closed(self):
        invalid = [
            ("", self.away, self.kickoff, []),
            (self.home, self.home, self.kickoff, []),
            (self.home, self.away, "not-a-time", []),
            (self.home, self.away, dt.datetime(2026, 9, 20), []),
            (self.home, self.away, self.kickoff, None),
        ]
        for args in invalid:
            with self.subTest(args=args):
                self.assertEqual(
                    diagnose_provider_identity(*args)["status"],
                    "invalid_expected_identity")


if __name__ == "__main__":
    unittest.main()
