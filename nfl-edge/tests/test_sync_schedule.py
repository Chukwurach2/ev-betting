import datetime as dt
import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / 'ops'))
from sync_schedule import rows_for_schedule


class SyncScheduleTests(unittest.TestCase):
    def test_maps_schedule_entries_to_game_rows(self):
        data = [
            {"game_id": "2026_01_NE_SEA", "season": 2026, "week": 1,
             "away_team": "NE", "home_team": "SEA",
             "kickoff": "2026-09-10T00:20:00+00:00", "status": "scheduled"},
        ]
        rows = rows_for_schedule(data)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["game_id"], "2026_01_NE_SEA")
        self.assertEqual(row["home_team"], "SEA")
        self.assertEqual(row["away_team"], "NE")
        self.assertEqual(row["status"], "scheduled")
        self.assertEqual(row["season"], 2026)
        self.assertEqual(row["week"], 1)
        self.assertEqual(row["kickoff"].tzinfo, dt.timezone.utc)

    def test_accepts_wrapped_games_object_and_zulu_time(self):
        data = {"games": [
            {"game_id": "g2", "away_team": "A", "home_team": "H",
             "kickoff": "2026-09-13T17:00:00Z"},
        ]}
        rows = rows_for_schedule(data)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "scheduled")
        self.assertIsNone(rows[0]["season"])

    def test_skips_malformed_entries_without_failing(self):
        data = [
            {"game_id": "bad1", "home_team": "H"},  # missing away/kickoff
            {"away_team": "A", "home_team": "H", "kickoff": "not-a-date"},
            {"game_id": "ok", "away_team": "A", "home_team": "H",
             "kickoff": "2026-09-13T17:00:00Z"},
        ]
        rows = rows_for_schedule(data)
        self.assertEqual([r["game_id"] for r in rows], ["ok"])


if __name__ == "__main__":
    unittest.main()
