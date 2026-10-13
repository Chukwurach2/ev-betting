import datetime as dt
import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / 'ops'))
from sync_schedule import (
    natural_game_key, resolve_existing_game_ids, rows_for_schedule,
    upsert_assignments,
)


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
            {"game_id": "bad1", "home_team": "H"},
            {"away_team": "A", "home_team": "H", "kickoff": "not-a-date"},
            {"game_id": "ok", "away_team": "A", "home_team": "H",
             "kickoff": "2026-09-13T17:00:00Z"},
        ]
        rows = rows_for_schedule(data)
        self.assertEqual([r["game_id"] for r in rows], ["ok"])

    def test_reuses_existing_legacy_identity_for_exact_game(self):
        schedule = rows_for_schedule([{
            "game_id": "2026_01_SF_LA", "season": 2026, "week": 1,
            "away_team": "SF", "home_team": "LA",
            "kickoff": "2026-09-11T00:35:00Z",
        }])
        existing = [{
            "game_id": "nfl-2026-reg-01-sf-lar", "season": 2026, "week": 1,
            "away_team": "SF", "home_team": "LAR",
            "kickoff": "2026-09-11T00:35:00+00:00",
        }]
        rows, aliases = resolve_existing_game_ids(schedule, existing)
        self.assertEqual(rows[0]["game_id"], "nfl-2026-reg-01-sf-lar")
        self.assertEqual(aliases[0]["schedule_game_id"], "2026_01_SF_LA")

    def test_prefers_legacy_id_when_duplicate_rows_already_exist(self):
        schedule = rows_for_schedule([{
            "game_id": "2026_01_NE_SEA", "season": 2026, "week": 1,
            "away_team": "NE", "home_team": "SEA",
            "kickoff": "2026-09-10T00:20:00Z",
        }])
        existing = [dict(schedule[0]), {
            **schedule[0], "game_id": "nfl-2026-reg-01-ne-sea",
        }]
        rows, aliases = resolve_existing_game_ids(schedule, existing)
        self.assertEqual(rows[0]["game_id"], "nfl-2026-reg-01-ne-sea")
        self.assertEqual(aliases[0]["candidate_count"], 2)

    def test_does_not_merge_different_kickoff_or_week(self):
        row = rows_for_schedule([{
            "game_id": "2026_02_NE_SEA", "season": 2026, "week": 2,
            "away_team": "NE", "home_team": "SEA",
            "kickoff": "2026-09-17T00:20:00Z",
        }])[0]
        existing = [{
            **row, "game_id": "nfl-other", "week": 1,
        }, {
            **row, "game_id": "nfl-rescheduled",
            "kickoff": dt.datetime(2026, 9, 17, 0, 25, tzinfo=dt.timezone.utc),
        }]
        rows, aliases = resolve_existing_game_ids([row], existing)
        self.assertEqual(rows[0]["game_id"], "2026_02_NE_SEA")
        self.assertEqual(aliases, [])

    def test_natural_key_requires_complete_identity(self):
        self.assertIsNone(natural_game_key({"season": 2026, "week": 1}))

    def test_upsert_never_regresses_terminal_status(self):
        sql = upsert_assignments(["game_id", "kickoff", "status"])
        self.assertIn("kickoff=EXCLUDED.kickoff", sql)
        self.assertIn("public.games.status='scheduled'", sql)
        self.assertIn("ELSE public.games.status", sql)


if __name__ == "__main__":
    unittest.main()
