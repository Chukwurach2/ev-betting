"""Tests for the NCAAF (track D) live-collector parameterization.

Covers: provider sport resolution, the deterministic NCAAF schedule sync,
the sport-parameterized checkpoint store/materializer, and exact-string
team matching (no hardcoded 130+ team dictionary).
"""
import datetime as dt
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))  # nfl-edge/
sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))

from model import provider_oddsapi as pov
import sync_ncaaf_schedule as ncaaf_sync
from collect_checkpoints import (find_provider_event, opener_eligible,
                                  materialize_quotes, quota_reserve_ok)
from postgres_checkpoints import PostgresCheckpointStore
from checkpoints import instant


class ProviderSportTests(unittest.TestCase):
    def _capture(self, **kwargs):
        calls = {}
        orig = pov._get

        def fake_req(url, params, timeout):
            calls["url"] = url
            calls["params"] = params
            return {}, {}

        pov._get = fake_req
        try:
            pov.fetch_events(api_key="K", **kwargs)
        finally:
            pov._get = orig
        return calls

    def test_default_sport_is_nfl(self):
        calls = self._capture()
        self.assertIn("/sports/americanfootball_nfl/events", calls["url"])

    def test_ncaaf_resolves_registry_sport(self):
        calls = self._capture(sport="ncaaf")
        self.assertIn("/sports/americanfootball_ncaaf/events", calls["url"])

    def test_unknown_sport_rejected(self):
        with self.assertRaises(ValueError):
            self._capture(sport="mlb")

    def test_event_odds_sport_param(self):
        calls = {}
        orig = pov._get

        def fake_req(url, params, timeout):
            calls["url"] = url
            return {"id": "e1"}, {}

        pov._get = fake_req
        try:
            pov.fetch_event_odds("e1", ["spreads"], api_key="K", sport="ncaaf")
        finally:
            pov._get = orig
        self.assertIn("/sports/americanfootball_ncaaf/events/e1/odds", calls["url"])


class NcaafSyncTests(unittest.TestCase):
    def test_game_id_deterministic_and_prefixed(self):
        a = ncaaf_sync.game_id_for_event(
            "Georgia Bulldogs", "Alabama Crimson Tide",
            dt.datetime(2026, 9, 12, 16, 0, tzinfo=dt.timezone.utc))
        b = ncaaf_sync.game_id_for_event(
            "Georgia Bulldogs", "Alabama Crimson Tide",
            dt.datetime(2026, 9, 12, 19, 30, tzinfo=dt.timezone.utc))
        self.assertTrue(a.startswith("ncaaf-"))
        # Same date -> same id (stable across provider id re-issues and
        # kickoff-time shifts within the day).
        self.assertEqual(a, b)
        c = ncaaf_sync.game_id_for_event(
            "Georgia Bulldogs", "Alabama Crimson Tide",
            dt.datetime(2026, 10, 3, 16, 0, tzinfo=dt.timezone.utc))
        self.assertNotEqual(a, c)

    def test_rows_for_events(self):
        events = [{
            "id": "provider-id-1",
            "home_team": "Ohio State Buckeyes",
            "away_team": "Michigan Wolverines",
            "commence_time": "2026-11-28T17:00:00Z",
        }]
        rows = ncaaf_sync.rows_for_events(events)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertTrue(r["game_id"].startswith("ncaaf-"))
        self.assertEqual(r["sport"], "ncaaf")
        self.assertEqual(r["status"], "scheduled")
        self.assertEqual(r["season"], 2026)
        self.assertIsNotNone(r["week"])
        # Malformed events are skipped, not fatal.
        self.assertEqual(ncaaf_sync.rows_for_events(
            [{"id": "x", "commence_time": "not-a-time"}]), [])
        self.assertEqual(ncaaf_sync.rows_for_events([]), [])


class TeamMatchingTests(unittest.TestCase):
    def setUp(self):
        self.kickoff = instant("2026-09-19T16:00:00Z")
        self.game = {"game_id": "ncaaf-abc", "home_team": "Texas Longhorns",
                     "away_team": "Oklahoma Sooners", "kickoff": self.kickoff}

    def _events(self):
        return [{"id": "e1", "home_team": "Texas Longhorns",
                 "away_team": "Oklahoma Sooners",
                 "commence_time": "2026-09-19T16:00:00Z"}]

    def test_ncaaf_matches_exact_strings(self):
        matches = find_provider_event(self.game, self._events(),
                                      self.kickoff, "ncaaf")
        self.assertEqual([m["id"] for m in matches], ["e1"])

    def test_ncaaf_rejects_inexact_match(self):
        events = [{"id": "e2", "home_team": "Texas Longhorns",
                   "away_team": "Oklahoma State Cowboys",
                   "commence_time": "2026-09-19T16:00:00Z"}]
        self.assertEqual(find_provider_event(self.game, events,
                                             self.kickoff, "ncaaf"), [])

    def test_opener_eligible_ncaaf_exact_strings(self):
        from checkpoints import plan
        now = self.kickoff - dt.timedelta(days=3)
        cp = [c for c in plan([{**self.game, "status": "scheduled"}], now)
              if c.name == "Opener"][0]
        keys = {(e["home_team"], e["away_team"],
                 instant(e["commence_time"])) for e in self._events()}
        self.assertTrue(opener_eligible(cp, self.game, keys, set(), sport="ncaaf"))
        self.assertFalse(opener_eligible(cp, self.game, keys, {"ncaaf-abc"},
                                        sport="ncaaf"))


class FakeConnection:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self.autocommit = True
        self._one = fetchone_result
        self._all = fetchall_result if fetchall_result is not None else []
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class FakeCheckpoint:
    def __init__(self):
        self.key = "k1"
        self.game_id = "ncaaf-abc"
        self.kickoff = dt.datetime(2026, 9, 19, 16, 0, tzinfo=dt.timezone.utc)
        self.name = "T-24"
        self.target = self.kickoff - dt.timedelta(hours=24)
        self.deadline = self.kickoff - dt.timedelta(hours=22, minutes=30)


class StoreTableTests(unittest.TestCase):
    def test_default_table_is_nfl(self):
        store = PostgresCheckpointStore(FakeConnection())
        self.assertEqual(store.table, "nfl_edge_checkpoints")

    def test_ncaaf_claim_never_touches_nfl_tables(self):
        conn = FakeConnection(fetchone_result={"checkpoint_key": "k1"})
        store = PostgresCheckpointStore(conn, table="ncaaf_edge_checkpoints")
        now = dt.datetime(2026, 9, 18, 16, 0, tzinfo=dt.timezone.utc)
        self.assertTrue(store.claim(FakeCheckpoint(), now))
        joined = "\n".join(conn.statements)
        self.assertIn("public.ncaaf_edge_checkpoints", joined)
        self.assertNotIn("nfl_edge", joined)

    def test_unsafe_table_rejected(self):
        with self.assertRaises(ValueError):
            PostgresCheckpointStore(FakeConnection(),
                                    table="nfl_edge_checkpoints; DROP TABLE x")

    def test_materialize_ncaaf_writes_ncaaf_tables(self):
        conn = FakeConnection(fetchall_result=[{
            "checkpoint_key": "ck1", "game_id": "ncaaf-abc",
            "kickoff": dt.datetime(2026, 9, 19, 16, 0, tzinfo=dt.timezone.utc),
            "quotes": [], "home_team": "Texas Longhorns",
            "away_team": "Oklahoma Sooners"}])
        out = materialize_quotes(conn, sport="ncaaf")
        self.assertEqual(out["checkpoints_materialized"], 1)
        joined = "\n".join(conn.statements)
        self.assertIn("ncaaf_edge_checkpoints", joined)
        self.assertIn("ncaaf_edge_odds_quotes", joined)
        self.assertNotIn("nfl_edge_", joined)


class QuotaIsolationTests(unittest.TestCase):
    def _headers(self, remaining):
        return {"x-requests-remaining": str(remaining),
                "x-requests-used": "1", "x-requests-last": "2"}

    def test_nfl_never_gated(self):
        ok, info = quota_reserve_ok("nfl", self._headers(0))
        self.assertTrue(ok)
        self.assertIsNone(info)

    def test_ncaaf_stands_down_below_reserve(self):
        ok, info = quota_reserve_ok("ncaaf", self._headers(1999))
        self.assertFalse(ok)
        self.assertEqual(info["remaining"], 1999)
        self.assertEqual(info["reserve"], 2000)

    def test_ncaaf_proceeds_at_reserve(self):
        ok, info = quota_reserve_ok("ncaaf", self._headers(2000))
        self.assertTrue(ok)
        self.assertIsNone(info)

    def test_ncaaf_unknown_quota_proceeds(self):
        # Missing headers must not strand the collector; the provider
        # reports remaining on every real response.
        ok, _ = quota_reserve_ok("ncaaf", {})
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
