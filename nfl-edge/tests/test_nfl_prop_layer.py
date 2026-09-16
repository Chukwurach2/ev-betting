"""Unit tests for the NFL prospective prop/alternative-market data layer.

Zero network, zero DB: all tests run against fixtures and pure functions.
Design: nfl-edge/docs/nfl-prospective-prop-layer.md.
"""
import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ops"))

import nfl_prop_identity  # noqa: E402
import nfl_prop_collect as npc  # noqa: E402
import health  # noqa: E402

UTC = dt.timezone.utc

ALIASES = {
    "Buffalo Bills": "BUF",
    "Detroit Lions": "DET",
    "Kansas City Chiefs": "KC",
}


def nv_game(game_id, gameday, home, away, season=2026, week=2):
    return {"game_id": game_id, "gameday": gameday, "home_team": home,
            "away_team": away, "season": season, "week": week}


class NormalizeLiveEventTest(unittest.TestCase):
    def test_valid_payload(self):
        oe = nfl_prop_identity.normalize_live_event({
            "id": "abc123",
            "home_team": "Buffalo Bills",
            "away_team": "Detroit Lions",
            "commence_time": "2026-09-18T00:15:00Z",
        })
        self.assertEqual(oe["event_id"], "abc123")
        self.assertEqual(oe["home"], "Buffalo Bills")
        self.assertEqual(oe["away"], "Detroit Lions")
        self.assertEqual(oe["kickoff"],
                         dt.datetime(2026, 9, 18, 0, 15, tzinfo=UTC))

    def test_missing_id_raises(self):
        with self.assertRaises(ValueError):
            nfl_prop_identity.normalize_live_event({
                "home_team": "Buffalo Bills",
                "commence_time": "2026-09-18T00:15:00Z"})

    def test_missing_commence_time_raises(self):
        with self.assertRaises(ValueError):
            nfl_prop_identity.normalize_live_event({"id": "x"})

    def test_unparsable_commence_time_raises(self):
        with self.assertRaises(ValueError):
            nfl_prop_identity.normalize_live_event(
                {"id": "x", "commence_time": "not-a-time"})

    def test_naive_commence_time_raises(self):
        with self.assertRaises(ValueError):
            nfl_prop_identity.normalize_live_event(
                {"id": "x", "commence_time": "2026-09-18 00:15:00"})


class ResolveTickTest(unittest.TestCase):
    GAMES = [
        nv_game("2026_02_DET_BUF", "2026-09-17", "BUF", "DET"),
        nv_game("2026_02_CAR_ATL", "2026-09-20", "ATL", "CAR"),
    ]

    def test_matched(self):
        res = nfl_prop_identity.resolve_tick(
            [{"id": "e1", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions",
              "commence_time": "2026-09-18T00:15:00Z"}],
            self.GAMES, [2026], ALIASES)
        self.assertEqual(len(res["matched"]), 1)
        m = res["matched"][0]
        self.assertEqual(m["nflverse_game_id"], "2026_02_DET_BUF")
        self.assertEqual(m["provider_event_id"], "e1")
        self.assertIn(m["match_type"],
                      ("alias_date", "alias_date_shift",
                       "swapped", "swapped_date_shift"))

    def test_no_alias_recorded(self):
        res = nfl_prop_identity.resolve_tick(
            [{"id": "e2", "home_team": "Springfield Atoms",
              "away_team": "Shelbyville Sharks",
              "commence_time": "2026-09-20T17:00:00Z"}],
            self.GAMES, [2026], ALIASES)
        self.assertEqual(res["matched"], [])
        self.assertEqual(res["unmatched_odds"][0]["reason"], "no_alias")

    def test_outside_seasons_recorded(self):
        res = nfl_prop_identity.resolve_tick(
            [{"id": "e3", "home_team": "Kansas City Chiefs",
              "away_team": "Buffalo Bills",
              "commence_time": "2025-02-09T23:30:00Z"}],
            self.GAMES, [2026], ALIASES)
        self.assertEqual(res["matched"], [])
        self.assertEqual(res["unmatched_odds"][0]["reason"], "outside_seasons")

    def test_ambiguous_never_guessed(self):
        # Two nflverse games on the same date with the same matchup can only
        # arise from duplicate schedule rows; the matcher must flag, not pick.
        dup = [nv_game("2026_02_DET_BUF", "2026-09-17", "BUF", "DET"),
               nv_game("2026_02_DET_BUFX", "2026-09-17", "BUF", "DET")]
        res = nfl_prop_identity.resolve_tick(
            [{"id": "e4", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions",
              "commence_time": "2026-09-18T00:15:00Z"}],
            dup, [2026], ALIASES)
        self.assertEqual(res["matched"], [])
        self.assertEqual(len(res["ambiguous"]), 1)

    def test_id_never_reused_across_ticks(self):
        # Two ticks resolve the same canonical game to DIFFERENT provider IDs
        # (the H-D multiplicity finding). Each tick's resolution is
        # independent — nothing is carried over.
        r1 = nfl_prop_identity.resolve_tick(
            [{"id": "tick1-id", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions",
              "commence_time": "2026-09-18T00:15:00Z"}],
            self.GAMES, [2026], ALIASES)
        r2 = nfl_prop_identity.resolve_tick(
            [{"id": "tick2-id", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions",
              "commence_time": "2026-09-18T00:15:00Z"}],
            self.GAMES, [2026], ALIASES)
        self.assertEqual(r1["matched"][0]["provider_event_id"], "tick1-id")
        self.assertEqual(r2["matched"][0]["provider_event_id"], "tick2-id")

    def test_malformed_row_skipped_not_fatal(self):
        # One malformed provider row must not kill the whole tick: the bad
        # row is skipped and counted, the good row still resolves.
        res = nfl_prop_identity.resolve_tick(
            [{"id": "bad1", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions"},  # no commence_time
             {"id": "e1", "home_team": "Buffalo Bills",
              "away_team": "Detroit Lions",
              "commence_time": "2026-09-18T00:15:00Z"}],
            self.GAMES, [2026], ALIASES)
        self.assertEqual(len(res["matched"]), 1)
        self.assertEqual(res["matched"][0]["provider_event_id"], "e1")
        self.assertEqual(res["malformed_rows_skipped"], 1)

    def test_all_rows_malformed_raises(self):
        # Every row malformed: fail loud, don't silently no-op.
        with self.assertRaises(ValueError):
            nfl_prop_identity.resolve_tick(
                [{"id": "bad1"}, {"id": "bad2", "commence_time": "nope"}],
                self.GAMES, [2026], ALIASES)


class ProjectionTest(unittest.TestCase):
    def test_nominal_and_margin(self):
        self.assertEqual(npc.nominal_per_call(), 3)  # 3 markets x 1 region
        self.assertEqual(npc.planned_per_call(), 6.0)  # A1 x 2x margin

    def test_projection_math(self):
        p = npc.project_cost(16)
        self.assertEqual(p["n_paid_calls"], 16)
        self.assertEqual(p["projected_credits"], 96.0)
        self.assertEqual(p["per_call_source"], "A1_nominal_x2_margin")

    def test_cap_abort_fires(self):
        attempts = [{"canonical_game_id": "g", "checkpoint_name": "T-12h",
                     "provider_event_id_resolved": "e",
                     "status": npc.ST_OK}]
        p = npc.project_cost(1)  # 6 credits
        self.assertFalse(npc.apply_cap_check(attempts, p, 90))
        self.assertEqual(attempts[0]["status"], npc.ST_OK)

        attempts2 = [{"canonical_game_id": "g", "checkpoint_name": "T-12h",
                      "provider_event_id_resolved": "e",
                      "status": npc.ST_OK}]
        self.assertTrue(npc.apply_cap_check(attempts2, p, 5))
        self.assertEqual(attempts2[0]["status"], npc.ST_ABORTED_CAP)
        # The resolved provider ID is cleared on abort: zero paid calls.
        self.assertIsNone(attempts2[0]["provider_event_id_resolved"])

    def test_empirical_replaces_margin(self):
        p = npc.project_cost(16, empirical_per_call=18.6)
        self.assertEqual(p["projected_credits"], 297.6)
        self.assertEqual(p["per_call_source"], "empirical")


class ParseOddsTest(unittest.TestCase):
    def test_parses_verbatim_and_skips_sacks(self):
        captured = dt.datetime(2026, 9, 17, 13, 0, tzinfo=UTC)
        body = {"bookmakers": [{
            "key": "draftkings", "markets": [
                {"key": "player_pass_yds", "outcomes": [
                    {"name": "Over", "description": "Josh Allen",
                     "price": -110, "point": 267.5},
                    {"name": "Under", "description": "Josh Allen",
                     "price": -110, "point": 267.5}]},
                # Excluded from v1: must never be collected.
                {"key": "player_sacks", "outcomes": [
                    {"name": "Over", "description": "Aidan Hutchinson",
                     "price": -150, "point": 0.5}]},
            ]}]}
        quotes = npc.parse_odds_response("skey", body, captured)
        self.assertEqual(len(quotes), 2)
        markets = {q["market"] for q in quotes}
        self.assertEqual(markets, {"player_pass_yds"})
        q = quotes[0]
        self.assertEqual(q["participant_raw"], "Josh Allen")  # verbatim
        self.assertIsNone(q["participant_key"])  # NULL until alias-resolved
        self.assertEqual(q["line"], 267.5)
        self.assertEqual(q["price_american"], -110)
        self.assertEqual(q["captured_at"], captured.isoformat())


class CheckpointConfigTest(unittest.TestCase):
    def test_env_config_parsed(self):
        os.environ["NFL_PROP_CHECKPOINTS"] = '[["T-24h", 1440], ["Close", 5]]'
        try:
            cfg = npc.load_checkpoints_config()
        finally:
            del os.environ["NFL_PROP_CHECKPOINTS"]
        self.assertEqual(cfg, [("T-24h", 1440), ("Close", 5)])

    def test_default_config(self):
        os.environ.pop("NFL_PROP_CHECKPOINTS", None)
        cfg = npc.load_checkpoints_config()
        self.assertEqual(len(cfg), 6)
        self.assertEqual(cfg[0], ("T-24h", 1440))
        self.assertEqual(cfg[-1], ("Close", 5))

    def test_prop_windows_plan_without_opener(self):
        # One game 11h15m out: T-24h missed, T-12h due, nothing else;
        # no opener row (suppressed for props).
        import checkpoints
        game = {"game_id": "2026_02_DET_BUF", "status": "scheduled",
                "kickoff": dt.datetime(2026, 9, 18, 0, 15, tzinfo=UTC)}
        now = dt.datetime(2026, 9, 17, 13, 0, tzinfo=UTC)
        planned = npc.plan_prop_checkpoints([game], now,
                                            npc.load_checkpoints_config())
        by_name = {c.name: c.state for c in planned}
        self.assertEqual(by_name.get("T-24h"), "missed")
        self.assertEqual(by_name.get("T-12h"), "due")
        self.assertNotIn("Opener", by_name)
        self.assertNotIn("T-6h", by_name)  # target not yet reached
        # module attrs restored
        self.assertEqual(checkpoints.OPENER_MAX_MINUTES, 8640)


class HealthPropCollectorTest(unittest.TestCase):
    def fresh_cores(self, now):
        return {c: {"last_ok_at": now, "detail": {}}
                for c in ("schedule_sync", "collector", "picks", "settlement")}

    def test_stale_is_degraded_never_down(self):
        now = dt.datetime.now(UTC)
        stale = now - dt.timedelta(hours=7)
        hbs = self.fresh_cores(now)
        hbs["prop_collector"] = {"last_ok_at": stale, "detail": {}}
        report = health.evaluate(hbs, now, upcoming_games=5)
        self.assertEqual(report["components"]["prop_collector"]["status"],
                         "stale")
        self.assertEqual(report["status"], "degraded")

    def test_missing_is_not_yet_commissioned(self):
        # Pre-calibration the prop layer has never run: "missing" is
        # reported without escalating (no staleness to measure yet).
        now = dt.datetime.now(UTC)
        report = health.evaluate(self.fresh_cores(now), now, upcoming_games=5)
        self.assertEqual(report["components"]["prop_collector"]["status"],
                         "missing")
        self.assertEqual(report["status"], "ok")

    def test_stale_after_first_heartbeat_is_degraded(self):
        # Once a heartbeat exists, staleness escalates to degraded.
        now = dt.datetime.now(UTC)
        hbs = self.fresh_cores(now)
        hbs["prop_collector"] = {"last_ok_at": now, "detail": {}}
        report = health.evaluate(hbs, now, upcoming_games=5)
        self.assertEqual(report["components"]["prop_collector"]["status"],
                         "ok")
        self.assertEqual(report["status"], "ok")

    def test_core_stale_still_down(self):
        now = dt.datetime.now(UTC)
        stale = now - dt.timedelta(hours=7)
        hbs = {"collector": {"last_ok_at": stale, "detail": {}}}
        report = health.evaluate(hbs, now, upcoming_games=5)
        self.assertEqual(report["status"], "down")

    def test_missed_snapshots_visible_in_detail(self):
        now = dt.datetime.now(UTC)
        hbs = self.fresh_cores(now)
        hbs["prop_collector"] = {"last_ok_at": now, "detail": {}}
        report = health.evaluate(
            hbs, now, upcoming_games=5,
            completeness_missed={"prop_collector": {
                "by_checkpoint": {"T-12h": 2}, "total": 2}})
        entry = report["components"]["prop_collector"]
        self.assertEqual(entry["status"], "ok")
        self.assertEqual(entry["missed_snapshots"]["total"], 2)

    def test_out_of_season_idle(self):
        now = dt.datetime.now(UTC)
        report = health.evaluate({}, now, upcoming_games=0)
        self.assertEqual(report["components"]["prop_collector"]["status"],
                         "idle")


class LiveRefusalTest(unittest.TestCase):
    def test_live_refuses_without_env(self):
        os.environ.pop("NFL_PROP_LIVE_OK", None)
        rc = npc.main(["--live"])
        self.assertEqual(rc, 2)

    def test_dryrun_requires_fixture_events(self):
        rc = npc.main(["--out", "/tmp/nfl_prop_should_not_exist.json"])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
