"""Tests for the EPA-v1 game-state feature store.

All tests run on synthetic data (no download). Download-dependent
integration is gated behind a cache check and skipped in CI.
"""
import datetime as dt
import os
import pathlib
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.epa import ingest as ingest_mod
from model.epa.features import (FEATURE_VERSION, TeamFeatureStore,
                                feature_names, opponent_adjust, shrink,
                                summarize_season)


def trow(**kw):
    """One synthetic per-team-game row with sane defaults."""
    row = {
        "season": 2023, "week": 1, "game_id": "g1", "game_date": "2023-09-10",
        "team": "KC", "opp": "BUF", "home": True,
        "off_n": 60, "off_epa_sum": 3.0,
        "def_n": 60, "def_epa_sum": -3.0,
        "off_db_n": 35, "off_db_epa_sum": 3.5,
        "off_rush_n": 25, "off_rush_epa_sum": -0.5,
        "def_db_n": 35, "def_db_epa_sum": -3.5,
        "def_rush_n": 25, "def_rush_epa_sum": 0.5,
        "off_succ_sum": 27, "off_succ_n": 60,
        "def_succ_sum": 27, "def_succ_n": 60,
        "off_explosive": 6, "def_explosive": 6,
        "off_early_epa_sum": 2.0, "off_early_n": 40,
        "def_early_epa_sum": -2.0, "def_early_n": 40,
        "off_rz_epa_sum": 1.0, "off_rz_n": 10,
        "def_rz_epa_sum": -1.0, "def_rz_n": 10,
        "pace": 28.0, "neutral_pass": 12, "neutral_n": 22,
        "off_sacks": 2, "def_sacks": 2,
        "off_turnovers": 1, "def_turnovers": 1,
        "rest_days": 7,
    }
    row.update(kw)
    return row


def qrow(**kw):
    row = {"season": 2023, "game_id": "g1", "game_date": "2023-09-10",
           "team": "KC", "passer_id": "qb1", "passer_name": "Q. Bone",
           "attempts": 30, "dropbacks": 32, "epa_sum": 3.2}
    row.update(kw)
    return row


def make_store(team_rows, qb_rows=(), finals=None, seasons=(2023,)):
    store = TeamFeatureStore()
    # Run the real opponent adjustment, like load() does.
    tg = pd.DataFrame(opponent_adjust(list(team_rows)))
    tg["game_date"] = pd.to_datetime(tg["game_date"])
    store.team_games = tg.sort_values(
        ["game_date", "team"]).reset_index(drop=True)
    qb = pd.DataFrame(list(qb_rows))
    if len(qb):
        qb["game_date"] = pd.to_datetime(qb["game_date"])
    store.qb_games = qb
    store.season_finals = finals or {}
    store._seasons = list(seasons)
    return store


class ShrinkTests(unittest.TestCase):
    def test_shrink_blends_toward_prior(self):
        self.assertAlmostEqual(shrink(0.5, 60, 0.0), 60 / 560 * 0.5,
                               places=9)

    def test_shrink_heavy_with_little_data(self):
        # 1 game's worth of plays: dominated by the prior.
        v = shrink(0.5, 60, 0.0)
        self.assertLess(abs(v - 0.0), abs(0.5 - 0.0) / 5)

    def test_shrink_converges_with_data(self):
        v = shrink(0.5, 100000, 0.0)
        self.assertAlmostEqual(v, 0.5, places=2)

    def test_shrink_none_value_returns_prior(self):
        self.assertEqual(shrink(None, 0, 0.07), 0.07)


class PointInTimeTests(unittest.TestCase):
    def test_later_games_do_not_change_earlier_features(self):
        rows_a = [trow(game_id="g%d" % i,
                       game_date="2023-09-%02d" % (10 + i))
                  for i in range(3)]
        rows_b = rows_a + [trow(game_id="g9", game_date="2024-09-10",
                                off_epa_sum=30.0)]
        a = make_store(rows_a)
        b = make_store(rows_b)
        fa = a.team_features("KC", "2024-01-01")
        fb = b.team_features("KC", "2024-01-01")
        self.assertIsNotNone(fa)
        self.assertEqual(fa, fb)

    def test_strictly_before_as_of(self):
        rows = [trow(game_id="g1", game_date="2023-09-10")]
        s = make_store(rows)
        self.assertIsNone(s.team_features("KC", "2023-09-10"))
        self.assertIsNotNone(s.team_features("KC", "2023-09-11"))

    def test_unknown_team_returns_none(self):
        s = make_store([trow()])
        self.assertIsNone(s.team_features("XX", "2024-01-01"))


class ShrinkageBehaviorTests(unittest.TestCase):
    def _league(self):
        # Neutral league games so the prior is ~0, not the team's own game.
        return [trow(team="T%d" % i, opp="U%d" % i, game_id="lg%d" % i,
                     game_date="2023-09-03", off_epa_sum=0.0,
                     def_epa_sum=0.0)
                for i in range(4)]

    def test_week2_feature_closer_to_prior_than_raw(self):
        # One huge game: raw rolling adj EPA = +0.5, prior ~ 0.
        rows = self._league() + [trow(game_id="g1", game_date="2023-09-10",
                                      off_n=60, off_epa_sum=30.0)]
        s = make_store(rows)
        f = s.team_features("KC", "2023-09-18")
        raw = 0.5
        prior = f["off_epa_prior"]
        self.assertLess(abs(f["off_epa_play"] - prior), abs(raw - prior))
        self.assertLess(f["off_epa_play"], raw * 0.75)

    def test_feature_names_and_version(self):
        rows = self._league() + [trow(game_id="g1", game_date="2023-09-10")]
        s = make_store(rows)
        f = s.team_features("KC", "2023-09-18")
        self.assertEqual(FEATURE_VERSION, "1")
        self.assertEqual(set(feature_names()),
                         set(k for k in f if k != "feature_version"))


class OpponentAdjustTests(unittest.TestCase):
    def test_adjustment_direction(self):
        rows = [
            # B has a bad defense (allows +0.30 EPA/play), C a good one.
            trow(team="B", opp="X", game_id="b1", game_date="2023-09-01",
                 off_epa_sum=0.0, def_epa_sum=18.0),
            trow(team="X", opp="B", game_id="b1", game_date="2023-09-01",
                 off_epa_sum=0.0, def_epa_sum=0.0),
            trow(team="C", opp="Y", game_id="c1", game_date="2023-09-01",
                 off_epa_sum=0.0, def_epa_sum=-18.0),
            trow(team="Y", opp="C", game_id="c1", game_date="2023-09-01",
                 off_epa_sum=0.0, def_epa_sum=0.0),
            # A torches B (+0.20 raw) then struggles vs C (-0.10 raw).
            trow(team="A", opp="B", game_id="a1", game_date="2023-09-10",
                 off_epa_sum=12.0, def_epa_sum=0.0),
            trow(team="B", opp="A", game_id="a1", game_date="2023-09-10",
                 off_epa_sum=0.0, def_epa_sum=0.0),
            trow(team="A", opp="C", game_id="a2", game_date="2023-09-17",
                 off_epa_sum=-6.0, def_epa_sum=0.0),
            trow(team="C", opp="A", game_id="a2", game_date="2023-09-17",
                 off_epa_sum=0.0, def_epa_sum=0.0),
        ]
        adj = opponent_adjust(rows)
        by_id = {(r["team"], r["game_id"]): r["adj_off_epa"] for r in adj}
        # Torching a bad defense gets docked below the raw +0.20.
        self.assertLess(by_id[("A", "a1")], 0.20)
        # Struggling against a good defense gets credited above raw -0.10.
        self.assertGreater(by_id[("A", "a2")], -0.10)

    def test_adjustment_is_point_in_time(self):
        # Opponent's LATER games must not affect the adjustment of an
        # earlier game.
        base = [
            trow(team="A", opp="B", game_id="a1", game_date="2023-09-10",
                 off_epa_sum=6.0, def_epa_sum=0.0),
            trow(team="B", opp="A", game_id="a1", game_date="2023-09-10",
                 off_epa_sum=0.0, def_epa_sum=0.0),
        ]
        later = [trow(team="B", opp="Z", game_id="b9",
                      game_date="2023-12-01",
                      off_epa_sum=0.0, def_epa_sum=60.0),
                 trow(team="Z", opp="B", game_id="b9",
                      game_date="2023-12-01",
                      off_epa_sum=0.0, def_epa_sum=0.0)]
        a1 = {(r["team"], r["game_id"]): r["adj_off_epa"]
              for r in opponent_adjust(base)}
        a2 = {(r["team"], r["game_id"]): r["adj_off_epa"]
              for r in opponent_adjust(base + later)}
        self.assertAlmostEqual(a1[("A", "a1")], a2[("A", "a1")], places=9)


class QBTests(unittest.TestCase):
    def _store(self):
        dates = ["2023-09-10", "2023-09-17", "2023-09-24", "2023-10-01"]
        grows = [trow(game_id="g%d" % (i + 1), game_date=d,
                      off_n=60, off_epa_sum=3.0)
                 for i, d in enumerate(dates)]
        qrows = [
            qrow(game_id="g1", game_date=dates[0],
                 passer_id="qb1", epa_sum=3.2),
            qrow(game_id="g2", game_date=dates[1],
                 passer_id="qb1", epa_sum=3.2),
            qrow(game_id="g3", game_date=dates[2],
                 passer_id="qb2", epa_sum=-1.6),
            qrow(game_id="g4", game_date=dates[3],
                 passer_id="qb2", epa_sum=-1.6),
        ]
        return make_store(grows, qrows)

    def test_expected_qb_follows_recent_attempts(self):
        f = self._store().team_features("KC", "2023-10-08")
        # Last 3 games (g2,g3,g4): qb2 has 60 attempts vs qb1's 30.
        self.assertEqual(f["expected_qb_id"], "qb2")

    def test_qb_change_flag(self):
        f = self._store().team_features("KC", "2023-10-08")
        # g4 top passer (qb2) != g2 top passer (qb1).
        self.assertTrue(f["qb_changed_3g"])

    def test_qb_epa_follows_new_passer(self):
        f = self._store().team_features("KC", "2023-10-08")
        # qb2 raw: -3.2 EPA / 64 dropbacks = -0.05; shrunk toward the
        # league-average-to-date prior, so it must sit between raw and prior.
        self.assertLess(f["qb_epa_dropback"], 0.05)
        self.assertGreater(f["qb_epa_dropback"], -0.05)

    def test_no_change_flag_when_stable(self):
        s = self._store()
        qb = s.qb_games
        qb["passer_id"] = "qb1"
        s.qb_games = qb
        f = s.team_features("KC", "2023-10-08")
        self.assertFalse(f["qb_changed_3g"])
        self.assertEqual(f["expected_qb_id"], "qb1")


class DeterminismTests(unittest.TestCase):
    def test_identical_inputs_identical_outputs(self):
        rows = [trow(game_id="g%d" % i, game_date="2023-09-%02d" % (10 + i),
                     off_epa_sum=float(i))
                for i in range(5)]
        qrows = [qrow(game_id="g%d" % i, game_date="2023-09-%02d" % (10 + i))
                 for i in range(5)]
        s1 = make_store(rows, qrows)
        s2 = make_store(rows, qrows)
        self.assertEqual(s1.team_features("KC", "2023-10-20"),
                         s2.team_features("KC", "2023-10-20"))


class SummarizeTests(unittest.TestCase):
    def _frame(self):
        n = 6
        return pd.DataFrame({
            "game_id": ["g1"] * n, "game_date": ["2023-09-10"] * n,
            "season": [2023] * n, "week": [1] * n, "season_type": ["REG"] * n,
            "home_team": ["KC"] * n, "away_team": ["BUF"] * n,
            "posteam": ["KC", "KC", "KC", "BUF", "BUF", "BUF"],
            "defteam": ["BUF", "BUF", "BUF", "KC", "KC", "KC"],
            "play_id": list(range(n)),
            "play_type": ["pass", "run", "pass", "pass", "run", "pass"],
            "epa": [0.5, -0.2, 1.5, 0.1, 0.0, -0.5],
            "success": [1.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            "down": [1, 2, 3, 1, 2, 1],
            "ydstogo": [10, 8, 5, 10, 9, 10],
            "yardline_100": [75, 70, 15, 75, 72, 80],
            "qb_dropback": [1, 0, 1, 1, 0, 1],
            "qb_kneel": [0] * n, "qb_spike": [0] * n,
            "rush_attempt": [0, 1, 0, 0, 1, 0],
            "pass_attempt": [1, 0, 1, 1, 0, 1],
            "sack": [0, 0, 0, 0, 0, 0],
            "passer_player_id": ["qb1", None, "qb1", "qb9", None, "qb9"],
            "passer_player_name": ["A", None, "A", "B", None, "B"],
            "interception": [0] * n, "fumble_lost": [0] * n,
            "game_seconds_remaining": [3600, 3560, 3520, 3480, 3440, 3400],
            "half_seconds_remaining": [1800, 1760, 1720, 1680, 1640, 1600],
            "score_differential": [0] * n,
        })

    def test_summarize_season_offense_defense(self):
        tg, qb = summarize_season(self._frame())
        by_team = {r["team"]: r for r in tg}
        kc = by_team["KC"]
        self.assertEqual(kc["off_n"], 3)
        self.assertAlmostEqual(kc["off_epa_sum"], 1.8, places=9)
        self.assertEqual(kc["def_n"], 3)
        self.assertAlmostEqual(kc["def_epa_sum"], -0.4, places=9)
        self.assertEqual(kc["off_db_n"], 2)
        self.assertEqual(kc["off_rz_n"], 1)  # only the 15-yard-line play
        q = [r for r in qb if r["team"] == "KC"]
        self.assertEqual(len(q), 1)
        self.assertEqual(q[0]["passer_id"], "qb1")
        self.assertEqual(q[0]["attempts"], 2)

    def test_summarize_skips_postseason(self):
        df = self._frame()
        df["season_type"] = "POST"
        tg, qb = summarize_season(df)
        self.assertEqual(tg, [])
        self.assertEqual(qb, [])


@unittest.skipUnless(
    any(f.startswith("play_by_play_") and f.endswith(".csv.gz")
        for f in os.listdir(ingest_mod.CACHE_DIR))
    if os.path.isdir(ingest_mod.CACHE_DIR) else False,
    "no cached play-by-play; synthetic tests cover logic")
class IntegrationTests(unittest.TestCase):
    def test_real_season_loads_and_prices(self):
        store = TeamFeatureStore().load(
            [2023], cache_dir=ingest_mod.CACHE_DIR)
        f = store.team_features("KC", "2024-02-01")
        self.assertIsNotNone(f)
        self.assertEqual(f["n_games"], 8)  # full rolling window
        self.assertTrue(-1.0 < f["off_epa_play"] < 1.0)
        self.assertTrue(-1.0 < f["def_epa_play"] < 1.0)
        self.assertIsNotNone(f["expected_qb_id"])
        self.assertEqual(f["feature_version"], "1")


if __name__ == "__main__":
    unittest.main()
