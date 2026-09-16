#!/usr/bin/env python3
"""Unit tests for nfl_hn8_eligibility.py (pure functions only).

No database, no network, no outcomes.
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent.parent / "ops"
sys.path.insert(0, str(HERE))

import nfl_hn8_eligibility as hn8  # noqa: E402


def _drive_rows():
    # Two drives for team AAA in game G1:
    #  drive 1: reaches RZ (yardline_100=10 pass), scores TD
    #  drive 2: reaches RZ, no TD; has a goal-to-go snap
    # One drive for team BBB: a kickoff return TD (td_team BBB, not
    # offensive) must NOT count as an offensive RZ TD; third-down plays.
    rows = [
        # drive 1, AAA offense
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="AAA", drive=1, play_type="pass", yardline_100=10,
             touchdown=0, td_team=None, goal_to_go=0, down=1,
             third_down_converted=0, third_down_failed=0,
             fourth_down_converted=0, fourth_down_failed=0),
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="AAA", drive=1, play_type="run", yardline_100=2,
             touchdown=1, td_team="AAA", goal_to_go=1, down=2,
             third_down_converted=0, third_down_failed=0,
             fourth_down_converted=0, fourth_down_failed=0),
        # drive 2, AAA offense: RZ trip, goal-to-go, no TD
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="AAA", drive=2, play_type="run", yardline_100=15,
             touchdown=0, td_team=None, goal_to_go=0, down=3,
             third_down_converted=0, third_down_failed=1,
             fourth_down_converted=0, fourth_down_failed=0),
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="AAA", drive=2, play_type="pass", yardline_100=5,
             touchdown=0, td_team=None, goal_to_go=1, down=4,
             third_down_converted=0, third_down_failed=0,
             fourth_down_converted=0, fourth_down_failed=1),
        # drive 3, BBB: kickoff (not a counted snap) + a 3rd-down conversion
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="BBB", drive=3, play_type="kickoff", yardline_100=0,
             touchdown=1, td_team="BBB", goal_to_go=0, down=None,
             third_down_converted=0, third_down_failed=0,
             fourth_down_converted=0, fourth_down_failed=0),
        dict(game_id="G1", season=2022, week=1, season_type="REG",
             posteam="BBB", drive=3, play_type="pass", yardline_100=40,
             touchdown=0, td_team=None, goal_to_go=0, down=3,
             third_down_converted=1, third_down_failed=0,
             fourth_down_converted=0, fourth_down_failed=0),
    ]
    return pd.DataFrame(rows, columns=hn8.PBP_COLUMNS)


def test_drive_aggregates_rz_trip_and_td():
    aggs = hn8.drive_aggregates(_drive_rows())
    a1 = aggs[("G1", 1, "AAA")]
    assert a1["rz_trip"] is True
    assert a1["rz_td"] is True      # offensive TD by AAA
    assert a1["gtg"] is True
    assert a1["gtg_td"] is True
    a2 = aggs[("G1", 2, "AAA")]
    assert a2["rz_trip"] is True
    assert a2["rz_td"] is False
    assert a2["gtg"] is True
    assert a2["gtg_td"] is False
    assert a2["third_att"] == 1 and a2["third_conv"] == 0
    assert a2["fourth_att"] == 1 and a2["fourth_conv"] == 0


def test_drive_aggregates_kickoff_not_rz_trip():
    # BBB's drive 3: the only TD is a kickoff return (td_team BBB) and the
    # kickoff snap is not counted -> no RZ trip, no offensive TD.
    aggs = hn8.drive_aggregates(_drive_rows())
    a3 = aggs[("G1", 3, "BBB")]
    assert a3["rz_trip"] is False
    assert a3["rz_td"] is False
    assert a3["third_att"] == 1 and a3["third_conv"] == 1


def test_team_game_metrics_sums():
    aggs = hn8.drive_aggregates(_drive_rows())
    tg = hn8.team_game_metrics(aggs)
    m = tg[("G1", "AAA")]
    assert m["rz_trips"] == 2 and m["rz_tds"] == 1
    assert m["third_att"] == 1 and m["third_conv"] == 0
    assert m["gtg_drives"] == 2 and m["gtg_tds"] == 1
    assert m["fourth_att"] == 1 and m["fourth_conv"] == 0


def test_trailing_window_strictly_before_and_exact_n():
    team_games = {"AAA": ["G1", "G2", "G3"]}
    order = {"G1": (2022, 1), "G2": (2022, 2), "G3": (2022, 3),
             "GX": (2022, 4)}
    # strictly before G3 -> [G1, G2]; requesting 3 -> None
    assert hn8.trailing_window_metrics(team_games, order, "G3", "AAA",
                                      n_games=3) is None
    assert hn8.trailing_window_metrics(team_games, order, "G3", "AAA",
                                      n_games=2) == ["G1", "G2"]
    # same-week games are excluded (strict <)
    assert hn8.trailing_window_metrics(team_games, order, "GX", "AAA",
                                      n_games=3) == ["G1", "G2", "G3"]


def test_window_rates_none_on_empty_denominator():
    r = hn8.window_rates({"rz_trips": 5, "rz_tds": 2, "third_att": 0,
                          "third_conv": 0, "gtg_drives": 0, "gtg_tds": 0,
                          "fourth_att": 4, "fourth_conv": 1})
    assert r["rz_td_pct"] == pytest.approx(0.4)
    assert r["third_pct"] is None
    assert r["gtg_td_pct"] is None
    assert r["fourth_pct"] == pytest.approx(0.25)
    assert r["rz_trips"] == 5


def test_summarize_window():
    tg = {("G1", "AAA"): {"rz_trips": 2, "rz_tds": 1, "third_att": 3,
                          "third_conv": 1, "gtg_drives": 1, "gtg_tds": 1,
                          "fourth_att": 0, "fourth_conv": 0},
          ("G2", "AAA"): {"rz_trips": 4, "rz_tds": 2, "third_att": 5,
                          "third_conv": 2, "gtg_drives": 2, "gtg_tds": 0,
                          "fourth_att": 1, "fourth_conv": 1},
          ("G1", "BBB"): {"rz_trips": 9, "rz_tds": 9, "third_att": 9,
                          "third_conv": 9, "gtg_drives": 9, "gtg_tds": 9,
                          "fourth_att": 9, "fourth_conv": 9}}
    s = hn8.summarize_window(["G1", "G2"], tg, "AAA")
    assert s["rz_trips"] == 6 and s["rz_tds"] == 3
    assert s["fourth_att"] == 1 and s["fourth_conv"] == 1


def test_decision_slot_latest_strictly_before_cutoff():
    ko = datetime(2024, 9, 8, 17, 0, tzinfo=timezone.utc)  # Sun 13:00 ET
    slots = [(2024, 1, "wed", datetime(2024, 9, 4, 12, 0, tzinfo=timezone.utc)),
             (2024, 1, "sat", datetime(2024, 9, 7, 12, 0, tzinfo=timezone.utc)),
             (2024, 1, "sun", datetime(2024, 9, 8, 15, 30, tzinfo=timezone.utc))]
    slot, _ = hn8.decision_slot_for_game(2024, 1, ko, slots)
    assert slot == "sat"  # cutoff = Sat 17:00 UTC; sun slot is after cutoff


def test_decision_slot_none_when_all_after_cutoff():
    ko = datetime(2024, 9, 5, 0, 30, tzinfo=timezone.utc)  # Thu 20:30 ET
    slots = [(2024, 1, "wed", datetime(2024, 9, 4, 12, 0, tzinfo=timezone.utc))]
    # cutoff = Wed 00:30 UTC < wed slot -> no eligible slot
    slot, _ = hn8.decision_slot_for_game(2024, 1, ko, slots)
    assert slot is None


def test_mde_slope_formula():
    m = hn8.mde_slope(400, 0.12)
    assert m == pytest.approx(2.4865 * 13.5 / (0.12 * 20.0))


def test_feasibility_verdict_floors():
    v, m = hn8.feasibility_verdict(249, 0.12)
    assert v == "INFEASIBLE" and m is None
    v, m = hn8.feasibility_verdict(250, 0.0001)  # absurd MDE
    assert v == "RETIRE-ON-POWER" and m > hn8.MDE_CAP
    v, m = hn8.feasibility_verdict(600, 0.15)
    assert v == "PROCEED-TO-FREEZE" and m <= hn8.MDE_CAP
    # boundary: exactly at cap is not a retire
    n = 10_000
    sd = 2.4865 * 13.5 / (hn8.MDE_CAP * math.sqrt(n))
    v, m = hn8.feasibility_verdict(n, sd)
    assert v == "PROCEED-TO-FREEZE" and m == pytest.approx(hn8.MDE_CAP)


def test_pbp_column_allowlist_excludes_scores():
    banned = [c for c in hn8.PBP_COLUMNS
              if "score" in c or c in ("result", "total_home_score",
                                      "total_away_score")]
    assert banned == []
