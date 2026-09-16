#!/usr/bin/env python3
"""Unit tests for nfl_hn7_eligibility.py (pure functions only).

No database, no network, no outcomes.
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent / "ops"
sys.path.insert(0, str(HERE))

import nfl_hn7_eligibility as hn7  # noqa: E402

UTC = timezone.utc


def _slots():
    # (season, week, slot, instant); week 5 of 2023, kickoff Sun 17:00 UTC
    base = datetime(2023, 10, 4, 12, tzinfo=UTC)  # Wed
    return [
        (2023, 5, "wed", base),
        (2023, 5, "sat", base + timedelta(days=3)),
        (2023, 5, "sun", base + timedelta(days=4, hours=3, minutes=30)),
    ]


class TestSpots:
    def test_kickoff_touchback_spot(self):
        assert hn7.tb_kick_spot(2022) == 25
        assert hn7.tb_kick_spot(2023) == 25
        assert hn7.tb_kick_spot(2024) == 30  # dynamic kickoff

    def test_kickoff_start_touchback(self):
        assert hn7.kickoff_start({"touchback": 1.0}, 2023) == 25
        assert hn7.kickoff_start({"touchback": 1.0}, 2024) == 30

    def test_kickoff_start_out_of_bounds(self):
        assert hn7.kickoff_start({"touchback": 0.0, "kickoff_out_of_bounds": 1.0}, 2023) == 40

    def test_kickoff_start_normal(self):
        # kick from own 35, 67-yd kick, 21-yd return -> start at own 19
        s = hn7.kickoff_start({"touchback": 0.0, "kickoff_out_of_bounds": 0.0,
                               "yardline_100": 35.0, "kick_distance": 67.0,
                               "return_yards": 21.0}, 2023)
        assert s == pytest.approx(19.0)

    def test_kickoff_start_nan_kick_distance_skipped(self):
        s = hn7.kickoff_start({"touchback": 0.0, "kickoff_out_of_bounds": 0.0,
                               "yardline_100": 35.0, "kick_distance": float("nan"),
                               "return_yards": 0.0}, 2023)
        assert s is None

    def test_punt_start_touchback(self):
        assert hn7.punt_start({"touchback": 1.0}) == 20

    def test_punt_start_normal(self):
        # punt from the 49 (receiving-team frame), 42-yd punt, downed
        s = hn7.punt_start({"touchback": 0.0, "yardline_100": 49.0,
                            "kick_distance": 42.0, "return_yards": 0.0})
        assert s == pytest.approx(7.0)

    def test_punt_start_with_return(self):
        s = hn7.punt_start({"touchback": 0.0, "yardline_100": 70.0,
                            "kick_distance": 51.0, "return_yards": 10.0})
        assert s == pytest.approx(29.0)


class TestShrinkage:
    def test_shrink_toward_zero(self):
        assert hn7.shrink_mean(4.0) == pytest.approx(4.0 * 8 / 12)
        assert hn7.shrink_mean(0.0) == 0.0
        assert hn7.shrink_mean(-3.0) == pytest.approx(-3.0 * 8 / 12)


class TestMDE:
    def test_mde_value(self):
        m = hn7.mde_slope(290)
        assert m == pytest.approx(2.4865 * 13.5 / (1.60 * math.sqrt(290)))

    def test_mde_decreases_with_n(self):
        assert hn7.mde_slope(400) < hn7.mde_slope(200)

    def test_infeasible_below_floor(self):
        v, m = hn7.feasibility_verdict(199)
        assert v == "INFEASIBLE" and m is None

    def test_retire_on_power(self):
        # N=290 -> MDE ~1.23 > 0.50 cap
        v, m = hn7.feasibility_verdict(290)
        assert v == "RETIRE-ON-POWER" and m > 0.50

    def test_proceed_only_at_huge_n(self):
        v, m = hn7.feasibility_verdict(20000)
        assert v == "PROCEED-TO-FREEZE" and m < 0.50


class TestKicker:
    def test_ok(self):
        primary, status = hn7.kicker_status(
            {"K. Allen": 14, "K. Backup": 2}, {"K. Allen": 2})
        assert (primary, status) == ("K. Allen", "ok")

    def test_ambiguous_share(self):
        _, status = hn7.kicker_status(
            {"K. A": 8, "K. B": 8}, {"K. A": 1})
        assert status == "ambiguous_kicker"

    def test_change_in_window(self):
        _, status = hn7.kicker_status(
            {"K. Old": 12, "K. New": 4}, {"K. New": 3})
        assert status == "kicker_change_in_window"

    def test_unknown_last_game(self):
        _, status = hn7.kicker_status({"K. Allen": 14}, {})
        assert status == "kicker_unknown_last_game"

    def test_no_attempts(self):
        _, status = hn7.kicker_status({}, {})
        assert status == "kicker_no_attempts_in_window"


class TestOutcomeBlindness:
    def test_forbidden_columns_rejected(self):
        with pytest.raises(SystemExit):
            hn7.assert_outcome_blind(
                hn7.PBP_COLUMNS + ["total_home_score"])
        with pytest.raises(SystemExit):
            hn7.assert_outcome_blind(["spread_line"])

    def test_allowed_columns_pass(self):
        hn7.assert_outcome_blind(hn7.PBP_COLUMNS)  # must not raise


class TestSlots:
    def test_decision_slot_is_wed(self):
        slot, inst = hn7.decision_slot_for_game(2023, 5, _slots())
        assert slot == "wed"
        assert inst == datetime(2023, 10, 4, 12, tzinfo=UTC)

    def test_closing_slot_latest_before_kickoff(self):
        ko = datetime(2023, 10, 8, 17, 0, tzinfo=UTC)
        slot, _ = hn7.closing_slot_for_game(2023, 5, ko, _slots())
        assert slot == "sun"

    def test_closing_slot_thursday_game(self):
        # Thursday 2023-10-05 20:15 ET = Fri 00:15 UTC -> latest slot is wed
        ko = datetime(2023, 10, 6, 0, 15, tzinfo=UTC)
        slot, _ = hn7.closing_slot_for_game(2023, 5, ko, _slots())
        assert slot == "wed"


class TestVOfTeamGame:
    def test_deviation_construction(self):
        v = {"fg": 6.0, "xp": 3.0, "retd": 0.0,
             "kfp_n": 5, "kfp_sum": 125.0, "pfp_n": 4, "pfp_sum": 100.0}
        base = {"fg": 5.0, "xp": 2.0, "retd": 0.2,
                "kick_start": 25.0, "punt_start": 24.0}
        got = hn7.v_of_team_game(v, base)
        # (6-5)+(3-2)+(0-0.2) -0.06*((125-125)) -0.06*((100-96))
        assert got == pytest.approx(1.0 + 1.0 - 0.2 - 0.06 * 4)
