"""Tests for residual-v1.  SHADOW research only."""
import math
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from research.residual_v1 import (  # noqa: E402
    attach_teams, challenger_fair, decide, null_simulation, slope_t_test,
)
from model.challenger.infer import load_challenger  # noqa: E402


def _unit(**kw):
    u = {"game": "g1", "home": "Kansas City Chiefs", "away": "Buffalo Bills",
         "market": "h2h_spreads", "line": 3.0, "season": 2024,
         "f_sat": 0.5, "f_close": 0.55}
    u.update(kw)
    return u


def test_null_simulation_nominal():
    res = null_simulation(n_seeds=40, n=300)
    assert res["verdict"] == "CALIBRATED", res
    assert res["rejection_rate"] <= 0.15


def test_planted_slope_detected():
    rng = np.random.default_rng(7)
    r = rng.normal(0, 0.05, 800)
    move = 0.5 * r + rng.normal(0, 0.01, 800)
    t = slope_t_test(r, move)
    assert t["p_one_sided"] < 0.05, t
    assert t["slope"] > 0.2, t


def test_no_signal_when_independent():
    rng = np.random.default_rng(11)
    r = rng.normal(0, 0.05, 800)
    move = rng.normal(0, 0.01, 800)
    t = slope_t_test(r, move)
    # either non-significant or tiny; the full rule must not fire on
    # a typical null draw
    assert not (t["p_one_sided"] < 0.05 and t["slope"] >= 0.2)


def test_decide_requires_all_gates():
    primary = {"p_one_sided": 0.01, "slope": 0.3}
    seasons = {"2022": {"slope": 0.3}, "2023": {"slope": -0.1},
               "2024": {"slope": 0.25}}
    assert decide(primary, seasons) == "signal"
    assert decide({"p_one_sided": 0.01, "slope": 0.1}, seasons) == "no_edge"
    assert decide({"p_one_sided": 0.2, "slope": 0.3}, seasons) == "no_edge"
    assert decide(primary, {"2022": {"slope": -0.3}}) == "no_edge"


def test_challenger_fair_spread_and_total():
    chal = load_challenger("v1")
    p_spread = challenger_fair(chal, _unit())
    p_total = challenger_fair(chal, _unit(market="totals", line=47.5))
    assert 0.0 < p_spread < 1.0, p_spread
    assert 0.0 < p_total < 1.0, p_total


def test_challenger_fair_home_underdog_direction():
    chal = load_challenger("v1")
    # f_close < 0.5 => home underdog => nflverse home_line = -L
    p = challenger_fair(chal, _unit(f_close=0.4))
    assert 0.0 < p < 1.0, p


def test_challenger_fair_unknown_team_skipped():
    chal = load_challenger("v1")
    assert challenger_fair(chal, _unit(home="Nonexistent Team")) is None


def test_attach_teams_maps_canonical_key():
    from research.devig_tournament import canonical_game_keys
    quotes = [
        {"home_team": "Kansas City Chiefs", "away_team": "Buffalo Bills",
         "kickoff": "2024-09-08T20:00:00+00:00"},
        {"home_team": "Kansas City Chiefs", "away_team": "Buffalo Bills",
         "kickoff": "2024-09-08T20:05:00+00:00"},
    ]
    key_of = canonical_game_keys(quotes)
    gkey = key_of[id(quotes[0])]
    units = attach_teams([{"game": gkey, "market": "h2h_spreads"}],
                         quotes)
    assert len(units) == 1
    assert units[0]["home"] == "Kansas City Chiefs"
    assert units[0]["away"] == "Buffalo Bills"
    # unknown key is dropped
    assert attach_teams([{"game": "nope", "market": "h2h_spreads"}],
                        quotes) == []
