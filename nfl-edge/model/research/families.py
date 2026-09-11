"""Champion/challenger tournament: model family registry.

A family is a chronological state machine evaluated by the replay lab
(model/research/replay.py). The interface contract:

    Family(teams)            build with the full team list
    .predict(game) -> dict | None
        game: {"home": abbr, "away": abbr}
        returns {"win_prob_home", "pred_margin_home", "pred_total"} or None
        when the family cannot price the game yet. Must use ONLY information
        from games already passed to .observe() -- no lookahead.
    .observe(game) -> None
        game: full dict with home_score/away_score. Called AFTER the game,
        exactly once, in chronological order.
    .fit_context() -> None
        called at each season boundary before predictions begin; families
        freeze fitted parameters (e.g. sigmas) here from prior seasons only.

A family earns Challenger status only by beating baselines out-of-sample on
Brier/log-loss AND showing positive simulated CLV-proxy and ROI with
acceptable calibration. Results land in MARKET_MATRIX.md.
"""
from __future__ import annotations

import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from model.challenger.elo import (Phi, expected_scores, init_ratings,
                                  update_ratings)

_FAMILIES: dict[str, type] = {}


def register(cls):
    """Class decorator registering a model family by its .name."""
    _FAMILIES[cls.name] = cls
    return cls


def family_names() -> list[str]:
    return sorted(_FAMILIES)


def get_family(name: str):
    try:
        return _FAMILIES[name]
    except KeyError:
        raise ValueError("unknown family %r (have %s)" % (name, family_names()))


@register
class EloV1:
    """Deterministic offensive/defensive Elo. Same engine as the v1 challenger.

    Zero-centered ratings, chronological updates, sigmas frozen per season
    from prior-season residuals only.
    """
    name = "elo-v1"

    def __init__(self, teams, league_avg=22.0, hfa=1.2, k=0.15):
        self._teams = set(teams)
        self.off, self.deff = init_ratings(teams)
        self.league_avg = league_avg
        self.hfa = hfa
        self.k = k
        self._margin_res: list[float] = []
        self._total_res: list[float] = []
        self.sm: float | None = None
        self.st: float | None = None

    def fit_context(self) -> None:
        if len(self._margin_res) >= 30:
            self.sm = statistics.pstdev(self._margin_res)
            self.st = statistics.pstdev(self._total_res)

    def _mus(self, game):
        exp_h, exp_a = expected_scores(self.off, self.deff, game["home"],
                                       game["away"], self.league_avg, self.hfa)
        return exp_h - exp_a, exp_h + exp_a

    def predict(self, game):
        if self.sm is None or self.st is None:
            return None
        if game["home"] not in self._teams or game["away"] not in self._teams:
            return None
        mu_margin, mu_total = self._mus(game)
        return {
            "win_prob_home": Phi(mu_margin / self.sm),
            "pred_margin_home": mu_margin,
            "pred_total": mu_total,
        }

    def observe(self, game) -> None:
        mu_margin, mu_total = self._mus(game)
        self._margin_res.append((game["home_score"] - game["away_score"])
                               - mu_margin)
        self._total_res.append((game["home_score"] + game["away_score"])
                               - mu_total)
        update_ratings(self.off, self.deff, game["home"], game["away"],
                       game["home_score"], game["away_score"],
                       self.league_avg, self.hfa, self.k)
