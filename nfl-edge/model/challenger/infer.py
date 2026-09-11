"""Inference: load a versioned challenger artifact and predict matchups."""
import json
import os

from .elo import cover_prob_home, over_prob, predict_game

# nflverse abbreviation -> canonical abbreviation used by the odds pipeline.
TEAM_ALIASES = {
    "ARI": "AZ",
    "LA": "LAR",
    "OAK": "LV",
    "SD": "LAC",
    "STL": "LAR",
}

ARTIFACT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "artifacts")


class Challenger:
    def __init__(self, payload):
        self.version = payload["version"]
        self.params = payload["params"]
        self.off = payload["ratings"]["off"]
        self.deff = payload["ratings"]["def"]
        self.sigma_margin = payload["params"]["sigma_margin"]
        self.sigma_total = payload["params"]["sigma_total"]
        self.league_avg = payload["params"]["league_avg"]
        self.hfa = payload["params"]["hfa"]
        self.role = payload.get("role", "research")
        self.gate = payload.get("gate", {})

    def _nv(self, team):
        """Map canonical abbreviation to nflverse code."""
        team = (team or "").upper()
        for nv, canon in TEAM_ALIASES.items():
            if canon == team:
                return nv
        return team

    def predict(self, home, away):
        """Pregame prediction. Returns None if either team is unknown."""
        out = predict_game(self.off, self.deff, self._nv(home), self._nv(away),
                           self.sigma_margin, self.sigma_total,
                           self.league_avg, self.hfa)
        if out is None:
            return None
        out["version"] = self.version
        return out

    def cover_probability(self, home, away, market, selection, line):
        """P(selection covers `line`). Returns None when not computable.

        market: 'spreads' | 'totals'. selection: 'home' | 'away' | 'over' | 'under'.
        For spreads, `line` is the HOME-perspective line in nflverse
        convention: positive => home favored (e.g. 3.5 means home -3.5).
        Callers using the Odds API convention (negative => favored) must
        negate: home_line = -db_line if selection is home else +db_line.
        """
        base = self.predict(home, away)
        if base is None:
            return None
        sel = (selection or "").lower()
        if market == "spreads":
            p_home = cover_prob_home(base["pred_margin_home"], line,
                                     self.sigma_margin)
            return p_home if sel == "home" else 1.0 - p_home
        if market == "totals":
            p_over = over_prob(base["pred_total"], line, self.sigma_total)
            return p_over if sel == "over" else 1.0 - p_over
        return None


def load_challenger(version=None, artifact_dir=ARTIFACT_DIR):
    """Load a challenger artifact. Default: highest version present."""
    versions = sorted(
        d for d in os.listdir(artifact_dir)
        if d.startswith("challenger-") and
        os.path.isdir(os.path.join(artifact_dir, d))
    ) if os.path.isdir(artifact_dir) else []
    if not versions:
        raise FileNotFoundError("no challenger artifact found in %s" % artifact_dir)
    if version is None:
        version = versions[-1].replace("challenger-", "")
    path = os.path.join(artifact_dir, "challenger-%s" % version, "model.json")
    with open(path) as f:
        return Challenger(json.load(f))


def latest_version(artifact_dir=ARTIFACT_DIR):
    versions = sorted(
        d for d in os.listdir(artifact_dir)
        if d.startswith("challenger-") and
        os.path.isdir(os.path.join(artifact_dir, d))
    ) if os.path.isdir(artifact_dir) else []
    return versions[-1].replace("challenger-", "") if versions else None
