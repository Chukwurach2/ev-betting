from __future__ import annotations
import numpy as np
import pandas as pd

METRICS = [
    "fd_td_rate",
    "fd_score_rate",
    "q1_td_rate",
    "q1_score_rate",
    "q1_epa",
    "q1_success",
    "q1_duration",
    "q1_plays",
]

def _team_game_realized(drives: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (game_id, team), g in drives.groupby(["game_id", "posteam"], sort=False):
        g = g.sort_values("drive_index")
        first = g.iloc[0]
        row = {
            "game_id": game_id,
            "team": team,
            "opponent": first["defteam"],
            "season": first.get("season", np.nan),
            "week": first.get("week", np.nan),
            "game_date": first.get("game_date", np.nan),
            "fd_td_rate": float(first["outcome"] == "TD"),
            "fd_score_rate": float(first["outcome"] in ("TD", "FG")),
            "q1_td_rate": float(g["outcome"].eq("TD").mean()),
            "q1_score_rate": float(g["outcome"].isin(["TD", "FG"]).mean()),
            "q1_epa": float(pd.to_numeric(g["epa_per_play"], errors="coerce").mean()),
            "q1_success": float(pd.to_numeric(g["success_rate"], errors="coerce").mean()),
            "q1_duration": float(pd.to_numeric(g["duration"], errors="coerce").mean()),
            "q1_plays": float(pd.to_numeric(g["plays"], errors="coerce").mean()),
        }
        rows.append(row)
    t = pd.DataFrame(rows)

    # Defensive realized metric is the opponent's offensive realization in the same game.
    opp = t[["game_id", "team"] + METRICS].copy()
    opp = opp.rename(columns={"team": "opponent", **{m: f"def_{m}" for m in METRICS}})
    t = t.merge(opp, on=["game_id", "opponent"], how="left")
    return t

def _league_defaults(tg: pd.DataFrame) -> dict[str, float]:
    defaults = {}
    for m in METRICS:
        defaults[f"off_{m}"] = float(pd.to_numeric(tg[m], errors="coerce").mean())
        defaults[f"def_{m}"] = float(pd.to_numeric(tg[f"def_{m}"], errors="coerce").mean())
    # Safe fallbacks.
    fallbacks = {
        "off_fd_td_rate": .20, "off_fd_score_rate": .34,
        "off_q1_td_rate": .20, "off_q1_score_rate": .34,
        "off_q1_epa": 0.0, "off_q1_success": .43,
        "off_q1_duration": 145.0, "off_q1_plays": 6.0,
    }
    for k, v in list(fallbacks.items()):
        defaults[k] = defaults.get(k) if np.isfinite(defaults.get(k, np.nan)) else v
        dk = k.replace("off_", "def_")
        defaults[dk] = defaults.get(dk) if np.isfinite(defaults.get(dk, np.nan)) else v
    return defaults

def build_pregame_states(drives: pd.DataFrame, alpha: float = 0.28):
    """
    Produces leakage-safe states. Each game's state is based only on prior games.
    """
    tg = _team_game_realized(drives)
    sort_cols = [c for c in ["season", "week", "game_date", "game_id"] if c in tg]
    tg = tg.sort_values(sort_cols).reset_index(drop=True)
    # Cold-start priors must not incorporate future validation outcomes.
    defaults = {prefix + metric: value for prefix in ('off_', 'def_')
                for metric, value in zip(METRICS, (.20, .34, .20, .34, 0., .43, 145., 6.))}

    states = []
    latest = {}
    # state_by_team stores exponentially smoothed realized metrics AFTER previous games.
    state_by_team: dict[str, dict[str, float]] = {}

    for _, r in tg.iterrows():
        team = r["team"]
        prior = state_by_team.get(team, {}).copy()
        pre = {"game_id": r["game_id"], "team": team}

        for m in METRICS:
            pre[f"off_{m}"] = prior.get(f"off_{m}", defaults[f"off_{m}"])
            pre[f"def_{m}"] = prior.get(f"def_{m}", defaults[f"def_{m}"])
        states.append(pre)

        # Update AFTER recording pregame values.
        new_state = prior.copy()
        for m in METRICS:
            for prefix, col in [("off_", m), ("def_", f"def_{m}")]:
                k = prefix + m
                obs = pd.to_numeric(pd.Series([r.get(col)]), errors="coerce").iloc[0]
                old = new_state.get(k, defaults[k])
                if np.isfinite(obs):
                    new_state[k] = alpha * float(obs) + (1 - alpha) * float(old)
        state_by_team[team] = new_state
        latest[team] = new_state.copy()

    state_df = pd.DataFrame(states)
    out = drives.merge(
        state_df.add_prefix("offstate_"),
        left_on=["game_id", "posteam"],
        right_on=["offstate_game_id", "offstate_team"],
        how="left",
    )
    out = out.merge(
        state_df.add_prefix("defstate_"),
        left_on=["game_id", "defteam"],
        right_on=["defstate_game_id", "defstate_team"],
        how="left",
    )

    # Offense uses its offensive state; defense uses opponent's defensive state.
    for m in METRICS:
        out[f"off_{m}"] = out[f"offstate_off_{m}"]
        out[f"def_{m}"] = out[f"defstate_def_{m}"]

    drop = [c for c in out.columns if c.startswith("offstate_") or c.startswith("defstate_")]
    out = out.drop(columns=drop)
    return out, latest, defaults

def future_drive_features(
    matchup: dict,
    team: str,
    opponent: str,
    team_state: dict,
    opp_state: dict,
    defaults: dict,
    drive_index: int,
) -> dict:
    home = team == matchup["home_team"]
    favored = float(matchup.get("home_favored_by", 0.0))
    row = {
        "is_home": int(home),
        "team_favored_by": favored if home else -favored,
        "total_line": float(matchup.get("total_line", 44.0)),
        "wind": float(matchup.get("wind", 0.0) or 0.0),
        "temp": float(matchup.get("temp", 65.0) or 65.0),
        "roof": str(matchup.get("roof", "unknown") or "unknown"),
        "surface": str(matchup.get("surface", "unknown") or "unknown"),
        "coach": str(matchup.get("home_coach" if home else "away_coach", "UNKNOWN") or "UNKNOWN"),
        "opp_coach": str(matchup.get("away_coach" if home else "home_coach", "UNKNOWN") or "UNKNOWN"),
        "drive_index": int(drive_index),
        "opening_drive": int(drive_index == 1),
    }
    for m in METRICS:
        row[f"off_{m}"] = float(team_state.get(f"off_{m}", defaults[f"off_{m}"]))
        row[f"def_{m}"] = float(opp_state.get(f"def_{m}", defaults[f"def_{m}"]))
    return row
