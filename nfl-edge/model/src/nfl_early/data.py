from __future__ import annotations
from pathlib import Path
from io import BytesIO
import pandas as pd
import numpy as np

NFLVERSE_URLS = [
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet",
    "https://github.com/nflverse/nflfastR-data/releases/download/pbp/play_by_play_{season}.parquet",
]

def parse_seasons(spec: str) -> list[int]:
    spec = str(spec)
    if ":" in spec:
        a, b = spec.split(":", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x.strip()) for x in spec.split(",") if x.strip()]

def download_season(season: int, out_dir: str | Path) -> Path:
    import requests
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"play_by_play_{season}.parquet"
    if out.exists():
        return out
    last_err = None
    for tmpl in NFLVERSE_URLS:
        url = tmpl.format(season=season)
        try:
            r = requests.get(url, timeout=90)
            r.raise_for_status()
            out.write_bytes(r.content)
            # Verify parquet is readable before accepting it.
            pd.read_parquet(out, columns=["game_id"])
            return out
        except Exception as e:
            last_err = e
            if out.exists():
                out.unlink(missing_ok=True)
    raise RuntimeError(f"Could not download season {season}: {last_err}")

def load_pbp(data_dir: str | Path, seasons: list[int]) -> pd.DataFrame:
    frames = []
    for season in seasons:
        p = Path(data_dir) / f"play_by_play_{season}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"Missing {p}. Run the download command first.")
        import pyarrow.parquet as pq
        wanted = {'game_id','posteam','defteam','home_team','away_team','qtr','drive',
                  'play_id','season','season_type','week','game_date','game_seconds_remaining',
                  'fixed_drive_result','drive_end_transition','interception','fumble_lost',
                  'td_team','touchdown','field_goal_result','punt_attempt','fourth_down_failed',
                  'play_type','epa','success','spread_line'}
        cols = [c for c in pq.read_schema(p).names if c in wanted]
        frame = pd.read_parquet(p, columns=cols)
        frames.append(frame[frame.qtr.isin([1,2])].copy())
    if not frames:
        raise ValueError("No seasons supplied")
    x = pd.concat(frames, ignore_index=True)
    if "season_type" in x:
        x = x[x["season_type"].eq("REG")].copy()
    return x

def _last_nonnull(s: pd.Series):
    z = s.dropna()
    return z.iloc[-1] if len(z) else np.nan

def _map_drive_result(result: object, g: pd.DataFrame, posteam: str) -> str:
    text = "" if pd.isna(result) else str(result).lower()

    # Prefer fixed_drive_result when present.
    if "touchdown" in text:
        # Defensive TDs can end an offensive drive; interception/fumble gets precedence.
        if g.get("interception", pd.Series(0, index=g.index)).fillna(0).max() > 0:
            return "TO"
        if g.get("fumble_lost", pd.Series(0, index=g.index)).fillna(0).max() > 0:
            return "TO"
        td_team = _last_nonnull(g["td_team"]) if "td_team" in g else np.nan
        if pd.isna(td_team) or str(td_team) == str(posteam):
            return "TD"
    if "field goal" in text or "field_goal" in text:
        if "field_goal_result" in g and (g["field_goal_result"].astype(str).str.lower() == "made").any():
            return "FG"
        if "made" in text:
            return "FG"
    if "punt" in text:
        return "PUNT"
    if any(k in text for k in ["turnover", "interception", "fumble"]):
        return "TO"
    if any(k in text for k in ["downs", "turnover on downs"]):
        return "DOWNS"

    # Fallback from play flags.
    if "td_team" in g and (g["td_team"].astype(str) == str(posteam)).any():
        return "TD"
    if "touchdown" in g and g["touchdown"].fillna(0).max() > 0:
        if not (
            g.get("interception", pd.Series(0, index=g.index)).fillna(0).max() > 0
            or g.get("fumble_lost", pd.Series(0, index=g.index)).fillna(0).max() > 0
        ):
            return "TD"
    if "field_goal_result" in g and (g["field_goal_result"].astype(str).str.lower() == "made").any():
        return "FG"
    if "punt_attempt" in g and g["punt_attempt"].fillna(0).max() > 0:
        return "PUNT"
    if (
        ("interception" in g and g["interception"].fillna(0).max() > 0)
        or ("fumble_lost" in g and g["fumble_lost"].fillna(0).max() > 0)
    ):
        return "TO"
    if "fourth_down_failed" in g and g["fourth_down_failed"].fillna(0).max() > 0:
        return "DOWNS"
    return "OTHER"

def build_drive_table(pbp: pd.DataFrame) -> pd.DataFrame:
    """
    Creates drives that START in Q1. Plays from Q2 are retained so a drive that crosses
    the quarter boundary receives its real terminal outcome and total game-clock duration.
    """
    required = ["game_id", "posteam", "defteam", "home_team", "away_team", "qtr", "drive"]
    missing = [c for c in required if c not in pbp.columns]
    if missing:
        raise ValueError(f"PBP is missing columns: {missing}")

    x = pbp[pbp["qtr"].isin([1, 2]) & pbp["posteam"].notna() & pbp["drive"].notna()].copy()
    sort_cols = [c for c in ["season", "week", "game_id", "play_id"] if c in x]
    x = x.sort_values(sort_cols)

    records = []
    meta_cols = [
        "season", "week", "game_date", "home_team", "away_team", "spread_line",
        "total_line", "roof", "surface", "temp", "wind", "home_coach", "away_coach"
    ]

    for (game_id, drive_id), g in x.groupby(["game_id", "drive"], sort=False):
        g = g.sort_values("play_id") if "play_id" in g else g
        start_qtr = int(pd.to_numeric(g["qtr"], errors="coerce").dropna().iloc[0])
        if start_qtr != 1:
            continue
        posteam = str(g["posteam"].dropna().iloc[0])
        defteam = str(g["defteam"].dropna().iloc[0])

        if "fixed_drive_result" in g:
            result_raw = _last_nonnull(g["fixed_drive_result"])
        elif "drive_end_transition" in g:
            result_raw = _last_nonnull(g["drive_end_transition"])
        else:
            result_raw = np.nan

        start_sec = pd.to_numeric(g.get("game_seconds_remaining"), errors="coerce").dropna()
        duration = np.nan
        if len(start_sec):
            duration = max(1.0, float(start_sec.iloc[0] - start_sec.iloc[-1]))

        scrimmage = g[g.get("play_type", pd.Series("", index=g.index)).isin(["run", "pass", "qb_kneel", "qb_spike"])]
        epa = pd.to_numeric(scrimmage.get("epa"), errors="coerce").mean() if "epa" in scrimmage else np.nan
        success = pd.to_numeric(scrimmage.get("success"), errors="coerce").mean() if "success" in scrimmage else np.nan
        n_plays = int(len(scrimmage))

        row = {
            "game_id": game_id,
            "drive_id": drive_id,
            "posteam": posteam,
            "defteam": defteam,
            "outcome": _map_drive_result(result_raw, g, posteam),
            "duration": duration,
            "epa_per_play": epa,
            "success_rate": success,
            "plays": n_plays,
        }
        for c in meta_cols:
            if c in g:
                row[c] = _last_nonnull(g[c])

        records.append(row)

    d = pd.DataFrame(records)
    if d.empty:
        raise ValueError("No Q1-starting drives were produced")

    order_cols = [c for c in ["season", "week", "game_id", "drive_id"] if c in d]
    d = d.sort_values(order_cols)
    d["drive_index"] = d.groupby(["game_id", "posteam"]).cumcount() + 1
    d["opening_drive"] = (d["drive_index"] == 1).astype(int)
    d["is_td"] = d["outcome"].eq("TD").astype(float)
    d["is_score"] = d["outcome"].isin(["TD", "FG"]).astype(float)
    d["is_home"] = d["posteam"].eq(d["home_team"]).astype(int)

    # nflfastR spread_line: positive means home favored.
    spread = pd.to_numeric(d.get("spread_line"), errors="coerce")
    d["team_favored_by"] = np.where(d["is_home"].eq(1), spread, -spread)

    # Coach mapping for offense and opposing defense.
    if "home_coach" in d and "away_coach" in d:
        d["coach"] = np.where(d["is_home"].eq(1), d["home_coach"], d["away_coach"])
        d["opp_coach"] = np.where(d["is_home"].eq(1), d["away_coach"], d["home_coach"])
    else:
        d["coach"] = "UNKNOWN"
        d["opp_coach"] = "UNKNOWN"
    return d
