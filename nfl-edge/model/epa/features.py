"""EPA-v1 game-state feature store.

Reusable football-intelligence layer: point-in-time team features from
nflverse play-by-play. Built for MANY markets (spreads, totals, team
totals, 1H/1Q totals, drive models, props) -- it is not a spread model.

Architecture:
  load(seasons) reads one season file at a time and collapses it into two
  small per-team-game tables (team_games, qb_games). Raw plays are discarded,
  so memory stays flat in the number of seasons.
  team_features(team, as_of) aggregates the last 8 games strictly before
  `as_of`, applies opponent adjustment (precomputed at load) and shrinkage.

Conventions:
  - EPA/play features are play-weighted (sums over sums), not averages of
    game averages.
  - Defensive EPA features are mean EPA of opponent plays: NEGATIVE is good.
  - "success" is the nflverse 50/70/100% yards-to-go definition.
  - Explosive play: EPA > 1.0.
"""
import datetime as dt
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

FEATURE_VERSION = "1"
SHRINK_K = 500        # plays for EPA-rate shrinkage
SHRINK_K_QB = 150     # dropbacks for QB shrinkage
ROLL_GAMES = 8        # rolling window (games)
MIN_PLAYS_NOTE = 1    # minimum games; documented, not enforced as plays

USECOLS = ["game_id", "game_date", "season", "week", "season_type",
           "home_team", "away_team", "posteam", "defteam", "play_id",
           "play_type", "epa", "success", "down", "ydstogo", "yardline_100",
           "qb_dropback", "qb_kneel", "qb_spike", "rush_attempt",
           "pass_attempt", "sack", "passer_player_id", "passer_player_name",
           "interception", "fumble_lost", "game_seconds_remaining",
           "half_seconds_remaining", "score_differential"]

SCRIMMAGE_TYPES = ("pass", "run")


def feature_names():
    """Ordered list of numeric/categorical features team_features returns."""
    return [
        "off_epa_play", "def_epa_play",
        "off_epa_dropback", "def_epa_dropback",
        "off_epa_rush", "def_epa_rush",
        "off_success_rate", "def_success_rate",
        "off_explosive_rate", "def_explosive_rate",
        "off_early_down_epa", "def_early_down_epa",
        "off_redzone_epa", "def_redzone_epa",
        "pace_sec_per_play",
        "neutral_pass_rate",
        "off_sack_rate", "def_sack_rate",
        "off_turnover_rate", "def_turnover_rate",
        "off_epa_prior", "def_epa_prior",
        "rest_days",
        "expected_qb_id", "expected_qb_name",
        "qb_epa_dropback", "qb_changed_3g",
        "n_games", "n_plays",
    ]


def shrink(value, n, prior, k=SHRINK_K):
    """Blend a rolling estimate toward a prior: w*n/(n+k).

    Early in a season n is small so the feature demonstrably shrinks
    toward the prior (Weeks 1-5 behavior is driven by the prior).
    """
    if value is None or prior is None:
        return value if prior is None else prior
    w = n / (n + k)
    return w * value + (1.0 - w) * prior


def _is_scrimmage(df):
    return (df["play_type"].isin(SCRIMMAGE_TYPES)
            & (df["qb_kneel"].fillna(0) == 0)
            & (df["qb_spike"].fillna(0) == 0)
            & df["epa"].notna())


def _unit_stats(plays):
    """Aggregate one team's one-sided plays (offense OR defense).

    `plays` must already be filtered to that unit's scrimmage plays.
    Returns a dict of sums/counts; rates are formed by the caller.
    """
    n = len(plays)
    if n == 0:
        return None
    epa = plays["epa"]
    db = plays[plays["qb_dropback"].fillna(0) == 1]
    ru = plays[plays["rush_attempt"].fillna(0) == 1]
    early = plays[plays["down"].isin([1, 2])]
    rz = plays[plays["yardline_100"] <= 20]
    succ = plays["success"].dropna()
    return {
        "n": n,
        "epa_sum": float(epa.sum()),
        "db_n": int(len(db)),
        "db_epa_sum": float(db["epa"].sum()) if len(db) else 0.0,
        "rush_n": int(len(ru)),
        "rush_epa_sum": float(ru["epa"].sum()) if len(ru) else 0.0,
        "succ_n": int(len(succ)),
        "succ_sum": float(succ.sum()) if len(succ) else 0.0,
        "explosive": int((epa > 1.0).sum()),
        "early_n": int(len(early)),
        "early_epa_sum": float(early["epa"].sum()) if len(early) else 0.0,
        "rz_n": int(len(rz)),
        "rz_epa_sum": float(rz["epa"].sum()) if len(rz) else 0.0,
        "sacks": int((plays["sack"].fillna(0) == 1).sum()),
        "turnovers": int(((plays["interception"].fillna(0) == 1)
                          | (plays["fumble_lost"].fillna(0) == 1)).sum()),
    }


def _pace_for_game(game):
    """Median seconds between consecutive plays, attributed to the offense.

    v1 proxy from the game clock: dt between a play and the previous play in
    the same half, 0 < dt <= 120s. Returns {posteam: median_seconds}.
    """
    g = game.sort_values("play_id")
    prev_gsr = g["game_seconds_remaining"].shift(1)
    prev_hsr = g["half_seconds_remaining"].shift(1)
    dt_s = prev_gsr - g["game_seconds_remaining"]
    same_half = g["half_seconds_remaining"] <= prev_hsr.fillna(10 ** 9)
    ok = dt_s.between(1, 120) & same_half.fillna(False)
    out = {}
    for team, grp in g[ok].groupby("posteam"):
        if team is None or (isinstance(team, float) and team != team):
            continue
        dts = (prev_gsr[grp.index] - grp["game_seconds_remaining"])
        dts = dts[(dts >= 1) & (dts <= 120)]
        if len(dts):
            out[team] = float(dts.median())
    return out


def _neutral_pass(plays):
    """(pass plays, total plays) in neutral situations.

    v1: 1st/2nd down, |score differential| <= 14, >120s left in the half.
    """
    sd = plays["score_differential"].fillna(0).abs()
    hsr = plays["half_seconds_remaining"].fillna(0)
    neut = plays[plays["down"].isin([1, 2]) & (sd <= 14) & (hsr > 120)]
    if len(neut) == 0:
        return 0, 0
    return int((neut["pass_attempt"].fillna(0) == 1).sum()), int(len(neut))


def summarize_season(pbp):
    """Collapse one season of plays into per-team-game tables.

    Returns (team_games, qb_games) as lists of dicts. Pure function of the
    plays frame: safe to unit-test on synthetic data.
    """
    pbp = pbp[pbp["season_type"] == "REG"].copy()
    pbp["game_date"] = pbp["game_date"].astype(str).str[:10]
    scr = pbp[_is_scrimmage(pbp)].copy()

    team_rows, qb_rows = [], []
    for game_id, game in scr.groupby("game_id", sort=False):
        gdate = game["game_date"].iloc[0]
        season = int(game["season"].iloc[0])
        week = int(game["week"].iloc[0])
        home, away = game["home_team"].iloc[0], game["away_team"].iloc[0]
        pace = _pace_for_game(game)
        for team in (home, away):
            opp = away if team == home else home
            off = game[game["posteam"] == team]
            deff = game[game["defteam"] == team]
            o, d = _unit_stats(off), _unit_stats(deff)
            if o is None or d is None:
                continue
            npass, nneut = _neutral_pass(off)
            row = {
                "season": season, "week": week, "game_id": game_id,
                "game_date": gdate, "team": team, "opp": opp,
                "home": team == home,
                "pace": pace.get(team),
                "neutral_pass": npass, "neutral_n": nneut,
            }
            for prefix, u in (("off", o), ("def", d)):
                for k, v in u.items():
                    row["%s_%s" % (prefix, k)] = v
            team_rows.append(row)
            qbg = off[off["passer_player_id"].notna()]
            for pid, pg in qbg.groupby("passer_player_id", sort=False):
                pdb = pg[pg["qb_dropback"].fillna(0) == 1]
                qb_rows.append({
                    "season": season, "game_id": game_id, "game_date": gdate,
                    "team": team, "passer_id": str(pid),
                    "passer_name": pg["passer_player_name"].iloc[0],
                    "attempts": int((pg["pass_attempt"].fillna(0) == 1).sum()),
                    "dropbacks": int(len(pdb)),
                    "epa_sum": float(pdb["epa"].sum()) if len(pdb) else 0.0,
                })
    return team_rows, qb_rows


def opponent_adjust(team_games):
    """One-step strength-of-schedule adjustment (v1 approximation).

    team_games: list of dicts with keys season/team/opp/game_date/off_n,
    off_epa_sum, def_n, def_epa_sum. Adds adj_off_epa / adj_def_epa per row:
      adj_off = raw_off - (opp_def_std - lg_def_avg)
      adj_def = raw_def - (opp_off_std - lg_off_avg)
    where opp_*_std is the opponent's season-to-date per-play mean before
    this game and lg_*_avg is the league season-to-date mean. Pure function;
    unit-testable on synthetic data.
    """
    tg = pd.DataFrame(team_games)
    if tg.empty:
        return team_games
    tg["game_date"] = pd.to_datetime(tg["game_date"])
    tg["raw_off"] = tg["off_epa_sum"] / tg["off_n"].replace(0, float("nan"))
    tg["raw_def"] = tg["def_epa_sum"] / tg["def_n"].replace(0, float("nan"))
    by_team = {}
    for (season, team), grp in tg.groupby(["season", "team"]):
        by_team[(season, team)] = grp.sort_values("game_date")[
            ["game_date", "raw_off", "raw_def"]].reset_index(drop=True)
    lg = tg.groupby("season")
    adj_off, adj_def = [], []
    for _, r in tg.iterrows():
        season, date = r["season"], r["game_date"]
        prior_lg = tg[(tg["season"] == season) & (tg["game_date"] < date)]
        lg_off = prior_lg["raw_off"].mean() if len(prior_lg) else 0.0
        lg_def = prior_lg["raw_def"].mean() if len(prior_lg) else 0.0
        opp = by_team.get((season, r["opp"]))
        if opp is not None:
            oh = opp[opp["game_date"] < date]
            o_off = oh["raw_off"].mean() if len(oh) else lg_off
            o_def = oh["raw_def"].mean() if len(oh) else lg_def
        else:
            o_off, o_def = lg_off, lg_def
        o_off = lg_off if pd.isna(o_off) else o_off
        o_def = lg_def if pd.isna(o_def) else o_def
        lg_off = 0.0 if pd.isna(lg_off) else lg_off
        lg_def = 0.0 if pd.isna(lg_def) else lg_def
        raw_off = 0.0 if pd.isna(r["raw_off"]) else r["raw_off"]
        raw_def = 0.0 if pd.isna(r["raw_def"]) else r["raw_def"]
        adj_off.append(raw_off - (o_def - lg_def))
        adj_def.append(raw_def - (o_off - lg_off))
    tg["adj_off_epa"] = adj_off
    tg["adj_def_epa"] = adj_def
    tg["game_date"] = tg["game_date"].dt.strftime("%Y-%m-%d")
    return tg.drop(columns=["raw_off", "raw_def"]).to_dict("records")


class TeamFeatureStore:
    """Point-in-time team feature store over nflverse play-by-play."""

    def __init__(self):
        self.team_games = None   # DataFrame, one row per team-game
        self.qb_games = None     # DataFrame, one row per team-game-passer
        self.season_finals = {}  # (team, season) -> {"off": x, "def": y}
        self._seasons = []

    def load(self, seasons, cache_dir=None, progress=None):
        """Load seasons (iterable of ints) into the store. Chronological."""
        from model.epa import ingest as ingest_mod
        cache_dir = cache_dir or ingest_mod.CACHE_DIR
        paths = ingest_mod.fetch_pbp(list(seasons), cache_dir)
        tg_all, qb_all = [], []
        for path in paths:
            if progress:
                progress(path)
            if path.endswith(".parquet"):
                pbp = pd.read_parquet(path, columns=USECOLS)
            else:
                pbp = pd.read_csv(path, usecols=USECOLS,
                                  compression="infer", low_memory=False)
            t_rows, q_rows = summarize_season(pbp)
            tg_all.extend(t_rows)
            qb_all.extend(q_rows)
            del pbp
        tg_all = opponent_adjust(tg_all)
        tg = pd.DataFrame(tg_all)
        tg["game_date"] = pd.to_datetime(tg["game_date"])
        tg = tg.sort_values(["game_date", "team"]).reset_index(drop=True)
        # Rest days from the team's own game dates (cross-season).
        tg["rest_days"] = tg.groupby("team")["game_date"].diff().dt.days
        qb = pd.DataFrame(qb_all)
        if len(qb):
            qb["game_date"] = pd.to_datetime(qb["game_date"])
            qb = qb.sort_values(["game_date", "team"]).reset_index(drop=True)
        # Prior-season finals: play-weighted adj EPA per (team, season).
        for (team, season), grp in tg.groupby(["team", "season"]):
            woff = (grp["adj_off_epa"] * grp["off_n"]).sum() \
                / grp["off_n"].sum() if grp["off_n"].sum() else 0.0
            wdef = (grp["adj_def_epa"] * grp["def_n"]).sum() \
                / grp["def_n"].sum() if grp["def_n"].sum() else 0.0
            self.season_finals[(team, int(season))] = {
                "off": float(woff), "def": float(wdef)}
        self.team_games = tg
        self.qb_games = qb
        self._seasons = sorted(tg["season"].unique().tolist())
        return self

    # -- internals ----------------------------------------------------
    def _history(self, team, as_of):
        as_of = pd.Timestamp(as_of) if not hasattr(as_of, "year") \
            else pd.Timestamp(as_of.year, as_of.month, as_of.day)
        tg = self.team_games
        h = tg[(tg["team"] == team) & (tg["game_date"] < as_of)]
        if len(h) == 0:
            return h
        # Same-season window only: last season's information enters through
        # the prior (50% prior-season final), not the rolling window.
        season = h.sort_values("game_date").iloc[-1]["season"]
        return h[h["season"] == season].tail(ROLL_GAMES)

    def _league_avg(self, as_of, col="adj_off_epa"):
        tg = self.team_games
        as_of = pd.Timestamp(as_of)
        prior = tg[tg["game_date"] < as_of]
        if len(prior) == 0:
            return 0.0
        return float(prior[col].mean())

    def _prior(self, team, as_of, side):
        """50% league-average-to-date + 50% team's prior-season final."""
        lg = self._league_avg(as_of, "adj_off_epa" if side == "off"
                              else "adj_def_epa")
        season = pd.Timestamp(as_of).year
        # Season label follows the fall: use the latest loaded season <= year.
        prev = max([s for s in self._seasons if s < season], default=None)
        team_final = self.season_finals.get((team, prev), {}).get(side)
        if team_final is None:
            team_final = lg
        return 0.5 * lg + 0.5 * team_final

    @staticmethod
    def _wmean(hist, val_col, w_col):
        w = hist[w_col].sum()
        if w == 0:
            return None, 0
        return float((hist[val_col] * hist[w_col]).sum() / w), int(w)

    def _rate(self, hist, num_col, den_col, prior, k=SHRINK_K):
        num = hist[num_col].sum()
        den = hist[den_col].sum()
        raw = float(num) / den if den else None
        return shrink(raw, int(den), prior, k)

    # -- public API ---------------------------------------------------
    def team_features(self, team, as_of):
        """Feature dict for `team` using only games before `as_of`.

        Returns None when the team has no history before as_of.
        """
        if self.team_games is None:
            raise RuntimeError("store not loaded: call load() first")
        hist = self._history(team, as_of)
        if len(hist) == 0:
            return None
        as_of_ts = pd.Timestamp(as_of)
        n_games = len(hist)
        off_prior = self._prior(team, as_of_ts, "off")
        def_prior = self._prior(team, as_of_ts, "def")

        def shrunk_mean(prefix, prior, k=SHRINK_K):
            raw, n = self._wmean(hist, "adj_%s_epa" % prefix,
                                 "%s_n" % prefix)
            return shrink(raw, n, prior, k), raw, n

        f = {}
        f["off_epa_play"], _, _ = shrunk_mean("off", off_prior)
        f["def_epa_play"], _, _ = shrunk_mean("def", def_prior)

        # Dropback / rush splits (adj by the same opponent logic is v1-simple:
        # splits use raw means shrunk toward split priors).
        for prefix, prior in (("off", off_prior), ("def", def_prior)):
            for split in ("db", "rush"):
                ncol, scol = "%s_%s_n" % (prefix, split), \
                    "%s_%s_epa_sum" % (prefix, split)
                num = hist[scol].sum()
                den = hist[ncol].sum()
                raw = float(num) / den if den else None
                f["%s_epa_%s"
                  % (prefix, "dropback" if split == "db" else "rush")] = \
                    shrink(raw, int(den), prior, k=200)

        lg = self._league_avg(as_of_ts)
        f["off_success_rate"] = self._rate(hist, "off_succ_sum",
                                           "off_succ_n", 0.45)
        f["def_success_rate"] = self._rate(hist, "def_succ_sum",
                                           "def_succ_n", 0.45)
        f["off_explosive_rate"] = self._rate(hist, "off_explosive", "off_n",
                                             0.10)
        f["def_explosive_rate"] = self._rate(hist, "def_explosive", "def_n",
                                             0.10)
        f["off_early_down_epa"] = self._rate(hist, "off_early_epa_sum",
                                             "off_early_n", off_prior, 200)
        f["def_early_down_epa"] = self._rate(hist, "def_early_epa_sum",
                                             "def_early_n", def_prior, 200)
        f["off_redzone_epa"] = self._rate(hist, "off_rz_epa_sum", "off_rz_n",
                                          off_prior, 120)
        f["def_redzone_epa"] = self._rate(hist, "def_rz_epa_sum", "def_rz_n",
                                          def_prior, 120)
        pace_vals = hist["pace"].dropna()
        f["pace_sec_per_play"] = shrink(
            float(pace_vals.median()) if len(pace_vals) else None,
            len(pace_vals) * 60, 28.0, k=300)
        f["neutral_pass_rate"] = self._rate(hist, "neutral_pass",
                                            "neutral_n", 0.55, k=120)
        f["off_sack_rate"] = self._rate(hist, "off_sacks", "off_db_n", 0.065,
                                        k=120)
        f["def_sack_rate"] = self._rate(hist, "def_sacks", "def_db_n", 0.065,
                                        k=120)
        f["off_turnover_rate"] = self._rate(hist, "off_turnovers", "off_n",
                                            0.025, k=300)
        f["def_turnover_rate"] = self._rate(hist, "def_turnovers", "def_n",
                                            0.025, k=300)
        f["off_epa_prior"] = round(off_prior, 4)
        f["def_epa_prior"] = round(def_prior, 4)
        rest = hist["rest_days"].iloc[-1]
        f["rest_days"] = None if pd.isna(rest) else int(rest)

        # -- QB (first-class, v1 proxy) -------------------------------
        qb = self.qb_games
        qh = qb[(qb["team"] == team) & (qb["game_date"] < as_of_ts)] \
            if qb is not None and len(qb) else qb.iloc[0:0]
        f["expected_qb_id"] = f["expected_qb_name"] = None
        f["qb_epa_dropback"] = None
        f["qb_changed_3g"] = False
        if len(qh):
            game_ids = qh.sort_values("game_date")["game_id"].unique()[-3:]
            last3 = qh[qh["game_id"].isin(game_ids)]
            by_qb = last3.groupby(["passer_id", "passer_name"]).agg(
                attempts=("attempts", "sum"), dropbacks=("dropbacks", "sum"),
                epa=("epa_sum", "sum")).reset_index()
            if len(by_qb):
                top = by_qb.sort_values("attempts", ascending=False).iloc[0]
                f["expected_qb_id"] = top["passer_id"]
                f["expected_qb_name"] = top["passer_name"]
                db = int(top["dropbacks"])
                raw_qb = float(top["epa"]) / db if db else None
                f["qb_epa_dropback"] = shrink(raw_qb, db, lg, k=SHRINK_K_QB)
                games = qh.sort_values("game_date")["game_id"].unique()
                if len(games) >= 2:
                    def top_passer(gid):
                        gg = qh[qh["game_id"] == gid]
                        return gg.sort_values(
                            "attempts", ascending=False)["passer_id"].iloc[0]
                    f["qb_changed_3g"] = (
                        top_passer(games[-1]) != top_passer(games[-3])
                        if len(games) >= 3 else
                        top_passer(games[-1]) != top_passer(games[0]))

        f["n_games"] = n_games
        f["n_plays"] = int(hist["off_n"].sum())
        out = {"feature_version": FEATURE_VERSION}
        for k in feature_names():
            v = f.get(k)
            out[k] = round(v, 4) if isinstance(v, float) else v
        return out
