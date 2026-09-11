"""Standardized evaluation artifacts for the research tournament.

standard_report() turns raw walk-forward predictions and simulated bets into
the canonical artifact every model family must produce. Pure functions,
deterministic given seed, stdlib only. No I/O, no network.

Conventions:
  * predictions: list of dicts with keys task ('win'|'spread'|'total'), p, y,
    season, favorite (bool|None), home (bool|None), line_bucket, total_bucket,
    edge_bucket ('small'|'med'|'large'), early_season (bool). Extra keys
    (side, team, book, weekday, kickoff_bucket) enable optional slices.
  * bets: list of dicts with keys p (taken-side probability, >= 0.5),
    y (0/1 on the taken side), profit_u, edge_pp, season. Extra keys as above.
  * Brier/log-loss reuse model.challenger.validate (same clipping); the
    binned calibration regression replicates its binning exactly so slopes
    match the existing gate numbers.
"""
from __future__ import annotations

import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", ".."))

from model.challenger.validate import brier, log_loss

ARTIFACTS_VERSION = "1"
BREAKEVEN_110 = 110.0 / 210.0  # 0.52381
BOOTSTRAP_B = 1000

# Optional slice fields: picked up automatically when present and not None.
OPTIONAL_SLICE_FIELDS = ("team", "book", "weekday", "kickoff_bucket")


def _binned_points(ps, ys, bins=10):
    """Decile-style bins of (mean_p, obs_rate, n), same chunking as
    model.challenger.validate.calibration_slope. Returns None when n < 50."""
    order = sorted(zip(ps, ys))
    n = len(order)
    if n < 50:
        return None
    pts = []
    for i in range(bins):
        chunk = order[i * n // bins:(i + 1) * n // bins]
        if not chunk:
            continue
        pts.append((sum(p for p, _ in chunk) / len(chunk),
                    sum(y for _, y in chunk) / len(chunk),
                    len(chunk)))
    return pts


def calibration_fit(ps, ys, bins=10):
    """Slope and intercept of observed~predicted over binned means.
    Perfectly calibrated -> (1.0, 0.0). Returns (None, None) if n < 50."""
    pts = _binned_points(ps, ys, bins)
    if not pts:
        return None, None
    xs = [p[0] for p in pts]
    zs = [p[1] for p in pts]
    mx = sum(xs) / len(xs)
    mz = sum(zs) / len(zs)
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None, None
    slope = sum((x - mx) * (z - mz) for x, z in zip(xs, zs)) / den
    return slope, mz - slope * mx


def reliability_buckets(ps, ys, bins=10):
    """Decile reliability table: [{mean_p, obs_rate, n}]. Works below n=50
    by using fewer bins; bucket counts always sum to n."""
    order = sorted(zip(ps, ys))
    n = len(order)
    if n == 0:
        return []
    b = min(bins, max(1, n // 5))
    out = []
    for i in range(b):
        chunk = order[i * n // b:(i + 1) * n // b]
        if not chunk:
            continue
        out.append({
            "mean_p": round(sum(p for p, _ in chunk) / len(chunk), 4),
            "obs_rate": round(sum(y for _, y in chunk) / len(chunk), 4),
            "n": len(chunk),
        })
    return out


def _predictive(rows):
    if not rows:
        return {"n": 0}
    ps = [r["p"] for r in rows]
    ys = [r["y"] for r in rows]
    slope, intercept = calibration_fit(ps, ys)
    out = {
        "n": len(rows),
        "brier": round(brier(ps, ys), 4),
        "log_loss": round(log_loss(ps, ys), 4),
        "calibration_slope": None if slope is None else round(slope, 3),
        "calibration_intercept": None if intercept is None
        else round(intercept, 3),
        "reliability": reliability_buckets(ps, ys),
    }
    if all(r.get("base") is not None for r in rows):
        bs = [r["base"] for r in rows]
        out["base_brier"] = round(brier(bs, ys), 4)
        out["base_log_loss"] = round(log_loss(bs, ys), 4)
        out["beats_baseline"] = (out["log_loss"] < out["base_log_loss"]
                                 and out["brier"] < out["base_brier"])
    return out


def _betting_summary(bet_rows):
    n = len(bet_rows)
    if n == 0:
        return {"n_bets": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "profit_u": 0.0, "roi": 0.0, "avg_edge_pp": 0.0,
                "realized_edge_pp": 0.0, "max_drawdown_u": 0.0}
    wins = sum(1 for b in bet_rows if b["y"] == 1)
    profit = sum(b["profit_u"] for b in bet_rows)
    cum = 0.0
    peak = 0.0
    dd = 0.0
    for b in bet_rows:
        cum += b["profit_u"]
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    return {
        "n_bets": n,
        "wins": wins,
        "losses": n - wins,
        "win_rate": round(wins / n, 4),
        "profit_u": round(profit, 2),
        "roi": round(profit / n, 4),
        "avg_edge_pp": round(sum(b["edge_pp"] for b in bet_rows) / n, 2),
        "realized_edge_pp": round(
            sum((1 if b["y"] == 1 else 0) - BREAKEVEN_110
                for b in bet_rows) / n * 100.0, 2),
        "max_drawdown_u": round(dd, 2),
    }


def bootstrap_roi(bet_rows, B=BOOTSTRAP_B, seed=7):
    """Seeded bootstrap of per-1u ROI. Returns ([lo, hi] 95% CI, P(ROI>0)).
    Returns (None, None) when there are no bets."""
    n = len(bet_rows)
    if n == 0:
        return None, None
    rng = random.Random(seed)
    profits = [b["profit_u"] for b in bet_rows]
    rois = []
    for _ in range(B):
        rois.append(sum(profits[rng.randrange(n)] for _ in range(n)) / n)
    rois.sort()
    lo = rois[int(0.025 * B)]
    hi = rois[min(B - 1, int(0.975 * B))]
    p_gt0 = sum(1 for r in rois if r > 0) / B
    return [round(lo, 4), round(hi, 4)], round(p_gt0, 4)


def _slice_metrics(pred_rows, bet_rows):
    out = {"predictive": _predictive(pred_rows)}
    out["betting"] = _betting_summary(bet_rows)
    return out


def _robustness(predictions, bets):
    per_season = {}
    for b in bets:
        per_season.setdefault(b["season"], []).append(b)
    rob = {
        "per_season": {s: _betting_summary(rows)
                       for s, rows in sorted(per_season.items())},
        "per_season_by_task": {},
        "slices": {},
    }
    for _task in ("spread", "total"):
        _tb = [b for b in bets if b.get("task") == _task]
        if _tb:
            _per = {}
            for b in _tb:
                _per.setdefault(b["season"], []).append(b)
            rob["per_season_by_task"][_task] = {
                s: _betting_summary(rows)["roi"]
                for s, rows in sorted(_per.items())}
    slices = rob["slices"]

    def filt(rows, **kw):
        return [r for r in rows
                if all(r.get(k) == v for k, v in kw.items())]

    # Favorite vs underdog (spread rows carry favorite=True/False/None).
    sp = [r for r in predictions
          if r["task"] == "spread" and r.get("favorite") is not None]
    if sp:
        slices["fav_vs_dog"] = {
            "favorite": _slice_metrics(
                [r for r in sp if r["favorite"]],
                [b for b in bets if b.get("task") == "spread"
                 and b.get("favorite") is True]),
            "underdog": _slice_metrics(
                [r for r in sp if not r["favorite"]],
                [b for b in bets if b.get("task") == "spread"
                 and b.get("favorite") is False]),
        }

    # Preferred side: home vs away on the taken side.
    sided = [r for r in predictions if r.get("side") in ("home", "away")]
    if sided:
        slices["preferred_side"] = {
            side: _slice_metrics(
                [r for r in sided if r["side"] == side],
                [b for b in bets if b.get("side") == side])
            for side in ("home", "away")
        }

    # Line / total / edge buckets.
    for name, task, field in (("spread_bucket", "spread", "line_bucket"),
                              ("total_bucket", "total", "total_bucket"),
                              ("edge_bucket", None, "edge_bucket")):
        rows = [r for r in predictions
                if (task is None or r["task"] == task)
                and r.get(field) is not None]
        groups = sorted({r[field] for r in rows})
        if groups:
            slices[name] = {
                g: _slice_metrics(
                    [r for r in rows if r[field] == g],
                    [b for b in bets
                     if (task is None or b.get("task") == task)
                     and b.get(field) == g])
                for g in groups
            }

    # Early season (approx. weeks 1-6) vs late.
    if any("early_season" in r for r in predictions):
        slices["early_vs_late"] = {
            label: _slice_metrics(
                [r for r in predictions if r.get("early_season") is want],
                [b for b in bets if b.get("early_season") is want])
            for label, want in (("early_season", True), ("late_season", False))
        }

    # Optional slices activate when the field is present in the rows.
    for field in OPTIONAL_SLICE_FIELDS:
        rows = [r for r in predictions if r.get(field) is not None]
        if not rows:
            continue
        groups = sorted({r[field] for r in rows}, key=str)
        slices[field] = {
            str(g): _slice_metrics(
                [r for r in rows if r[field] == g],
                [b for b in bets if b.get(field) == g])
            for g in groups
        }
    return rob


def standard_report(predictions, bets, meta, seed=7):
    """Build the canonical evaluation artifact.

    predictions: list of {task, p, y, season, favorite, home, line_bucket,
        total_bucket, edge_bucket, early_season, ...}.
    bets: list of {p, y, profit_u, edge_pp, season, ...}.
    meta: dict describing the run (family, windows, edge_min, ...).
    seed: bootstrap RNG seed; same seed -> identical artifact.
    """
    predictions = list(predictions or [])
    bets = list(bets or [])

    predictive = {}
    for task in ("win", "spread", "total"):
        rows = [r for r in predictions if r["task"] == task]
        if rows:
            predictive[task] = _predictive(rows)

    betting = _betting_summary(bets)
    by_task = {}
    for task in ("spread", "total"):
        tb = [b for b in bets if b.get("task") == task]
        if tb:
            by_task[task] = _betting_summary(tb)
    if by_task:
        betting["by_task"] = by_task

    ci, p_gt0 = bootstrap_roi(bets, seed=seed)
    uncertainty = {
        "bootstrap_B": BOOTSTRAP_B,
        "seed": seed,
        "roi_ci_95": ci,
        "p_roi_gt_0": p_gt0,
    }
    if by_task:
        uncertainty["by_task"] = {}
        for task in ("spread", "total"):
            rows = [b for b in bets if b.get("task") == task]
            if rows:
                tci, tp = bootstrap_roi(rows, seed=seed)
                uncertainty["by_task"][task] = {"roi_ci_95": tci,
                                                "p_roi_gt_0": tp}

    return {
        "artifacts_version": ARTIFACTS_VERSION,
        "meta": dict(meta or {}),
        "predictive": predictive,
        "betting": betting,
        "robustness": _robustness(predictions, bets),
        "uncertainty": uncertainty,
    }
