"""Automated model governance: promotion and demotion decisions.

Thresholds are CONFIGURABLE (DEFAULT_CONFIG); there are no hidden constants.
A promotion requires every check to pass. A demotion moves a model back one
or more steps on deteriorating rolling evidence. Governance never deletes a
model, a version, or an evaluation artifact: poor evidence demotes, it does
not erase. Failed experiments stay in the ledger permanently.

evaluate_promotion consumes the standardized evaluation artifact produced by
the research lab (predictive / betting / robustness / uncertainty sections).
It reads defensively: missing sections fail their checks, and the reasons
name exactly what is missing.

Decisions:
  promote  every check passes.
  reject   roi <= 0 AND the model fails to beat its baseline (evidence
           against, not just absence of evidence).
  hold     anything else (inconclusive or incomplete).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                ".."))

DEFAULT_CONFIG = {
    "min_bets": 500,
    "min_seasons": 3,
    "min_season_win_frac": 0.6,
    "cal_slope_lo": 0.8,
    "cal_slope_hi": 1.2,
    "min_p_roi_gt_0": 0.9,
    "min_edge_pp": 0.0,
    "require_beats_baseline": True,
    # Multiple-testing protection: a statistically positive but tiny edge
    # does not promote. realized_edge_pp must clear this bar.
    "min_effect_size_pp": 1.0,
    # Final validation must come from seasons never used during development.
    # evaluation["holdout"] = {"n_bets", "roi", "p_roi_gt_0"} on those seasons.
    "min_holdout_bets": 200,
}


def _task_for_market(market_key):
    """Predictive task name for a market key, or None if unknown."""
    k = market_key or ""
    if "spread" in k:
        return "spread"
    if "total" in k:
        return "total"
    if "moneyline" in k or k == "win":
        return "win"
    return None


def _season_rois(robustness, task=None):
    """Normalize the per-season robustness section to {season: roi}.

    Prefers the task-specific table when the artifact carries one.
    """
    if not isinstance(robustness, dict):
        return {}
    if task:
        by_task = robustness.get("per_season_by_task") or {}
        if isinstance(by_task.get(task), dict):
            return {s: r for s, r in by_task[task].items()
                    if r is not None}
    per = robustness.get("per_season")
    out = {}
    if isinstance(per, dict):
        for season, m in per.items():
            if isinstance(m, dict) and m.get("roi") is not None:
                out[season] = m["roi"]
            elif isinstance(m, dict):
                bets = m.get("bets") or {}
                if isinstance(bets, dict) and bets.get("roi") is not None:
                    out[season] = bets["roi"]
    elif isinstance(per, list):
        for m in per:
            if isinstance(m, dict) and m.get("season") is not None \
                    and m.get("roi") is not None:
                out[m["season"]] = m["roi"]
    return out


def evaluate_promotion(market_key, family, version, evaluation, config=None):
    """Decide promote / hold / reject for one model version on one market.

    Returns {"decision", "reasons", "checks", "market_key", "family",
    "version"} where checks maps check name -> bool and reasons are
    human-readable strings. Missing evaluation sections fail their checks.
    """
    cfg = dict(DEFAULT_CONFIG)
    if config:
        cfg.update(config)
    evaluation = evaluation or {}
    predictive = evaluation.get("predictive") or {}
    betting = evaluation.get("betting") or {}
    robustness = evaluation.get("robustness") or {}
    uncertainty = evaluation.get("uncertainty") or {}

    task = _task_for_market(market_key)
    task_m = predictive.get(task) if task else None
    # Prefer per-task betting/uncertainty slices when the artifact has them;
    # otherwise fall back to the top-level (overall) sections.
    if task:
        _bt = (betting.get("by_task") or {}).get(task)
        if _bt:
            betting = _bt
        _ut = (uncertainty.get("by_task") or {}).get(task)
        if _ut:
            uncertainty = _ut

    checks = {}
    reasons = []

    # 1. sample size
    n_bets = betting.get("n_bets")
    if n_bets is None:
        checks["min_bets"] = False
        reasons.append("missing section: betting.n_bets")
    else:
        checks["min_bets"] = n_bets >= cfg["min_bets"]
        reasons.append("n_bets %d %s %d" % (
            n_bets, ">=" if checks["min_bets"] else "<", cfg["min_bets"]))

    # 2. positive economics
    roi = betting.get("roi")
    if roi is None:
        checks["roi_positive"] = False
        reasons.append("missing section: betting.roi")
    else:
        checks["roi_positive"] = roi > 0
        reasons.append("%s roi %+.3f %s 0" % (
            task or market_key, roi, ">" if checks["roi_positive"] else "<="))

    # 3. perceived edge (CLV proxy)
    edge_pp = betting.get("avg_edge_pp")
    if edge_pp is None:
        checks["edge_positive"] = False
        reasons.append("missing section: betting.avg_edge_pp")
    else:
        checks["edge_positive"] = edge_pp > cfg["min_edge_pp"]
        reasons.append("avg edge %+.2fpp %s %.2fpp" % (
            edge_pp, ">" if checks["edge_positive"] else "<=",
            cfg["min_edge_pp"]))

    # 4. beats the baseline on the market's predictive task
    if not cfg["require_beats_baseline"]:
        checks["beats_baseline"] = True
        reasons.append("beats_baseline check disabled by config")
        beats = None
    elif task is None:
        checks["beats_baseline"] = False
        beats = None
        reasons.append("missing section: no predictive task mapped for "
                       "market %r" % market_key)
    elif not isinstance(task_m, dict):
        checks["beats_baseline"] = False
        beats = None
        reasons.append("missing section: predictive.%s" % task)
    else:
        beats = task_m.get("beats_baseline")
        checks["beats_baseline"] = beats is True
        reasons.append("%s beats_baseline=%s" % (task, beats))

    # 5. calibration slope in range
    slope = task_m.get("calibration_slope") if isinstance(task_m, dict) \
        else None
    if slope is None:
        checks["calibration"] = False
        reasons.append("missing section: predictive.%s.calibration_slope"
                       % (task or "?"))
    else:
        checks["calibration"] = cfg["cal_slope_lo"] <= slope \
            <= cfg["cal_slope_hi"]
        reasons.append("calibration slope %.2f %s [%.1f, %.1f]" % (
            slope, "inside" if checks["calibration"] else "outside",
            cfg["cal_slope_lo"], cfg["cal_slope_hi"]))

    # 6. robustness across seasons
    season_rois = _season_rois(robustness, task)
    n_seasons = len(season_rois)
    if n_seasons < cfg["min_seasons"]:
        checks["season_robustness"] = False
        reasons.append("only %d seasons with roi (need >= %d)"
                       % (n_seasons, cfg["min_seasons"]))
    else:
        frac = sum(1 for r in season_rois.values() if r > 0) / n_seasons
        checks["season_robustness"] = frac >= cfg["min_season_win_frac"]
        reasons.append("%d/%d seasons profitable (%.2f %s %.2f)" % (
            sum(1 for r in season_rois.values() if r > 0), n_seasons, frac,
            ">=" if checks["season_robustness"] else "<",
            cfg["min_season_win_frac"]))

    # 7. statistical confidence that expected ROI > 0
    p_roi = uncertainty.get("p_roi_gt_0")
    if p_roi is None:
        checks["p_roi_gt_0"] = False
        reasons.append("missing section: uncertainty.p_roi_gt_0")
    else:
        checks["p_roi_gt_0"] = p_roi >= cfg["min_p_roi_gt_0"]
        reasons.append("P(ROI>0)=%.2f %s %.2f" % (
            p_roi, ">=" if checks["p_roi_gt_0"] else "<",
            cfg["min_p_roi_gt_0"]))

    # 8. minimum effect size (multiple-testing protection): a positive but
    #    negligible realized edge does not promote.
    realized = betting.get("realized_edge_pp")
    if realized is None:
        checks["effect_size"] = False
        reasons.append("missing section: betting.realized_edge_pp")
    else:
        checks["effect_size"] = realized >= cfg["min_effect_size_pp"]
        reasons.append("realized edge %+.2fpp %s %.2fpp" % (
            realized, ">=" if checks["effect_size"] else "<",
            cfg["min_effect_size_pp"]))

    # 9. untouched holdout: the final validation window must be seasons never
    #    used during feature/model development. Absence fails the check.
    holdout = evaluation.get("holdout") or {}
    hb = holdout.get("n_bets")
    if not isinstance(hb, int) or hb < cfg["min_holdout_bets"]:
        checks["holdout"] = False
        reasons.append("holdout: %s (need >= %d bets on untouched seasons)"
                       % ("none recorded" if hb is None else "%d bets" % hb,
                          cfg["min_holdout_bets"]))
    else:
        hroi = holdout.get("roi")
        checks["holdout"] = hroi is not None and hroi > 0
        reasons.append("holdout roi %s %s 0 (%d bets)" % (
            ("%+.3f" % hroi) if hroi is not None else "missing",
            ">" if checks["holdout"] else "<=",
            hb))

    if all(checks.values()):
        decision = "promote"
    elif roi is not None and roi <= 0 and beats is False:
        decision = "reject"
    else:
        decision = "hold"

    return {
        "decision": decision,
        "reasons": reasons,
        "checks": checks,
        "market_key": market_key,
        "family": family,
        "version": version,
    }


def check_demotion(current_status, rolling, config=None):
    """Evaluate whether a live model should step back.

    rolling: dict with any of {"roi", "p_roi_gt_0", "beats_baseline"} from
    recent forward/shadow evidence. Returns {"action": "keep"|"demote",
    "to": status|None, "reasons": [...]}. Demotion only ever moves backward
    along the status chain; it never deletes the model or its history.
    """
    cfg = dict(DEFAULT_CONFIG)
    if config:
        cfg.update(config)
    rolling = rolling or {}
    reasons = []

    if current_status == "production":
        roi = rolling.get("roi")
        p_roi = rolling.get("p_roi_gt_0")
        bad_roi = roi is not None and roi <= 0
        bad_conf = p_roi is not None and p_roi < 0.5
        if bad_roi:
            reasons.append("rolling roi %+.3f <= 0" % roi)
        if bad_conf:
            reasons.append("P(ROI>0)=%.2f < 0.50" % p_roi)
        if bad_roi or bad_conf:
            return {"action": "demote", "to": "challenger",
                    "reasons": reasons or ["production evidence deteriorated"]}
        return {"action": "keep", "to": None,
                "reasons": ["production evidence within thresholds"]}

    if current_status == "challenger":
        beats = rolling.get("beats_baseline")
        if beats is False:
            return {"action": "demote", "to": "research",
                    "reasons": ["no longer beats baseline on rolling "
                                "evidence"]}
        return {"action": "keep", "to": None,
                "reasons": ["challenger evidence within thresholds"]}

    return {"action": "keep", "to": None,
            "reasons": ["no demotion rule for status %r" % current_status]}
