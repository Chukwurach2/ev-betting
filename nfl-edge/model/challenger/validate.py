"""Validation metrics and the promotion gate.

The gate is strict on purpose: a challenger only graduates to a pick signal
for spreads/totals if it BEATS THE CLOSING LINE on out-of-sample cover
prediction. Beating a naive baseline on win probability is not enough,
because our markets are spreads and totals.
"""
import math

EPS = 1e-6


def _clip(ps):
    return [min(max(p, EPS), 1.0 - EPS) for p in ps]


def log_loss(ps, ys):
    ps = _clip(ps)
    return -sum(y * math.log(p) + (1 - y) * math.log(1 - p)
                for y, p in zip(ys, ps)) / len(ps)


def brier(ps, ys):
    return sum((p - y) ** 2 for y, p in zip(ys, ps)) / len(ps)


def calibration_slope(ps, ys, bins=10):
    """Slope of observed rate vs predicted mean across decile bins.
    Well-calibrated ~ 1.0. Returns None when not computable."""
    order = sorted(zip(ps, ys))
    n = len(order)
    if n < 50:
        return None
    xs, zs = [], []
    for i in range(bins):
        chunk = order[i * n // bins:(i + 1) * n // bins]
        if not chunk:
            continue
        xs.append(sum(p for p, _ in chunk) / len(chunk))
        zs.append(sum(y for _, y in chunk) / len(chunk))
    mx = sum(xs) / len(xs)
    mz = sum(zs) / len(zs)
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return sum((x - mx) * (z - mz) for x, z in zip(xs, zs)) / den


def evaluate(predictions):
    """predictions: list of dicts with keys task ('win'|'spread'|'total'),
    p (model prob), y (0/1), base (baseline prob). Returns metrics dict."""
    out = {}
    for task in ("win", "spread", "total"):
        rows = [r for r in predictions if r["task"] == task]
        if not rows:
            out[task] = {"n": 0}
            continue
        ps = [r["p"] for r in rows]
        ys = [r["y"] for r in rows]
        bs = [r["base"] for r in rows]
        m = {
            "n": len(rows),
            "log_loss": log_loss(ps, ys),
            "brier": brier(ps, ys),
            "base_log_loss": log_loss(bs, ys),
            "base_brier": brier(bs, ys),
        }
        m["beats_baseline"] = (m["log_loss"] < m["base_log_loss"]
                               and m["brier"] < m["base_brier"])
        if task == "win":
            m["calibration_slope"] = calibration_slope(ps, ys)
        out[task] = m
    return out


def gate_decision(metrics):
    """Decide promotion. Returns (promoted: bool, reasons: list[str])."""
    reasons = []
    spread = metrics.get("spread", {})
    total = metrics.get("total", {})
    win = metrics.get("win", {})

    def _ok(m, name):
        if m.get("n", 0) < 200:
            reasons.append("%s: only %d validation games (< 200)" % (name, m.get("n", 0)))
            return False
        if not m.get("beats_baseline"):
            reasons.append(
                "%s: does not beat the closing line "
                "(ll %.4f vs %.4f, brier %.4f vs %.4f)" % (
                    name, m["log_loss"], m["base_log_loss"],
                    m["brier"], m["base_brier"]))
            return False
        return True

    s_ok = _ok(spread, "spread")
    t_ok = _ok(total, "total")
    if win and win.get("n", 0) >= 200 and not win.get("beats_baseline"):
        reasons.append("win probability does not beat the home-win baseline")
    slope = (win or {}).get("calibration_slope")
    if slope is not None and not 0.8 <= slope <= 1.2:
        reasons.append("win-prob calibration slope %.2f outside [0.8, 1.2]" % slope)

    promoted = s_ok and t_ok and not reasons
    if promoted:
        reasons.append("beats the closing line on spread and total; "
                       "eligible for shadow challenger signal")
    return promoted, reasons
