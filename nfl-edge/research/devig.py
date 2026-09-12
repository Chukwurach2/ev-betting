"""De-vig methods for two-outcome markets (pure functions, no I/O).

Each method maps a two-sided implied-probability pair (p1, p2) with
overround R = p1 + p2 to fair probabilities (q1, q2) summing to 1.

Candidate set for devig-tournament-v1 (preregistered in
docs/preregistrations/devig-tournament-v1.md):

- multiplicative: q_i = p_i / R (baseline, industry standard)
- additive:      q_i = p_i - (R - 1) / 2 (equal margin in probability space)
- power:         q_i = p_i^k / (p_1^k + p_2^k), k solved per market from
                 p_1^k + p_2^k = 1 (Lopez favorite-longshot correction)

Shin is deliberately NOT a candidate: on two-outcome markets Shin's
method is algebraically identical to additive (verified numerically in
tests/test_devig.py against the published Shin formula). Including it
would double-count one method in the multiple-testing budget.
"""
from __future__ import annotations

import math


def _valid_pair(p1, p2):
    """Return (p1, p2) as floats, or None when unusable."""
    try:
        a, b = float(p1), float(p2)
    except (TypeError, ValueError):
        return None
    if not (0.0 < a < 1.0 and 0.0 < b < 1.0):
        return None
    if not (math.isfinite(a) and math.isfinite(b)):
        return None
    return a, b


def _check_out(q1, q2):
    """Validity guard: fair probs must be finite and strictly in (0, 1)."""
    if q1 is None or q2 is None:
        return None
    if not (0.0 < q1 < 1.0 and 0.0 < q2 < 1.0):
        return None
    if not (math.isfinite(q1) and math.isfinite(q2)):
        return None
    s = q1 + q2
    if not math.isfinite(s) or abs(s - 1.0) > 1e-9:
        return None
    return q1, q2


def devig_multiplicative(p1, p2):
    """q_i = p_i / R. Returns (q1, q2) or None."""
    v = _valid_pair(p1, p2)
    if v is None:
        return None
    a, b = v
    r = a + b
    if r <= 0:
        return None
    return _check_out(a / r, b / r)


def devig_additive(p1, p2):
    """q_i = p_i - (R - 1) / 2. Returns (q1, q2) or None."""
    v = _valid_pair(p1, p2)
    if v is None:
        return None
    a, b = v
    shave = (a + b - 1.0) / 2.0
    return _check_out(a - shave, b - shave)


def _power_k(a, b):
    """Solve p_1^k + p_2^k = 1 for k by bisection. Returns k or None."""
    def f(k):
        return a ** k + b ** k - 1.0
    # f -> (n_outcomes - 1) > 0 as k -> 0+; f decreases monotonically
    # in k for p_i in (0, 1), and f -> -1 as k -> inf.
    lo, hi = 1e-9, 1.0
    flo, fhi = f(lo), f(hi)
    if not (math.isfinite(flo) and math.isfinite(fhi)):
        return None
    if fhi > 0:
        # root is above 1: double hi until bracketed
        while fhi > 0 and hi < 1e6:
            hi *= 2.0
            fhi = f(hi)
            if not math.isfinite(fhi):
                return None
    else:
        # root is below 1 (R < 1): shrink lo toward 0
        while flo < 0 and lo > 1e-300:
            lo /= 2.0
            flo = f(lo)
            if not math.isfinite(flo):
                return None
    if not (flo > 0 > fhi):
        return None
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        fm = f(mid)
        if not math.isfinite(fm):
            return None
        if fm > 0:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-12:
            break
    return 0.5 * (lo + hi)


def devig_power(p1, p2):
    """Lopez power method: q_i = p_i^k / (p_1^k + p_2^k), k solved from
    p_1^k + p_2^k = 1. Returns (q1, q2) or None."""
    v = _valid_pair(p1, p2)
    if v is None:
        return None
    a, b = v
    if abs(a + b - 1.0) < 1e-12:
        return _check_out(a, b)
    k = _power_k(a, b)
    if k is None or k <= 0:
        return None
    try:
        pa, pb = a ** k, b ** k
    except (OverflowError, ValueError):
        return None
    s = pa + pb
    if s <= 0 or not math.isfinite(s):
        return None
    return _check_out(pa / s, pb / s)


DEVIG_METHODS: dict[str, callable] = {
    "multiplicative": devig_multiplicative,
    "additive": devig_additive,
    "power": devig_power,
}


def devig_method_names() -> list[str]:
    """Preregistered candidate method names, sorted."""
    return sorted(DEVIG_METHODS)


def get_devig_method(name: str):
    """Fetch a de-vig method by name, else raise ValueError."""
    try:
        return DEVIG_METHODS[name]
    except KeyError:
        raise ValueError("unknown de-vig method %r (have %s)"
                         % (name, devig_method_names()))
