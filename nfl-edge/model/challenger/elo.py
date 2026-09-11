"""Pure, deterministic Elo building blocks. No I/O, no randomness."""
import math


def Phi(x):
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def init_ratings(teams):
    """Zero-centered offensive/defensive ratings (points above average)."""
    return ({t: 0.0 for t in teams}, {t: 0.0 for t in teams})


def expected_scores(off, deff, home, away, league_avg=22.0, hfa=1.2):
    """Expected points for home and away.

    deff > 0 means a GOOD defense (reduces opponent scoring).
    """
    exp_h = league_avg + off[home] - deff[away] + hfa / 2.0
    exp_a = league_avg + off[away] - deff[home] - hfa / 2.0
    return exp_h, exp_a


def update_ratings(off, deff, home, away, home_score, away_score,
                   league_avg=22.0, hfa=1.2, k=0.15):
    """Chronological Elo update. Mutates the dicts in place, recentered."""
    exp_h, exp_a = expected_scores(off, deff, home, away, league_avg, hfa)
    err_h = home_score - exp_h
    err_a = away_score - exp_a
    off[home] += k * err_h
    deff[away] -= k * err_h   # opponent scored more -> defense was worse
    off[away] += k * err_a
    deff[home] -= k * err_a
    # Recenter to stop league-wide drift (keeps ratings zero-sum).
    n = len(off)
    mo = sum(off.values()) / n
    md = sum(deff.values()) / n
    for t in off:
        off[t] -= mo
        deff[t] -= md
    return exp_h, exp_a


def predict_game(off, deff, home, away, sigma_margin, sigma_total,
                 league_avg=22.0, hfa=1.2):
    """Pregame probabilities from ratings. Returns dict or None if a team
    is unknown (e.g. expansion team with no history -> caller skips)."""
    if home not in off or away not in off:
        return None
    exp_h, exp_a = expected_scores(off, deff, home, away, league_avg, hfa)
    mu_margin = exp_h - exp_a
    mu_total = exp_h + exp_a
    return {
        "win_prob_home": Phi(mu_margin / sigma_margin),
        "pred_margin_home": mu_margin,
        "pred_total": mu_total,
        "cover_prob_home": None,  # filled by caller with a specific line
        "over_prob": None,
    }


def cover_prob_home(pred_margin_home, spread_line_home, sigma_margin):
    """P(home covers). spread_line_home > 0 means home favored."""
    return Phi((pred_margin_home - spread_line_home) / sigma_margin)


def over_prob(pred_total, total_line, sigma_total):
    return Phi((pred_total - total_line) / sigma_total)
