"""Fixed full-game over residuals for the NBA Cup development diagnostic."""
from collections import Counter
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from math import fsum

from .nba_cup_schedule import FIRST, LAST, audit_fixtures, population_fixtures


def score(raw):
    text = str(raw)
    if not text.isascii() or not text.isdecimal():
        raise ValueError('Nonnegative integer final score required')
    return int(text)


def finite_decimal(raw):
    try:
        value = Decimal(str(raw))
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def over_residual(row):
    home, away = score(row['home_score']), score(row['away_score'])
    if home == away:
        raise ValueError('Tied NBA final is unsupported')
    if any(row.get(key) in (None, '') for key in ('close_total', 'close_over_prob')):
        return None, 'missing_benchmark'
    line = finite_decimal(row['close_total'])
    probability = finite_decimal(row['close_over_prob'])
    if (line is None or line <= 0 or line % 1 != Decimal('0.5')
            or probability is None or not 0 < probability < 1):
        return None, 'unsupported_benchmark'
    margin = Decimal(home + away) - line
    over = margin > 0
    expected_result = 'over' if over else 'under'
    if (str(row.get('total_result')).strip().lower() != expected_result
            or finite_decimal(row.get('total_margin')) != margin):
        raise ValueError('Publisher total grading contradicts the recomputed final')
    return float(int(over) - probability), None


def monday(day):
    return day - timedelta(days=day.weekday())


def weekly_totals(rows, schedule_crosswalk, cup_fixture_ids, *, source_scope_verified):
    """Audit schedule before reading scores. The caller retains source evidence."""
    if source_scope_verified is not True:
        raise ValueError('Document the schedule, rules and benchmark scope first')
    rows = list(rows)
    labels = audit_fixtures(rows, schedule_crosswalk, cup_fixture_ids)
    first, last = monday(FIRST), monday(LAST)
    weeks = {
        first + timedelta(weeks=i): ([], [])
        for i in range((last - first).days // 7 + 1)
    }
    exclusions = {'cup': Counter(), 'non_cup': Counter()}
    for row, (_, _, _, day) in population_fixtures(rows):
        treated = labels[row['game_id']]
        residual, reason = over_residual(row)
        if reason:
            exclusions['cup' if treated else 'non_cup'][reason] += 1
            continue
        cup, non_cup = weeks[monday(day)]
        (cup if treated else non_cup).append(residual)

    weekly = []
    for week, (cup, non_cup) in weeks.items():
        contrast = None
        if cup and non_cup:
            contrast = fsum(cup) / len(cup) - fsum(non_cup) / len(non_cup)
        weekly.append(dict(week=week.isoformat(), cup=len(cup), non_cup=len(non_cup),
                           contrast=contrast))
    return {'weeks': weekly, 'benchmark_exclusions': exclusions}
