"""Fixed weekly estimator for the registered NHL immediate-rematch diagnostic."""
from collections import Counter
from datetime import timedelta
from math import fsum
from random import Random

from .nhl_rematch_prior_loser import EASTERN, classify_games, scores, start


def monday(game):
    day = start(game).astimezone(EASTERN).date()
    return day - timedelta(days=day.weekday())


def weekly_statistics(games, assignments):
    first, last = min(map(monday, games)), max(map(monday, games))
    weeks = {
        first + timedelta(weeks=offset): ([], [])
        for offset in range((last - first).days // 7 + 1)
    }
    for game in games:
        assignment = assignments[game['game_id']]
        if assignment['eligible']:
            home, away = scores(game)
            residual = int(home > away) - float(game['close_home_win_prob'])
            treated, controls = weeks[monday(game)]
            (treated if assignment['treated'] else controls).append(residual)

    report = []
    for week, (treated, controls) in weeks.items():
        contrast = None
        if treated and controls:
            contrast = fsum(treated) / len(treated) - fsum(controls) / len(controls)
        report.append(dict(week=week.isoformat(), treated=len(treated),
                           controls=len(controls), contrast=contrast))
    return report


def interpolated_quantile(ordered, quantile):
    index = quantile * (len(ordered) - 1)
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (index - lower) * (ordered[upper] - ordered[lower])


def bootstrap(weighted_weeks, attempts, seed):
    rng = Random(seed)
    count = len(weighted_weeks)
    blocks = [
        tuple(weighted_weeks[(offset + step) % count] for step in range(4))
        for offset in range(count)
    ]
    valid = []
    for _ in range(attempts):
        sample = []
        for offset in range(0, count, 4):
            sample.extend(blocks[rng.randrange(count)][:min(4, count - offset)])
        weight = sum(weight for weight, _ in sample)
        if weight:
            valid.append(fsum(numerator for _, numerator in sample) / weight)
    valid.sort()
    interval = [interpolated_quantile(valid, q) for q in (0.0005, 0.9995)] if valid else None
    return dict(attempts=attempts, valid=len(valid), skipped=attempts - len(valid),
                interval=interval)


def summarize_weeks(weeks):
    comparable = [week for week in weeks if week['contrast'] is not None]
    treated = sum(week['treated'] for week in comparable)
    controls = sum(week['controls'] for week in comparable)
    report = dict(weeks=weeks, comparable_weeks=len(comparable),
                  comparable_treated=treated, comparable_controls=controls,
                  contrast=None, bootstrap=None, status='insufficient_coverage')
    if treated:
        report['contrast'] = fsum(
            (week['treated'] / treated) * week['contrast'] for week in comparable
        )
    if treated < 30 or controls < 80 or len(comparable) < 10:
        return report

    weighted = [
        (0, 0.0) if week['contrast'] is None else
        (week['treated'], week['treated'] * week['contrast'])
        for week in weeks
    ]
    result = bootstrap(weighted, attempts=200000, seed=20261110)
    report['bootstrap'] = result
    if result['valid'] >= 190000:
        report['status'] = 'not_supported'
        if report['contrast'] >= 0.03 and result['interval'][0] > 0:
            report['status'] = 'development_lead'
    return report


def evaluate(rows, *, source_scope_verified):
    """Run the fixed diagnostic after a separately documented source-scope review."""
    if source_scope_verified is not True:
        raise ValueError('Verify closing benchmark and score target scope before evaluation')
    rows = list(rows)
    assignments = {record['game_id']: record for record in classify_games(rows)}
    regular = [row for row in rows if row['season_type'] == 'regular']
    report = summarize_weeks(weekly_statistics(regular, assignments))
    report['assignment_counts'] = dict(Counter(record['reason'] for record in assignments.values()))
    return report
