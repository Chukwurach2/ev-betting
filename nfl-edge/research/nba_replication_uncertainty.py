"""Fixed descriptive uncertainty for nba-b2b-fade-replication-v1.

No downloader, selection search, execution-profit estimate or promotion gate.
The caller must first audit and select unique protocol-eligible games.
"""
from datetime import datetime, timedelta
from math import ceil
from random import Random
from zoneinfo import ZoneInfo

BLOCK_WEEKS = 4
REPLICATES = 20_000
ATTEMPT_CAP = 100_000
SEED = 20261029


def weekly_counts(records):
    """Monday-start Eastern weeks, retaining empty weeks between endpoints."""
    counts, seen = {}, set()
    for row in records:
        gid = row.get('game_id')
        if not gid or gid in seen:
            raise ValueError('Missing or duplicate selected game ID')
        seen.add(gid)
        outcome = row.get('outcome')
        if outcome not in ('win', 'loss'):
            raise ValueError('Half-point replication requires win or loss')
        t = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
        if t.tzinfo is None:
            raise ValueError('Selected start requires timezone')
        day = t.astimezone(ZoneInfo('America/New_York')).date()
        week = day - timedelta(days=day.weekday())
        wins, decisions = counts.get(week, (0, 0))
        counts[week] = (wins + int(outcome == 'win'), decisions + 1)
    if not counts:
        return []
    week, end = min(counts), max(counts)
    result = []
    while week <= end:
        result.append((week.isoformat(), *counts.get(week, (0, 0))))
        week += timedelta(weeks=1)
    return result


def circular_draw(weeks, rng):
    """Draw whole week aggregates in fixed four-week circular blocks."""
    n = len(weeks)
    if not n:
        raise ValueError('Cannot resample empty weeks')
    selected = []
    while len(selected) < n:
        start = rng.randrange(n)
        selected.extend(weeks[(start + j) % n] for j in range(BLOCK_WEEKS))
    selected = selected[:n]
    wins = sum(w for _, w, _ in selected)
    decisions = sum(d for _, _, d in selected)
    return wins / decisions if decisions else None


def replication_interval(records):
    weeks = weekly_counts(records)
    decisions = sum(d for _, _, d in weeks)
    nonempty = sum(d > 0 for _, _, d in weeks)
    result = dict(method='four_week_circular_bootstrap_descriptive',
                  seed=SEED, block_weeks=BLOCK_WEEKS,
                  calendar_weeks=len(weeks), nonempty_weeks=nonempty,
                  decisions=decisions, minimum_decisions=100,
                  minimum_nonempty_weeks=20, valid_replicates=0,
                  skipped_zero_decision=0, attempts=0, interval95=None,
                  sample_gate='sufficient' if decisions >= 100 and nonempty >= 20
                  else 'insufficient_descriptive_inconclusive',
                  limitations='Approximate local dependence only; no season-long team clustering, executable ROI or promotion.')
    if not weeks:
        return result
    rng, values = Random(SEED), []
    while len(values) < REPLICATES and result['attempts'] < ATTEMPT_CAP:
        result['attempts'] += 1
        value = circular_draw(weeks, rng)
        if value is None:
            result['skipped_zero_decision'] += 1
        else:
            values.append(value)
    result['valid_replicates'] = len(values)
    if len(values) != REPLICATES:
        result['sample_gate'] = 'bootstrap_incomplete_inconclusive'
        return result
    values.sort()
    result['interval95'] = [values[ceil(q * len(values)) - 1]
                            for q in (0.025, 0.975)]
    return result
