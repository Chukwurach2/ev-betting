"""Preregistered NFL long-rest diagnostic. Synthetic-test before subgroup use."""
import argparse
import csv
import hashlib
import io
import json
import math
import random
from collections import Counter
from datetime import datetime
from pathlib import Path
from research.nfl_rematch import audit_schedule as validate_schedule
from research.nfl_rematch import EXPOSED, integer, percentile, exclusive_json

SOURCE_SHA256 = 'fdd15d1c67c59ff5440f5ea5de79067b5aaa1ebf5ab872014f9e1175204a89e3'


def rest_role(home_days, away_days):
    if home_days is None or away_days is None:
        return 'missing_prior'
    ordinary = lambda d: 5 <= d <= 9
    long = lambda d: 12 <= d <= 18
    if ordinary(home_days) and ordinary(away_days):
        return 'control'
    if long(home_days) and ordinary(away_days):
        return 'home'
    if long(away_days) and ordinary(home_days):
        return 'away'
    return 'other_rest'


def audit_schedule(rows, expected=(272, 32, 17)):
    # Shared validator reads schedule/identity only, never scores or prices.
    validation = validate_schedule(rows, expected)
    previous, labels = {}, {}
    regular = [r for r in rows if r['season_type'] == 'regular']
    for row in sorted(regular, key=lambda r: (datetime.fromisoformat(r['start_time_utc'].replace('Z', '+00:00')), r['game_id'])):
        at = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
        gaps = []
        for side in ('home', 'away'):
            team = row[side + '_team_id']
            gap = (at - previous[team]).total_seconds() / 86400 if team in previous else None
            if gap is not None and gap <= 0:
                raise ValueError('prior start must be strictly earlier')
            gaps.append(gap)
        labels[row['game_id']] = dict(week=integer(row['week']), home_days=gaps[0], away_days=gaps[1], role=rest_role(*gaps))
        for side in ('home', 'away'):
            previous[row[side + '_team_id']] = at
    return dict(regular_games=validation['regular_games'], teams=validation['teams'],
                games_per_team=expected[2], postseason_excluded=validation['postseason_excluded'],
                schedule_counts=dict(Counter(x['role'] for x in labels.values())), labels=labels)


def bootstrap(weeks, attempts=20000, seed=20261105):
    rng, draws, skipped = random.Random(seed), [], 0
    for _ in range(attempts):
        sample = [(start + offset) % 18 + 1 for start in [rng.randrange(18) for _ in range(6)] for offset in range(3)]
        denominator = sum(weeks[w]['weight'] for w in sample)
        if not denominator:
            skipped += 1
            continue
        # Repeated weeks repeat every constituent role/game together. Stratum
        # means remain identical; treated-count weights repeat with that week.
        draws.append(sum(weeks[w]['weighted_contrast'] for w in sample) / denominator)
    draws.sort()
    return dict(attempts=attempts, seed=seed, valid_draws=len(draws), skipped_zero_weight=skipped,
                interval=[percentile(draws, .005), percentile(draws, .995)] if draws else None)


def evaluate(rows, audit, minimums=(20, 80, 6)):
    groups = {(w, role): {'treated': [], 'controls': []} for w in range(1, 19) for role in ('home', 'away')}
    excluded = Counter()
    for row in rows:
        if row['season_type'] != 'regular':
            continue
        p = float(row['close_home_win_prob'])
        hs, aws = integer(row['home_score']), integer(row['away_score'])
        if not math.isfinite(p) or not 0 < p < 1 or min(hs, aws) < 0:
            raise ValueError('invalid probability/score')
        label = audit['labels'][row['game_id']]
        # Independent flags are nonexclusive; a row with any flag is excluded.
        flags = [name for name, yes in [('missing_prior', label['role'] == 'missing_prior'),
                 ('other_rest', label['role'] == 'other_rest'), ('search_exposed', row['game_id'] in EXPOSED),
                 ('tied_final', hs == aws)] if yes]
        excluded.update(flags)
        if flags:
            continue
        roles = ('home', 'away') if label['role'] == 'control' else (label['role'],)
        for role in roles:
            probability = p if role == 'home' else 1 - p
            win = int(hs > aws) if role == 'home' else int(aws > hs)
            group = 'controls' if label['role'] == 'control' else 'treated'
            groups[(label['week'], role)][group].append((win, probability))
    strata, weeks, t_count, c_count, comparable = [], {}, 0, 0, []
    def summary(xs):
        return dict(appearances=len(xs), wins=sum(w for w, p in xs),
                    mean_probability=sum(p for w, p in xs) / len(xs) if xs else None,
                    mean_residual=sum(w - p for w, p in xs) / len(xs) if xs else None,
                    brier=sum((w - p) ** 2 for w, p in xs) / len(xs) if xs else None)
    for week in range(1, 19):
        weight, numerator = 0, 0
        for role in ('home', 'away'):
            t, c = groups[(week, role)]['treated'], groups[(week, role)]['controls']
            ts, cs = summary(t), summary(c)
            contrast = ts['mean_residual'] - cs['mean_residual'] if t and c else None
            n = len(t) if contrast is not None else 0
            strata.append(dict(week=week, role=role, treated=ts, controls=cs, contrast=contrast, weight=n))
            weight += n
            numerator += n * contrast if n else 0
            if n:
                t_count += n
                c_count += len(c)
        weeks[week] = dict(weight=weight, weighted_contrast=numerator)
        if weight:
            comparable.append(week)
    report = dict(id='nfl-long-rest-calibration-v1', status='insufficient_data',
                  exclusions_nonexclusive=dict(excluded), strata=strata, weeks=weeks,
                  comparable_treated=t_count, comparable_control_appearances=c_count,
                  comparable_weeks=comparable, primary_contrast=None, uncertainty=None,
                  inference='Reused-season development diagnostic only; no ROI/CLV/causality/pick promotion.')
    if t_count < minimums[0] or c_count < minimums[1] or len(comparable) < minimums[2]:
        return report
    contrast = sum(x['weighted_contrast'] for x in weeks.values()) / t_count
    uncertainty = bootstrap(weeks)
    status = 'insufficient_data' if uncertainty['valid_draws'] < 19000 else (
        'descriptive_lead' if contrast >= .03 and uncertainty['interval'][0] > 0 else 'not_supported')
    report.update(status=status, primary_contrast=contrast, uncertainty=uncertainty)
    return report


def verify_source(raw):
    if len(raw) != 71573 or hashlib.sha256(raw).hexdigest() != SOURCE_SHA256:
        raise ValueError('registered source fingerprint mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('private_output_directory', type=Path)
    args = parser.parse_args()
    raw = args.source.read_bytes()
    verify_source(raw)
    rows = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    audit = audit_schedule(rows)
    exclusive_json(args.private_output_directory / 'schedule-audit.json', audit)
    exclusive_json(args.private_output_directory / 'source-receipt.json', dict(bytes=len(raw), sha256=SOURCE_SHA256))
    report = evaluate(rows, audit)
    exclusive_json(args.private_output_directory / 'report.json', report)
    print(json.dumps({k: report[k] for k in ('id', 'status', 'primary_contrast', 'uncertainty')}))


if __name__ == '__main__':
    main()
