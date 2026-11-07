"""Fixed NFL closing-probability diagnostic; no executable-price inference."""
import argparse
import csv
import hashlib
import io
import json
import random
from collections import Counter
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from research.nfl_rematch import audit_schedule as validate_schedule
from research.nfl_rematch import EXPOSED, integer, percentile, exclusive_json

SOURCE_SHA256 = 'fdd15d1c67c59ff5440f5ea5de79067b5aaa1ebf5ab872014f9e1175204a89e3'
MINIMUMS = (30, 80, 8)


def probability(value):
    """Preserve the source decimal exactly before complement/band comparisons."""
    try:
        decimal = Decimal(str(value))
        if not decimal.is_finite() or not 0 < decimal < 1:
            raise ValueError('invalid probability')
        return Fraction(decimal)
    except (InvalidOperation, TypeError):
        raise ValueError('invalid probability') from None


def band(selected_p):
    if not 0 < selected_p <= Fraction(1, 2):
        raise ValueError('invalid selected probability')
    if selected_p == Fraction(1, 2):
        return 'even_probability'
    if selected_p <= Fraction(1, 4):
        return 'treated'
    if selected_p >= Fraction(1, 3):
        return 'controls'
    return 'gap'


def label(row):
    p_home = probability(row['close_home_win_prob'])
    selected_p = min(p_home, 1 - p_home)
    return dict(week=integer(row['week']),
                side='home' if p_home < Fraction(1, 2) else 'away' if p_home > Fraction(1, 2) else None,
                selected_probability=str(selected_p), band=band(selected_p))


def audit_schedule(rows, expected=(272, 32, 17)):
    """Schedule, identity and exact probability fields only; never read scores."""
    validation = validate_schedule(rows, expected)
    labels = {r['game_id']: label(r) for r in rows if r['season_type'] == 'regular'}
    return dict(regular_games=validation['regular_games'], teams=validation['teams'],
                games_per_team=expected[2], postseason_excluded=validation['postseason_excluded'],
                band_counts=dict(Counter(x['band'] for x in labels.values())), labels=labels)


def bootstrap(weeks, attempts=20000, seed=20261107):
    rng, draws, skipped = random.Random(seed), [], 0
    for _ in range(attempts):
        sample = [(start + offset) % 18 + 1
                  for start in [rng.randrange(18) for _ in range(6)] for offset in range(3)]
        denominator = sum(weeks[w]['weight'] for w in sample)
        if not denominator:
            skipped += 1
            continue
        # Sampling a week repeats every constituent game together, including
        # its comparator mean and treated-count weight. No global controls.
        draws.append(sum(weeks[w]['weighted_contrast'] for w in sample) / denominator)
    draws.sort()
    return dict(attempts=attempts, seed=seed, valid_draws=len(draws), skipped_zero_weight=skipped,
                interval=[percentile(draws, .005), percentile(draws, .995)] if draws else None)


def summarize(xs):
    return dict(games=len(xs), wins=sum(w for w, p in xs),
                mean_probability=sum(p for w, p in xs) / len(xs) if xs else None,
                mean_residual=sum(w - p for w, p in xs) / len(xs) if xs else None,
                brier=sum((w - p) ** 2 for w, p in xs) / len(xs) if xs else None)


def evaluate(rows, audit, minimums=MINIMUMS):
    groups = {w: {'treated': [], 'controls': []} for w in range(1, 19)}
    excluded = Counter()
    for row in sorted(rows, key=lambda r: r['game_id']):
        if row['season_type'] != 'regular':
            continue
        current = label(row)  # Validates probability even for excluded rows.
        hs, aws = integer(row['home_score']), integer(row['away_score'])
        if min(hs, aws) < 0:
            raise ValueError('invalid score')
        if current != audit['labels'][row['game_id']]:
            raise ValueError('audit label mismatch')
        flags = [name for name, yes in (
            ('search_exposed', row['game_id'] in EXPOSED), ('tied_final', hs == aws),
            ('even_probability', current['band'] == 'even_probability'), ('gap', current['band'] == 'gap')) if yes]
        excluded.update(flags)
        if flags:
            continue
        win = int(hs > aws) if current['side'] == 'home' else int(aws > hs)
        groups[current['week']][current['band']].append((win, float(Fraction(current['selected_probability']))))
    weeks, comparable, t_count, c_count = {}, [], 0, 0
    totals = {'treated': [], 'controls': []}
    for week in range(1, 19):
        t, c = groups[week]['treated'], groups[week]['controls']
        ts, cs = summarize(t), summarize(c)
        for role in totals:
            totals[role].extend(groups[week][role])
        contrast = ts['mean_residual'] - cs['mean_residual'] if t and c else None
        weight = len(t) if contrast is not None else 0
        weeks[week] = dict(treated=ts, controls=cs, contrast=contrast, weight=weight,
                           weighted_contrast=weight * contrast if weight else 0)
        if weight:
            comparable.append(week)
            t_count += len(t)
            c_count += len(c)
    report = dict(id='nfl-longshot-calibration-v1', status='insufficient_data',
                  exclusions_nonexclusive=dict(excluded), treated=summarize(totals['treated']),
                  controls=summarize(totals['controls']), weeks=weeks, comparable_weeks=comparable,
                  comparable_treated=t_count, comparable_controls=c_count, primary_contrast=None,
                  uncertainty=None, inference='Reused-season descriptive development only; no ROI/CLV/causality/pick promotion.')
    if t_count < minimums[0] or c_count < minimums[1] or len(comparable) < minimums[2]:
        return report
    contrast = sum(w['weighted_contrast'] for w in weeks.values()) / t_count
    uncertainty = bootstrap(weeks)
    status = 'insufficient_bootstrap_draws' if uncertainty['valid_draws'] < 19000 else (
        'descriptive_lead' if contrast <= -.03 and uncertainty['interval'][1] < 0 else 'not_supported')
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
    verify_source(raw)  # Abort before parsing/auditing a substitute source.
    rows = list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    audit = audit_schedule(rows)
    exclusive_json(args.private_output_directory / 'schedule-audit.json', audit)
    exclusive_json(args.private_output_directory / 'source-receipt.json', dict(bytes=len(raw), sha256=SOURCE_SHA256))
    report = evaluate(rows, audit)
    exclusive_json(args.private_output_directory / 'report.json', report)
    print(json.dumps({k: report[k] for k in ('id', 'status', 'primary_contrast', 'uncertainty')}))


if __name__ == '__main__':
    main()
