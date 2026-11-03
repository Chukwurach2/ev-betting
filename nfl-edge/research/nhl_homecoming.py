"""Fixed closing-consensus descriptive screen; never an execution model.

Run audit_schedule before inspecting scores. No downloader or parameter search.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import ceil, isfinite
from random import Random
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo('America/New_York')


def start(row):
    value = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Timezone required')
    return value


def audit_schedule(rows):
    """Dates/IDs/probabilities only: this function must not read outcomes."""
    regular = [r for r in rows if r['season_type'] == 'regular']
    if len(regular) != 1312:
        raise ValueError('Require exactly1312regular games')
    seen, team_starts, counts, histories = set(), set(), Counter(), defaultdict(list)
    names = {}
    for r in regular:
        gid, h, a = r['game_id'], r['home_team_id'], r['away_team_id']
        if not gid or gid in seen or not h or not a or h == a:
            raise ValueError('Missing, duplicate or conflicting identity')
        seen.add(gid)
        t = start(r)
        day = t.astimezone(EASTERN).date()
        if day.isoformat() != r['date_et'] or not '2025-07-01' <= r['date_et'] < '2026-07-01':
            raise ValueError('Wrong season or Eastern date')
        p = float(r['close_home_win_prob'])
        if not isfinite(p) or not 0 < p < 1:
            raise ValueError('Invalid closing probability')
        for team, name, home in ((h, r['home_team'], True), (a, r['away_team'], False)):
            if not name or (team in names and names[team] != name):
                raise ValueError('Unstable team identity')
            names[team] = name
            if (team, t) in team_starts:
                raise ValueError('Duplicate team start')
            team_starts.add((team, t))
            counts[team] += 1
            histories[team].append((t, gid, home))
    if len(counts) != 32 or set(counts.values()) != {82}:
        raise ValueError('Require32teams with82games each')
    treated = set()
    for history in histories.values():
        history.sort()
        for i, (t, gid, home) in enumerate(history):
            if not home or i < 3:
                continue
            prior = history[i-3:i]
            if (all(not was_home for _, _, was_home in prior)
                    and t - prior[0][0] <= timedelta(days=14)
                    and timedelta(hours=18) <= t - prior[-1][0] <= timedelta(hours=96)):
                treated.add(gid)
    treated_teams = {r['home_team_id'] for r in regular if r['game_id'] in treated}
    audit = dict(regular_games=len(regular), teams=len(counts), games_per_team=82,
                 treated=len(treated), treated_home_teams=len(treated_teams),
                 controls=len(regular)-len(treated), excluded_postseason=len(rows)-len(regular))
    audit['sample_gate'] = ('sufficient' if audit['treated'] >= 50
                            and audit['treated_home_teams'] >= 20 and audit['controls'] >= 500
                            else 'insufficient_abort_before_outcomes')
    return regular, treated, audit


def weekly_sums(records):
    grouped = defaultdict(lambda: [0.0, 0, 0.0, 0])
    for r in records:
        day = start(r).astimezone(EASTERN).date()
        week = day - timedelta(days=day.weekday())
        idx = 0 if r['treated'] else 2
        grouped[week][idx] += r['residual']
        grouped[week][idx+1] += 1
    if not grouped:
        return []
    week, end, result = min(grouped), max(grouped), []
    while week <= end:
        result.append((week.isoformat(), *grouped[week]))
        week += timedelta(weeks=1)
    return result


def bootstrap(weeks):
    rng, values, skipped = Random(20261103), [], 0
    n = len(weeks)
    if not n:
        raise ValueError('Empty bootstrap')
    for _ in range(20000):
        indices = []
        while len(indices) < n:
            first = rng.randrange(n)
            indices.extend((first+j) % n for j in range(4))
        sums = [sum(weeks[i][j] for i in indices[:n]) for j in range(1, 5)]
        if not sums[1] or not sums[3]:
            skipped += 1
        else:
            values.append(sums[0]/sums[1] - sums[2]/sums[3])
    values.sort()
    return dict(method='four_week_circular_bootstrap', seed=20261103, calendar_weeks=n,
                draws=20000, valid_draws=len(values), skipped_empty_group=skipped,
                interval95=([values[ceil(q*len(values))-1] for q in (.025, .975)]
                            if len(values) == 20000 else None))


def evaluate(rows):
    regular, treated, audit = audit_schedule(rows)
    report = dict(experiment='nhl-homecoming-calibration-v1', audit=audit,
                  status='data_infeasible_no_outcomes_inspected', endpoint=None)
    if audit['sample_gate'] != 'sufficient':
        return report
    records = []
    for r in regular:
        scores = [float(r[k]) for k in ('home_score', 'away_score')]
        if any(not isfinite(x) or x < 0 or not x.is_integer() for x in scores) or scores[0] == scores[1]:
            raise ValueError('Invalid final scores')
        won = int(scores[0] > scores[1])
        if r.get('moneyline_winner') and r['moneyline_winner'] != ('home' if won else 'away'):
            raise ValueError('Winner label disagrees with final')
        p = float(r['close_home_win_prob'])
        records.append(dict(game_id=r['game_id'], start_time_utc=r['start_time_utc'],
                            treated=r['game_id'] in treated, won=won, probability=p,
                            residual=won-p, brier=(won-p)**2))
    groups = {}
    for flag, name in ((True, 'treated'), (False, 'controls')):
        subset = [r for r in records if r['treated'] == flag]
        groups[name] = dict(n=len(subset), wins=sum(r['won'] for r in subset),
                            **{k: sum(r[k] for r in subset)/len(subset)
                               for k in ('probability', 'residual', 'brier')})
    contrast = groups['treated']['residual'] - groups['controls']['residual']
    uncertainty = bootstrap(weekly_sums(records))
    interval = uncertainty['interval95']
    supported = interval is not None and contrast <= -.03 and interval[1] < 0
    report.update(status='descriptive_lead_only' if supported else 'not_supported',
                  endpoint=contrast, groups=groups, uncertainty=uncertainty,
                  limitations='Closing compiler estimates, schedule proxy, incomplete dependence control; no executable prices, ROI, independent-source validation or pick promotion.')
    return report
