"""Registered prior-only assignment; no downloader, estimator or live model."""
from collections import Counter
from datetime import datetime, timedelta
from math import isfinite
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo('America/New_York')


def start(row):
    t = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
    if t.tzinfo is None or t.utcoffset() != timedelta(0):
        raise ValueError('UTC start required')
    return t


def audit_schedule(rows):
    """Validate dates/identity/probability without reading any score."""
    regular = [r for r in rows if r['season_type'] == 'regular']
    if len(regular) != 1312:
        raise ValueError('Require1312regular games')
    ids, team_starts, counts, names = set(), set(), Counter(), {}
    for r in regular:
        gid, h, a = r['game_id'], r['home_team_id'], r['away_team_id']
        if not gid or gid in ids or not h or not a or h == a:
            raise ValueError('Invalid or duplicate identity')
        ids.add(gid)
        t = start(r)
        day = t.astimezone(EASTERN).date().isoformat()
        if day != r['date_et'] or not '2025-07-01' <= day < '2026-07-01':
            raise ValueError('Invalid season/Eastern date')
        p = float(r['close_home_win_prob'])
        if not isfinite(p) or not 0 < p < 1:
            raise ValueError('Invalid closing probability')
        for team, name in ((h, r['home_team']), (a, r['away_team'])):
            if not name or (team in names and names[team] != name):
                raise ValueError('Unstable team identity')
            if (team, t) in team_starts:
                raise ValueError('Duplicate team start')
            team_starts.add((team, t))
            names[team] = name
            counts[team] += 1
    if len(counts) != 32 or set(counts.values()) != {82}:
        raise ValueError('Require32teams/82games each')
    return sorted(regular, key=lambda r: (start(r), r['game_id']))


def scores(row):
    values = []
    for key in ('home_score', 'away_score'):
        raw = row[key]
        if isinstance(raw, bool) or not str(raw).isdigit():
            raise ValueError('Nonnegative integer final required')
        values.append(int(raw))
    if values[0] == values[1]:
        raise ValueError('Tied final unsupported')
    winner = row.get('moneyline_winner')
    if winner is not None and winner != ('home' if values[0] > values[1] else 'away'):
        raise ValueError('Compiled winner contradicts final')
    return tuple(values)


def assign_one(current, home_prior, away_prior):
    """Current scores are deliberately not accessed. Caller audits schedule first."""
    result = dict(game_id=current['game_id'], start_time_utc=current['start_time_utc'],
                  eligible=False, treated=None, reason='missing_prior')
    if home_prior is None or away_prior is None:
        return result
    h, a = current['home_team_id'], current['away_team_id']
    for team, prior in ((h, home_prior), (a, away_prior)):
        if team not in (prior['home_team_id'], prior['away_team_id']):
            raise ValueError('Prior does not contain current team')
        scores(prior)
    if home_prior['game_id'] == away_prior['game_id']:
        fields = ('home_team_id', 'away_team_id', 'start_time_utc', 'home_score', 'away_score')
        if any(str(home_prior[k]) != str(away_prior[k]) for k in fields):
            raise ValueError('Shared prior fixture conflict')
        if {home_prior['home_team_id'], home_prior['away_team_id']} != {h, a}:
            raise ValueError('Shared prior pair conflict')
    hg, ag = start(current) - start(home_prior), start(current) - start(away_prior)
    result.update(home_prior_id=home_prior['game_id'], away_prior_id=away_prior['game_id'],
                  home_gap_hours=hg.total_seconds()/3600, away_gap_hours=ag.total_seconds()/3600)
    if not all(timedelta(hours=18) <= gap <= timedelta(hours=96) for gap in (hg, ag)):
        result['reason'] = 'rest_window'
        return result
    hs, aws = scores(home_prior)
    home_lost = hs < aws if home_prior['home_team_id'] == h else aws < hs
    if not home_lost:
        result['reason'] = 'home_prior_not_loss'
        return result
    treated = home_prior['game_id'] == away_prior['game_id']
    result.update(eligible=True, treated=treated,
                  reason='immediate_rematch_prior_loss' if treated else 'different_previous_fixture')
    return result


def classify_games(rows):
    """Assignment only. Full estimator/source-semantics review remains a later gate."""
    regular = audit_schedule(rows)
    for row in regular:
        scores(row)
    histories, records = {}, []
    for row in regular:
        h, a = row['home_team_id'], row['away_team_id']
        records.append(assign_one(row, histories.get(h), histories.get(a)))
        # Only update after assignment; current outcome can affect later games only.
        histories[h] = histories[a] = row
    return records
