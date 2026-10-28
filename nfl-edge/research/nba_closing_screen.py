"""Offline descriptive screen; consensus closing lines are not executable quotes."""
import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo


def summarize(outcomes):
    counts = Counter(outcomes)
    w, l, pushes = counts['win'], counts['loss'], counts['push']
    n = w + l
    interval = None
    if n:
        z = 1.959963984540054
        p = w / n
        center = (p + z*z/(2*n)) / (1 + z*z/n)
        radius = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n)) / (1+z*z/n)
        interval = [center-radius, center+radius]
    net = Decimal(w)*Decimal(100)/Decimal(110)-Decimal(l)
    return dict(wins=w, losses=l, pushes=pushes, decisions=n,
                cover_rate=w/n if n else None, wilson95=interval,
                hypothetical_minus110_net_units=str(net),
                hypothetical_minus110_roi=float(net/Decimal(n+pushes)) if n+pushes else None)


def screen(rows):
    seen, teams, parsed = set(), {}, []
    for r in rows:
        gid = r['game_id']
        if not gid or gid in seen:
            raise ValueError('Missing or duplicate game ID')
        seen.add(gid)
        t = datetime.fromisoformat(r['start_time_utc'].replace('Z', '+00:00'))
        if t.tzinfo is None:
            raise ValueError('Timezone required')
        day = t.astimezone(ZoneInfo('America/New_York')).date()
        if day.isoformat() != r['date_et']:
            raise ValueError('Date/time identity mismatch')
        for side in ('home', 'away'):
            tid, name = r[side+'_team_id'], r[side+'_team']
            if not tid or not name or (tid in teams and teams[tid] != name):
                raise ValueError('Team identity mismatch')
            teams[tid] = name
        if r['home_team_id'] == r['away_team_id']:
            raise ValueError('Same team on both sides')
        hs, aws = int(r['home_score']), int(r['away_score'])
        line = Decimal(r['close_spread_home'])
        if not line.is_finite() or min(hs, aws) < 0 or hs == aws:
            raise ValueError('Invalid final or line')
        margin = Decimal(hs-aws)+line
        label = 'home' if margin > 0 else 'away' if margin < 0 else 'push'
        if label != r['ats_result'] or margin != Decimal(r['ats_margin']):
            raise ValueError('Publisher ATS disagrees with recomputed result')
        parsed.append((t, gid, day, r, line, margin))
    previous, excluded, dog, fade, selections = {}, Counter(), [], [], []
    for t, gid, day, r, line, margin in sorted(parsed, key=lambda x: (x[0], x[1])):
        ids = [r['home_team_id'], r['away_team_id']]
        prior = [previous.get((r['season'], tid)) for tid in ids]
        for old in prior:
            if old and old[0] >= t:
                raise ValueError('Nonchronological team games')
        for tid in ids:
            previous[(r['season'], tid)] = (t, day)
        reason = None
        if r['season_type'] != 'regular':
            reason = 'postseason'
        elif day.year == 2024 and day.month == 12:
            reason = 'december_missing_cup_schedule'
        elif any(x is None for x in prior):
            reason = 'previous_game_unavailable'
        elif abs(line) % 1 != Decimal('0.5'):
            reason = 'not_half_point'
        if reason:
            excluded[reason] += 1
            continue
        hours = [(t-x[0]).total_seconds()/3600 for x in prior]
        b2b = [(day-x[1]).days == 1 and 18 <= h <= 36 for x, h in zip(prior, hours)]
        treated = [i for i in (0, 1) if b2b[i] and hours[1-i] >= 48]
        if len(treated) != 1:
            excluded['rest_population_not_met'] += 1
            continue
        side = treated[0]
        own_margin = margin if side == 0 else -margin
        own_line = line if side == 0 else -line
        outcome = 'win' if own_margin > 0 else 'loss' if own_margin < 0 else 'push'
        fade.append('loss' if outcome == 'win' else 'win' if outcome == 'loss' else 'push')
        if own_line > 0:
            dog.append(outcome)
        selections.append(dict(game_id=gid, b2b_side='home' if side == 0 else 'away',
                               underdog=own_line > 0, outcome=outcome))
    return dict(mode='descriptive_consensus_close_not_execution', input_rows=len(parsed),
                team_ids=len(teams), exclusions=dict(excluded),
                b2b_underdog=summarize(dog), fade_b2b=summarize(fade),
                selected_game_count=len(selections), selections=selections,
                limitations=['One season; December candidates excluded for missing Cup games.',
                             'Publisher provenance not independently certified.',
                             'No quote times or offered per-book prices; -110 is hypothetical.',
                             'Wilson interval ignores team/week dependence; not confirmatory.',
                             'No fitted model, calibrated 60% probabilities or future picks.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input')
    parser.add_argument('output')
    args = parser.parse_args()
    raw = Path(args.input).read_bytes()
    result = screen(list(csv.DictReader(raw.decode('utf8').splitlines())))
    result['source_sha256'] = hashlib.sha256(raw).hexdigest()
    with open(args.output, 'x', encoding='utf8') as f:
        json.dump(result, f, indent=2)
    print(json.dumps({k: v for k, v in result.items() if k != 'selections'}))


if __name__ == '__main__':
    main()
