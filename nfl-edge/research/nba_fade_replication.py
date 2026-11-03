"""Fixed 2025-26 descriptive replication; never an execution-profit backtest."""
import argparse
import csv
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from research.nba_closing_screen import _screen
from research.nba_replication_uncertainty import replication_interval


def replicate(rows):
    # All selected rests use the previous exact source-listed team game.
    # Known omitted Cup games occurred in December. For every retained January
    # start, verify a later listed prior game exists to isolate that blind spot.
    checked = _screen(rows, '2025-26', date(2025, 7, 1), date(2026, 7, 1), (2025, 12))
    by_id = {r['game_id']: r for r in rows}
    selected = checked['selections']
    ordered = sorted(rows, key=lambda r: r['start_time_utc'])
    previous = {}
    for row in ordered:
        t = datetime.fromisoformat(row['start_time_utc'].replace('Z', '+00:00'))
        day = t.astimezone(ZoneInfo('America/New_York')).date()
        if day.year == 2026 and day.month == 1:
            for side in ('home', 'away'):
                old = previous.get(row[side+'_team_id'])
                if old is None or old <= date(2025, 12, 16):
                    raise ValueError('Unresolved January prior-game Cup coverage')
        for side in ('home', 'away'):
            previous[row[side+'_team_id']] = day
    outcomes = [dict(game_id=s['game_id'],
                     start_time_utc=by_id[s['game_id']]['start_time_utc'],
                     outcome='loss' if s['outcome'] == 'win' else 'win')
                for s in selected]
    return dict(experiment_id='nba-b2b-fade-replication-v1',
                mode='descriptive_consensus_close_not_execution',
                input_rows=checked['input_rows'], team_ids=checked['team_ids'],
                exclusions=checked['exclusions'], primary=checked['fade_b2b'],
                uncertainty=replication_interval(outcomes),
                limitations=['Same compiler, not independent-source validation.',
                             'Known Cup omissions isolated; full independent schedule audit not certified.',
                             'No offered-book prices/times or executable ROI.',
                             'No calibrated individual probabilities or automatic promotion.'],
                private_selections=outcomes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input')
    parser.add_argument('output')
    args = parser.parse_args()
    if datetime.now(ZoneInfo('America/New_York')).date() < date(2026, 10, 29):
        raise ValueError('Registered earliest evaluation date has not arrived')
    raw = Path(args.input).read_bytes()
    result = replicate(list(csv.DictReader(raw.decode('utf8').splitlines())))
    result['source_sha256'] = hashlib.sha256(raw).hexdigest()
    with open(args.output, 'x', encoding='utf8') as f:
        json.dump(result, f, indent=2)
    print(json.dumps({k: v for k, v in result.items() if k != 'private_selections'}))


if __name__ == '__main__': main()
