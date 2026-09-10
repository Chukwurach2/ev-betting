"""Print due/missed checkpoints from a schedule without spending API credits."""
import argparse
import datetime as dt
import json
from dataclasses import asdict
from checkpoints import plan
p=argparse.ArgumentParser();p.add_argument('--schedule',default='ops/schedule-2026.json');p.add_argument('--now')
a=p.parse_args()
with open(a.schedule) as f: data=json.load(f)
games=data if isinstance(data,list) else data['games']
print(json.dumps([asdict(c) for c in plan(games,a.now or dt.datetime.now(dt.timezone.utc))],default=str,indent=2))
