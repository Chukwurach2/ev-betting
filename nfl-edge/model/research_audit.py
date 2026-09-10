"""Audit full-game data coverage without treating closing prices as executable."""
import csv,datetime as dt,hashlib,io,json,pathlib,urllib.request
URL='https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv'
FIELDS=['home_moneyline','away_moneyline','spread_line','home_spread_odds','away_spread_odds','total_line','over_odds','under_odds']

def audit(data):
    rows=list(csv.DictReader(io.StringIO(data.decode())))
    if not rows or not {'season','game_id','home_score','away_score'}<=set(rows[0]):
        raise ValueError('Schedule schema does not match required fields')
    settled=[r for r in rows if r['home_score'] not in ('','NA') and r['away_score'] not in ('','NA')]
    seasons=sorted({r['season'] for r in settled})
    return {'experiment_id':'full-game-coverage-v1','source':URL,'sha256':hashlib.sha256(data).hexdigest(),
            'created_at':dt.datetime.now(dt.timezone.utc).isoformat(),'rows':len(rows),'settled_games':len(settled),
            'available_columns':list(rows[0]),
            'coverage_by_season':[{ 'season':s,'settled_games':sum(r['season']==s for r in settled),
                **{f:sum(r['season']==s and r.get(f) not in (None,'','NA') for r in settled) for f in FIELDS}} for s in seasons],
            'production_validated':False,'price_timestamp_verified':False,
            'limitation':'Historical line/price availability is not proof the quote was executable at a proposed pregame decision time.'}

if __name__=='__main__':
    with urllib.request.urlopen(URL,timeout=60) as response: data=response.read()
    report=audit(data)
    out=pathlib.Path('artifacts-research');out.mkdir(exist_ok=True)
    (out/'coverage.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2),flush=True)
