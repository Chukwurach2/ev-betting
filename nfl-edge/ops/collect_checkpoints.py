"""One bounded shadow-quote collection run. No model promotion or wager output."""
import datetime as dt
import json
import os
import pathlib
import sys
from dataclasses import asdict
from urllib.parse import urlsplit
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
from checkpoints import plan,collect,instant
from postgres_checkpoints import PostgresCheckpointStore
from provider_oddsapi import fetch_events,fetch_event_odds,normalize,quota

NAMES=dict(zip(
    'AZ ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC LV LAC LAR MIA MIN NE NO NYG NYJ PHI PIT SF SEA TB TEN WAS'.split(),
    ['Arizona Cardinals','Atlanta Falcons','Baltimore Ravens','Buffalo Bills','Carolina Panthers','Chicago Bears',
     'Cincinnati Bengals','Cleveland Browns','Dallas Cowboys','Denver Broncos','Detroit Lions','Green Bay Packers',
     'Houston Texans','Indianapolis Colts','Jacksonville Jaguars','Kansas City Chiefs','Las Vegas Raiders',
     'Los Angeles Chargers','Los Angeles Rams','Miami Dolphins','Minnesota Vikings','New England Patriots',
     'New Orleans Saints','New York Giants','New York Jets','Philadelphia Eagles','Pittsburgh Steelers',
     'San Francisco 49ers','Seattle Seahawks','Tampa Bay Buccaneers','Tennessee Titans','Washington Commanders']))

def canonical(team): return {'LA':'LAR','ARI':'AZ'}.get(team,team)
def implied(odds): return 100/(100+odds) if odds>0 else abs(odds)/(100+abs(odds))

def settlement(market,line):
    integer=float(line).is_integer()
    if market=='FULL_GAME_TOTAL':
        return ('Regulation plus overtime; push when final combined score equals line; book rules govern voids'
                if integer else 'Regulation plus overtime; no push at half-point line; book rules govern voids')
    if market=='FULL_GAME_SPREAD':
        return ('Regulation plus overtime; push when adjusted margin equals zero; book rules govern voids'
                if integer else 'Regulation plus overtime; no push at half-point spread; book rules govern voids')
    return 'First quarter only; no push at half-point line; book rules govern voids'

def pair_quotes(payload):
    normalized=normalize(payload)
    groups={}
    for q in normalized:
        line=abs(q.line) if q.market=='FULL_GAME_SPREAD' else q.line
        groups.setdefault((q.provider_event_id,q.market_key,q.sportsbook_key,line,q.observed_at),[]).append(q)
    result=[]
    for (_,_,_,line,_),quotes in groups.items():
        if len(quotes)!=2:continue
        market=quotes[0].market
        if market in {'Q1_TOTAL','FULL_GAME_TOTAL'}:
            if {q.selection for q in quotes}!={'Over','Under'} or any(q.line!=line for q in quotes):continue
        elif market=='FULL_GAME_SPREAD':
            teams={payload.get('home_team'),payload.get('away_team')}
            if {q.selection for q in quotes}!=teams or abs(quotes[0].line+quotes[1].line)>1e-9:continue
        else:continue
        denominator=sum(implied(q.american_odds) for q in quotes)
        for q in quotes:
            result.append({**asdict(q),'fair_probability':implied(q.american_odds)/denominator,
                'fair_method':'paired_same_book_conditional_on_no_push',
                'settlement_rules':settlement(market,line),'ny_licensed':True})
    return result

def main():
    import psycopg
    from psycopg.rows import dict_row
    database=os.environ.get('NFL_EDGE_DATABASE_URL')
    if not database or not os.environ.get('THE_ODDS_API_KEY'):
        raise ValueError('NFL_EDGE_DATABASE_URL and THE_ODDS_API_KEY are required')
    if '-pooler.' in (urlsplit(database).hostname or ''):
        raise ValueError('Use a direct database connection for the session advisory lock')
    clock=lambda:dt.datetime.now(dt.timezone.utc)
    with psycopg.connect(database,autocommit=True,connect_timeout=10,row_factory=dict_row) as connection:
        if not connection.execute('SELECT pg_try_advisory_lock(20260910) AS acquired').fetchone()['acquired']:
            print(json.dumps({'status':'already_running'}));return
        connection.execute("SET statement_timeout='10s'")
        store=PostgresCheckpointStore(connection);store.reconcile(clock())
        rows=connection.execute("SELECT game_id,home_team,away_team,kickoff,status FROM public.games WHERE status='scheduled' AND kickoff BETWEEN now()-interval '1 day' AND now()+interval '1 day'").fetchall()
        unique={}
        for r in rows:
            k=(canonical(r['home_team']),canonical(r['away_team']),r['kickoff'])
            if k not in unique or r['game_id'].startswith('nfl-'):unique[k]=r
        games=list(unique.values()); checkpoints=plan(games,clock())
        events,headers=fetch_events() if any(c.state=='due' for c in checkpoints) else ([],{})
        by_id={r['game_id']:r for r in games}
        def fetch(checkpoint):
            game=by_id[checkpoint.game_id]
            matches=[e for e in events if e.get('home_team')==NAMES.get(canonical(game['home_team']))
                     and e.get('away_team')==NAMES.get(canonical(game['away_team']))
                     and instant(e['commence_time'])==checkpoint.kickoff]
            if len(matches)!=1: raise ValueError('Provider event did not match exact matchup and kickoff')
            payload,_=fetch_event_odds(matches[0]['id'],['spreads','totals'],bookmakers=['draftkings','fanduel'])
            if payload.get('id')!=matches[0]['id'] or instant(payload['commence_time'])!=checkpoint.kickoff:
                raise ValueError('Provider response event changed')
            return pair_quotes(payload)
        _summary = collect(checkpoints,store,fetch,clock,
                         remaining=quota(headers)['remaining'],max_requests=8,cost_per_request=2)
        print(json.dumps({'status':'shadow_collection_only',**_summary}))
        try:
            # Best-effort pipeline heartbeat; must never break collection.
            from heartbeat import record_heartbeat
            record_heartbeat(connection, 'collector', {
                'credits_remaining': quota(headers).get('remaining'),
                'checkpoints': {k: _summary.get(k) for k in
                                ('captured', 'missed', 'duplicate', 'deferred',
                                 'failed', 'requests', 'credits_budgeted')},
            })
        except Exception as e:  # noqa: BLE001 - heartbeat is advisory only
            print(json.dumps({'heartbeat': 'failed',
                              'reason': str(e)[:120]}))

if __name__=='__main__':
    try:main()
    except Exception:
        print(json.dumps({'status':'failed','reason':'Worker prerequisites or collection failed; no recommendations published'}))
        sys.exit(1)
