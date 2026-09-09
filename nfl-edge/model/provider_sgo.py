from __future__ import annotations
import os, requests, datetime as dt
from dataclasses import dataclass

API='https://api.sportsgameodds.com/v2/events'
NY_BOOKS={'DraftKings','FanDuel','BetMGM','Caesars','Fanatics Sportsbook','BetRivers','bet365','Bally Bet','ESPN BET'}

# NFL Edge accepts only these normalized contracts. Unsupported feed contracts are ignored.
MARKET_MAP={
    'points-all-1q-ou-over':('Q1_TOTAL','Over'),
    'points-all-1q-ou-under':('Q1_TOTAL','Under'),
    'points-all-1q-yn-yes':('Q1_ANY_SCORE','Yes'),
    'points-all-1q-yn-no':('Q1_ANY_SCORE','No'),
    'touchdowns-all-1q-yn-yes':('Q1_ANY_TD','Yes'),
    'touchdowns-all-1q-yn-no':('Q1_ANY_TD','No'),
    'bothTeamsScored-all-1q-yn-yes':('Q1_BOTH_SCORE','Yes'),
    'bothTeamsScored-all-1q-yn-no':('Q1_BOTH_SCORE','No'),
    'points-all-1mx5-yn-yes':('FIRST_5_MIN_SCORE','Yes'),
    'points-all-1mx5-yn-no':('FIRST_5_MIN_SCORE','No'),
}

@dataclass(frozen=True)
class Quote:
    provider_event_id:str; odd_id:str; sportsbook:str; market:str; selection:str
    line:float|None; american_odds:int; observed_at:str; source_url:str

def _iso(x):
    if not x: return None
    return dt.datetime.fromisoformat(str(x).replace('Z','+00:00')).astimezone(dt.timezone.utc).isoformat()

def fetch_nfl(api_key:str|None=None, timeout=20):
    key=api_key or os.getenv('SPORTSGAMEODDS_API_KEY')
    if not key: raise RuntimeError('SPORTSGAMEODDS_API_KEY missing')
    r=requests.get(API,params={'apiKey':key,'leagueID':'NFL','oddsAvailable':'true','limit':100},timeout=timeout)
    r.raise_for_status(); return r.json().get('data',[])

def normalize(events):
    out=[]
    for e in events:
        event_id=str(e.get('eventID') or e.get('id') or '')
        odds=e.get('odds') or {}
        items=odds.items() if isinstance(odds,dict) else []
        for odd_id, obj in items:
            mapped=MARKET_MAP.get(odd_id)
            # First-drive specials vary by feed naming; only map when an exact configured id exists.
            if not mapped: continue
            market,selection=mapped
            books=(obj or {}).get('byBookmaker') or {}
            for book,v in books.items():
                if book not in NY_BOOKS or not isinstance(v,dict): continue
                price=v.get('odds') if v.get('odds') is not None else v.get('americanOdds')
                try: price=int(price)
                except Exception: continue
                if not (price<=-100 or price>=100): continue
                observed=_iso(v.get('lastUpdated') or v.get('updatedAt') or obj.get('lastUpdated'))
                if not observed: continue
                line=v.get('line') if v.get('line') is not None else obj.get('line')
                try: line=None if line in ('',None) else float(line)
                except Exception: line=None
                out.append(Quote(event_id,odd_id,book,market,selection,line,price,observed,API))
    return out
