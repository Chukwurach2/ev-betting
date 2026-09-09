from __future__ import annotations
import os, math, datetime as dt
from dataclasses import dataclass

BASE='https://api.the-odds-api.com/v4'
SPORT='americanfootball_nfl'
# Verified NY mobile brands should still be rechecked against NYSGC each run.
NY_BOOK_KEYS={'draftkings','fanduel'}

# Only exact published/provider-discovered keys are normalized. Unknown contracts remain unsupported.
MARKET_MAP={'totals_q1':('Q1_TOTAL', None), 'alternate_totals_q1':('Q1_TOTAL', None)}

@dataclass(frozen=True)
class Quote:
    provider_event_id:str; market_key:str; sportsbook:str; market:str; selection:str
    line:float|None; american_odds:int; observed_at:str; source_url:str

def _iso(x):
    if not x: return None
    try:
        t=dt.datetime.fromisoformat(str(x).replace('Z','+00:00'))
        return t.astimezone(dt.timezone.utc).isoformat() if t.tzinfo else None
    except (ValueError,TypeError): return None

def _key(api_key=None):
    k=api_key or os.getenv('THE_ODDS_API_KEY')
    if not k: raise RuntimeError('THE_ODDS_API_KEY missing')
    return k

def fetch_events(api_key=None, timeout=20):
    import requests
    url=f'{BASE}/sports/{SPORT}/events'
    r=requests.get(url,params={'apiKey':_key(api_key),'dateFormat':'iso'},timeout=timeout)
    _check(r)
    return r.json(), dict(r.headers)

def fetch_event_markets(event_id, api_key=None, timeout=20, bookmakers=None):
    import requests
    url=f'{BASE}/sports/{SPORT}/events/{event_id}/markets'
    params={'apiKey':_key(api_key),'dateFormat':'iso'}
    if bookmakers: params['bookmakers']=','.join(bookmakers)
    else: params['regions']='us'
    r=requests.get(url,params=params,timeout=timeout); _check(r)
    return r.json(), dict(r.headers)

def fetch_event_odds(event_id, markets, api_key=None, timeout=20, bookmakers=None):
    import requests
    if not markets: return {}, {}
    url=f'{BASE}/sports/{SPORT}/events/{event_id}/odds'
    params={'apiKey':_key(api_key),'markets':','.join(markets),'oddsFormat':'american','dateFormat':'iso'}
    if bookmakers: params['bookmakers']=','.join(bookmakers)
    else: params['regions']='us'
    r=requests.get(url,params=params,timeout=timeout); _check(r)
    return r.json(), dict(r.headers)

def available_supported_market_keys(markets_response):
    found=set()
    for b in (markets_response or {}).get('bookmakers',[]):
        if b.get('key') not in NY_BOOK_KEYS: continue
        for m in b.get('markets') or []:
            k=m.get('key') if isinstance(m,dict) else m
            if k in MARKET_MAP: found.add(k)
    return sorted(found)

def normalize(event):
    out=[]
    event_id=str((event or {}).get('id') or '')
    for book in (event or {}).get('bookmakers') or []:
        bkey=book.get('key'); title=book.get('title') or bkey
        if bkey not in NY_BOOK_KEYS: continue
        for m in book.get('markets') or []:
            # Event-odds responses timestamp each market independently. Do not
            # substitute fetch time or a bookmaker-level timestamp.
            observed=_iso(m.get('last_update'))
            if not observed: continue
            mkey=m.get('key'); mapped=MARKET_MAP.get(mkey)
            if not mapped: continue
            market,_=mapped
            for o in m.get('outcomes') or []:
                price=o.get('price')
                try: price=int(price)
                except Exception: continue
                if isinstance(o.get('price'),bool) or float(o.get('price'))!=price or not (price<=-100 or price>=100): continue
                point=o.get('point')
                try: line=None if point is None else float(point)
                except Exception: line=None
                selection=str(o.get('name') or '')
                # Totals require Over/Under; team totals include team name in description/selection depending provider.
                if selection not in {'Over','Under'}: continue
                # Integer totals need explicit push pricing, unavailable in this model.
                if line is None or not math.isfinite(line) or line < 0 or abs(line % 1) != .5: continue
                out.append(Quote(event_id,mkey,title,market,selection,line,price,observed,f'{BASE}/sports/{SPORT}/events/{event_id}/odds'))
    return out

def _check(response):
    if not response.ok:
        # requests exceptions include the URL (and API key); never expose it.
        raise RuntimeError('Odds provider HTTP '+str(response.status_code))

def quota(headers):
    def n(k):
        try:return int(headers.get(k))
        except Exception:return None
    return {'remaining':n('x-requests-remaining'),'used':n('x-requests-used'),'last_cost':n('x-requests-last')}
