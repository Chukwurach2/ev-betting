from __future__ import annotations
import os, math, datetime as dt
from dataclasses import dataclass

BASE='https://api.the-odds-api.com/v4'
SPORT='americanfootball_nfl'
# Sport registry (nfl-edge/ops/sports.py) maps sport keys ('nfl'|'ncaaf') to
# provider sport keys. Imported defensively: this module is also imported
# with only model/ on sys.path (tests, readiness), where ops.* is not
# visible. In that case only the default sport='nfl' (the SPORT constant)
# is available; every existing call site behaves identically.
try:
    from ops.sports import odds_api_sport as _registry_api_sport
except ImportError:
    _registry_api_sport = None

def _api_sport(sport='nfl'):
    """Resolve the provider sport key, defaulting to the NFL constant."""
    if _registry_api_sport is not None:
        return _registry_api_sport(sport)  # raises ValueError on unknown sport
    if sport != 'nfl':
        raise RuntimeError(
            "sport=%r needs ops.sports on sys.path; only sport='nfl' "
            "is available in this import context" % (sport,))
    return SPORT
# Verified NY mobile brands should still be rechecked against NYSGC each run.
# williamhill_us (Caesars) and fanatics need a paid API tier; they are kept
# here so the book list is tier-driven, not code-driven.
NY_BOOK_KEYS={'draftkings','fanduel','betmgm','betrivers','espnbet',
              'williamhill_us','fanatics'}
# Sharp/offshore books used only as pricing signal, never as execution venues.
SIGNAL_ONLY_BOOKS={'pinnacle'}

# Only exact published/provider-discovered keys are normalized. Unknown contracts remain unsupported.
MARKET_MAP={
    'totals_q1':('Q1_TOTAL', None),
    'alternate_totals_q1':('Q1_TOTAL', None),
    'totals':('FULL_GAME_TOTAL', None),
    'spreads':('FULL_GAME_SPREAD', None),
}

@dataclass(frozen=True)
class Quote:
    provider_event_id:str; market_key:str; sportsbook:str; market:str; selection:str
    line:float|None; american_odds:int; observed_at:str; source_url:str; sportsbook_key:str

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

def _get(url, params, timeout):
    import requests
    try:
        response = requests.get(url, params=params, timeout=timeout)
        _check(response)
        payload = response.json()
    except (requests.RequestException, ValueError):
        # Transport and JSON exceptions can retain the prepared request URL,
        # including apiKey. Keep those details out of logs and run receipts.
        raise RuntimeError('Odds provider request failed') from None
    return payload, dict(response.headers)

def fetch_events(api_key=None, timeout=20, sport='nfl'):
    url=f'{BASE}/sports/{_api_sport(sport)}/events'
    return _get(url, {'apiKey':_key(api_key),'dateFormat':'iso'}, timeout)

def fetch_event_markets(event_id, api_key=None, timeout=20, bookmakers=None,
                        sport='nfl'):
    url=f'{BASE}/sports/{_api_sport(sport)}/events/{event_id}/markets'
    params={'apiKey':_key(api_key),'dateFormat':'iso'}
    if bookmakers: params['bookmakers']=','.join(bookmakers)
    else: params['regions']='us'
    return _get(url, params, timeout)

def fetch_event_odds(event_id, markets, api_key=None, timeout=20,
                   bookmakers=None, regions='us', sport='nfl'):
    if not markets: return {},{}
    url=f'{BASE}/sports/{_api_sport(sport)}/events/{event_id}/odds'
    params={'apiKey':_key(api_key),'dateFormat':'iso','oddsFormat':'american','markets':','.join(markets)}
    if bookmakers: params['bookmakers']=','.join(bookmakers)
    params['regions']=regions
    return _get(url, params, timeout)

def fetch_odds(markets, api_key=None, timeout=20, bookmakers=None,
               regions='us', sport='nfl'):
    url=f'{BASE}/sports/{_api_sport(sport)}/odds'
    params={'apiKey':_key(api_key),'dateFormat':'iso','oddsFormat':'american','markets':','.join(markets)}
    if bookmakers: params['bookmakers']=','.join(bookmakers)
    params['regions']=regions
    return _get(url, params, timeout)

def fetch_scores(days_from=3, api_key=None, timeout=20, sport='nfl'):
    url=f'{BASE}/sports/{_api_sport(sport)}/scores'
    return _get(url,{'apiKey':_key(api_key),'dateFormat':'iso','daysFrom':str(days_from)},timeout)

def quota(headers):
    def number(name):
        try:return int(headers.get(name))
        except (TypeError,ValueError):return None
    return {'remaining':number('x-requests-remaining'),'used':number('x-requests-used'),'last':number('x-requests-last')}

def normalize(event, allowed_books=None, sport='nfl'):
    """Flatten one event's bookmakers/markets into Quote rows.

    allowed_books: book keys to keep (default NY_BOOK_KEYS). Pass an
    explicit set to include signal-only books (e.g. pinnacle) or to
    widen the quote universe without touching the NY default.
    sport: selects the provider sport key used in the provenance URL
    (default 'nfl' keeps every existing call site identical).
    """
    keep = set(allowed_books) if allowed_books is not None else NY_BOOK_KEYS
    event_id=event.get('id') if isinstance(event,dict) else None
    if not isinstance(event_id,str) or not event_id:return []
    quotes=[]
    for book in event.get('bookmakers') or []:
        book_key=book.get('key')
        if book_key not in keep:continue
        for offered in book.get('markets') or []:
            mapped=MARKET_MAP.get(offered.get('key'))
            observed_at=_iso(offered.get('last_update'))
            if not mapped or not observed_at:continue
            market=mapped[0]
            for side in offered.get('outcomes') or []:
                try: price=int(side.get('price'))
                except (TypeError,ValueError):continue
                if isinstance(side.get('price'),bool) or price!=side.get('price') or not (price<=-100 or price>=100):continue
                try: line=float(side.get('point'))
                except (TypeError,ValueError):continue
                if not math.isfinite(line) or abs(line*2-round(line*2))>1e-9:continue
                selection=side.get('name')
                if not isinstance(selection,str) or not selection:continue
                if market in {'Q1_TOTAL','FULL_GAME_TOTAL'} and selection not in {'Over','Under'}:continue
                # The rejected Q1 model does not estimate push mass, so keep its shadow
                # contract restricted to half points. Full-game integer lines carry
                # explicit push settlement and remain research-only.
                if market=='Q1_TOTAL' and line.is_integer():continue
                if market in {'Q1_TOTAL','FULL_GAME_TOTAL'} and line<0:continue
                quotes.append(Quote(event_id,offered['key'],book.get('title') or book_key,
                    market,selection,line,price,observed_at,
                    f'{BASE}/sports/{_api_sport(sport)}/events/{event_id}/odds',book_key))
    return quotes

def _check(response):
    if not response.ok:
        raise RuntimeError(f'Odds provider request failed with HTTP {response.status_code}')
