"""One bounded shadow-quote collection run. No model promotion or wager output."""
import datetime as dt
import hashlib
import json
import os
import pathlib
import sys
import time
from dataclasses import asdict
from urllib.parse import urlsplit
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]))  # research.devig, ops.sports
from checkpoints import WINDOWS,plan,collect,instant
from research.devig import devig_multiplicative  # tournament-selected de-vig
from ops.sports import get as sport_config, checkpoints_table, odds_quotes_table
from postgres_checkpoints import PostgresCheckpointStore
from provider_oddsapi import (fetch_events,fetch_event_odds,normalize,quota,
                               NY_BOOK_KEYS)

# Quote universe for the price-discovery engine. Widening the book list is
# quota-NEUTRAL on The Odds API: cost is markets x regions (<=10 bookmakers
# = 1 region equivalent), so 5 books cost the same 2 credits/request as 2.
# After upgrading to a paid tier, extend with the paid-only NY books and the
# sharp signal book:
#   NFL_EDGE_BOOKMAKERS="draftkings,fanduel,betmgm,betrivers,espnbet,pinnacle,williamhill_us,fanatics"
#   NFL_EDGE_REGIONS="us,eu"   (eu is what carries pinnacle)
BOOKMAKERS = [b.strip() for b in
              os.environ.get("NFL_EDGE_BOOKMAKERS",
                             "draftkings,fanduel,betmgm,betrivers,espnbet"
                             ).split(",") if b.strip()]
REGIONS = os.environ.get("NFL_EDGE_REGIONS", "us")
N_REGIONS = len([r for r in REGIONS.split(",") if r.strip()]) or 1

NAMES=dict(zip(
    'AZ ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC LV LAC LAR MIA MIN NE NO NYG NYJ PHI PIT SF SEA TB TEN WAS'.split(),
    ['Arizona Cardinals','Atlanta Falcons','Baltimore Ravens','Buffalo Bills','Carolina Panthers','Chicago Bears',
     'Cincinnati Bengals','Cleveland Browns','Dallas Cowboys','Denver Broncos','Detroit Lions','Green Bay Packers',
     'Houston Texans','Indianapolis Colts','Jacksonville Jaguars','Kansas City Chiefs','Las Vegas Raiders',
     'Los Angeles Chargers','Los Angeles Rams','Miami Dolphins','Minnesota Vikings','New England Patriots',
     'New Orleans Saints','New York Giants','New York Jets','Philadelphia Eagles','Pittsburgh Steelers',
     'San Francisco 49ers','Seattle Seahawks','Tampa Bay Buccaneers','Tennessee Titans','Washington Commanders']))

def canonical(team): return {'LA':'LAR','ARI':'AZ'}.get(team,team)

# Per-sport session advisory locks: the NFL and NCAAF collectors run as
# separate scheduled workflows and must never block each other. The NFL lock
# id is unchanged.
ADVISORY_LOCKS={'nfl':20260910,'ncaaf':20260913}
# Heartbeat component names per sport (the health monitor keys on these).
HEARTBEAT_COMPONENTS={'nfl':'collector','ncaaf':'collector_ncaaf'}

def collector_env(sport):
    """Book/region config for a collector run.

    Env prefix is NFL_EDGE_* for nfl, NCAAF_EDGE_* for ncaaf so the two
    workflows configure independently. Defaults: NFL keeps its existing
    universe; NCAAF starts with the NY-licensed books actually observed in
    the us-region feed (Pinnacle is absent there; see the NCAAF contract).
    """
    prefix='NCAAF_EDGE' if sport=='ncaaf' else 'NFL_EDGE'
    default_books=('draftkings,fanduel,betmgm,betrivers' if sport=='ncaaf'
                   else 'draftkings,fanduel,betmgm,betrivers,espnbet')
    books=[b.strip() for b in os.environ.get(
        prefix+'_BOOKMAKERS',default_books).split(',') if b.strip()]
    regions=os.environ.get(prefix+'_REGIONS','us')
    n_regions=len([r for r in regions.split(',') if r.strip()]) or 1
    return books,regions,n_regions

def quota_reserve_ok(sport, headers):
    """Quota isolation: the NFL collector has priority on the shared key.

    When remaining provider quota falls below the NCAAF reserve floor, the
    NCAAF collector stands down entirely (free events feed only, zero paid
    requests) so a large college slate -- or any unexpected college-collector
    cost -- can never starve an NFL T-90/close checkpoint. The NFL collector
    has no such gate. Pure: decides from sport + response headers only.
    """
    if sport != 'ncaaf':
        return True, None
    remaining = quota(headers).get('remaining')
    reserve = int(os.environ.get('NCAAF_EDGE_MIN_QUOTA_REMAINING', '2000'))
    if remaining is not None and remaining < reserve:
        return False, {'remaining': remaining, 'reserve': reserve}
    return True, None

def find_provider_event(game,events,kickoff,sport):
    """Match provider events by exact matchup and kickoff.

    NFL maps provider team names through the NAMES/canonical alias table
    (32 teams, stable). NCAAF has 130+ teams: match on the exact
    home_team/away_team strings from the events feed, never a hardcoded
    name dictionary.
    """
    if sport=='nfl':
        want=(NAMES.get(canonical(game['home_team'])),
              NAMES.get(canonical(game['away_team'])))
    else:
        want=(game['home_team'],game['away_team'])
    return [e for e in events
            if e.get('home_team')==want[0] and e.get('away_team')==want[1]
            and instant(e['commence_time'])==kickoff]

def provider_event_keys(events):
    """Signatures of provider events, for the opener gate (pure)."""
    keys=set()
    for e in events:
        try:
            keys.add((e.get('home_team'),e.get('away_team'),instant(e['commence_time'])))
        except (ValueError,TypeError,KeyError):
            continue
    return keys

def opener_eligible(checkpoint,game,event_keys,captured_game_ids,sport='nfl'):
    """Pure opener gate: skip when the game already has a captured checkpoint
    (first-seen value is gone) or the provider has no event yet (no lines to
    capture -> retry a later run without spending or claiming).

    sport='nfl' (default) keeps the existing alias-table matching; other
    sports match on exact provider team strings.
    """
    if checkpoint.name!='Opener': return True
    if checkpoint.game_id in captured_game_ids: return False
    if sport=='nfl':
        home,away=(NAMES.get(canonical(game['home_team'])),
                    NAMES.get(canonical(game['away_team'])))
    else:
        home,away=game['home_team'],game['away_team']
    return (home,away,instant(checkpoint.kickoff)) in event_keys
def resolve_provider_event_id(connection, table, game_id):
    """Most recent provider event id captured for a game, if any.

    Lets post-commencement checkpoints (notably Close) fetch event odds
    directly by id instead of re-matching the live events feed, which no
    longer lists commenced games. Regression: the 2026-09-20 close-watch
    live test dispatched all 4 close clusters in-window and every run
    concluded success, yet all 14 Close checkpoints recorded 'failed'
    with 'Provider event did not match exact matchup and kickoff' --
    the events feed had already dropped the commenced games while the
    prop collector (which persists provider_event_id per snapshot)
    captured 15/15 closes. Identity reuse only: quotes still pass the
    checkpoint freshness filter, so point-in-time safety is unchanged.
    """
    row = connection.execute(
        """SELECT quotes FROM public.%s
           WHERE game_id=%%s AND status='captured'
             AND quotes IS NOT NULL AND jsonb_array_length(quotes) > 0
           ORDER BY ended_at DESC NULLS LAST, started_at DESC LIMIT 1""" % table,
        (game_id,)).fetchone()
    if not row:
        return None
    quotes = row['quotes'] if isinstance(row, dict) else row[0]
    if not quotes:
        return None
    return (quotes[0] or {}).get('provider_event_id')

def implied(odds): return 100/(100+odds) if odds>0 else abs(odds)/(100+abs(odds))

EARLY_TARGET_WAIT_SECONDS = 120

def seconds_until_imminent_target(games, now, max_wait_seconds=EARLY_TARGET_WAIT_SECONDS):
    """Return a bounded wait for the next fixed checkpoint target.

    A runner may reach the planner seconds before a target and otherwise exit
    without work. Waiting here changes neither the target nor its deadline;
    provider timestamps and the strict on-time evidence gate remain authoritative.
    Openers are intentionally excluded because their target is first observation.
    """
    now = instant(now)
    waits = []
    for game in games:
        if game.get('status', 'scheduled') != 'scheduled':
            continue
        kickoff = instant(game['kickoff'])
        for _, minutes in WINDOWS:
            seconds = (kickoff - dt.timedelta(minutes=minutes) - now).total_seconds()
            if 0 < seconds <= max_wait_seconds:
                waits.append(seconds)
    return min(waits, default=0.0)

def settlement(market,line):
    integer=float(line).is_integer()
    if market=='FULL_GAME_TOTAL':
        return ('Regulation plus overtime; push when final combined score equals line; book rules govern voids'
                if integer else 'Regulation plus overtime; no push at half-point line; book rules govern voids')
    if market=='FULL_GAME_SPREAD':
        return ('Regulation plus overtime; push when adjusted margin equals zero; book rules govern voids'
                if integer else 'Regulation plus overtime; no push at half-point spread; book rules govern voids')
    return 'First quarter only; no push at half-point line; book rules govern voids'

def pair_quotes(payload,allowed_books=None,sport='nfl'):
    books=BOOKMAKERS if allowed_books is None else allowed_books
    normalized=normalize(payload, allowed_books=books, sport=sport)
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
        # Tournament-selected de-vig (devig-tournament-v1): multiplicative,
        # q_i = p_i / R. Algebraically the same as the old inline formula,
        # but now a single source of truth shared with research.
        fair = devig_multiplicative(implied(quotes[0].american_odds),
                                    implied(quotes[1].american_odds))
        if fair is None:
            continue
        for q, fq in zip(quotes, fair):
            result.append({**asdict(q),'fair_probability':fq,
                'fair_method':'paired_same_book_conditional_on_no_push',
                'settlement_rules':settlement(market,line),
                'ny_licensed':q.sportsbook_key in NY_BOOK_KEYS})
    return result

def materialize_quotes(connection,sport='nfl'):
    """Fan captured checkpoint quotes out into the sport's odds-quotes table.

    The live collector stores quotes as a jsonb array on the checkpoint row;
    the picks engine and settlement CLV read the normalized odds-quotes
    table (migration 002 designed it for exactly this fan-out, but no code
    ever performed it -- so the forward shadow loop was silently starved of
    quotes). Idempotent: quote_id is a deterministic hash, so re-runs and
    the one-time backfill of old checkpoints insert nothing twice.
    Best-effort: a failure here must never break collection; the next run
    retries anything missed.

    sport selects the checkpoint/quotes tables via the sports registry.
    Default 'nfl' keeps every existing call site identical; 'ncaaf' writes
    only to ncaaf_edge_* tables, never to nfl_edge_*.
    """
    cp_table=checkpoints_table(sport)
    q_table=odds_quotes_table(sport)
    rows = connection.execute(
        """
        SELECT c.checkpoint_key, c.game_id, c.kickoff, c.quotes,
               g.home_team, g.away_team
        FROM public.%s c
        JOIN public.games g ON g.game_id = c.game_id
        WHERE c.status = 'captured'
          AND NOT EXISTS (SELECT 1 FROM public.%s q
                          WHERE q.checkpoint_key = c.checkpoint_key)
        """ % (cp_table,q_table)).fetchall()
    attempted = 0
    for r in rows:
        for q in (r["quotes"] or []):
            qid = hashlib.sha256("|".join([
                r["checkpoint_key"], str(q.get("provider_event_id")),
                str(q.get("market")), str(q.get("selection")),
                str(q.get("line")), str(q.get("sportsbook_key")),
                str(q.get("observed_at"))]).encode()).hexdigest()
            connection.execute(
                """
                INSERT INTO public.%s
                  (quote_id, checkpoint_key, provider_event_id, home_team,
                   away_team, kickoff, sportsbook, book_key, market,
                   selection, line, american_odds, fair_probability,
                   observed_at, settlement_rules, ny_licensed)
                VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)
                ON CONFLICT (quote_id) DO NOTHING
                """ % q_table,
                (qid, r["checkpoint_key"], q.get("provider_event_id"),
                 r["home_team"], r["away_team"], r["kickoff"],
                 q.get("sportsbook"), q.get("sportsbook_key"),
                 q.get("market"), q.get("selection"), q.get("line"),
                 q.get("american_odds"), q.get("fair_probability"),
                 q.get("observed_at"), q.get("settlement_rules"),
                 q.get("ny_licensed")))
            attempted += 1
    return {"checkpoints_materialized": len(rows),
            "quote_rows_attempted": attempted}


def parse_args(argv=None):
    import argparse
    ap=argparse.ArgumentParser(description='Shadow checkpoint collector')
    ap.add_argument('--sport',default='nfl',choices=('nfl','ncaaf'),
                    help="sport key from the ops.sports registry "
                         "(default 'nfl': existing behavior unchanged)")
    return ap.parse_args(argv)

def main(argv=None):
    import psycopg
    from psycopg.rows import dict_row
    sport=parse_args(argv).sport
    # Validates against the registry; raises ValueError on unknown sport.
    sport_config(sport)
    books,regions,n_regions=collector_env(sport)
    database=os.environ.get('NFL_EDGE_DATABASE_URL')
    if not database or not os.environ.get('THE_ODDS_API_KEY'):
        raise ValueError('NFL_EDGE_DATABASE_URL and THE_ODDS_API_KEY are required')
    if '-pooler.' in (urlsplit(database).hostname or ''):
        raise ValueError('Use a direct database connection for the session advisory lock')
    clock=lambda:dt.datetime.now(dt.timezone.utc)
    with psycopg.connect(database,autocommit=True,connect_timeout=10,row_factory=dict_row) as connection:
        if not connection.execute('SELECT pg_try_advisory_lock(%s) AS acquired'
                                  % ADVISORY_LOCKS[sport]).fetchone()['acquired']:
            print(json.dumps({'status':'already_running','sport':sport}));return
        connection.execute("SET statement_timeout='10s'")
        store=PostgresCheckpointStore(connection,table=checkpoints_table(sport))
        store.reconcile(clock())
        rows=connection.execute(
            "SELECT game_id,home_team,away_team,kickoff,status FROM public.games "
            "WHERE sport=%s AND status='scheduled' "
            "AND kickoff BETWEEN now()-interval '1 day' AND now()+interval '6 days'",
            (sport,)).fetchall()
        unique={}
        for r in rows:
            if sport=='nfl':
                k=(canonical(r['home_team']),canonical(r['away_team']),r['kickoff'])
            else:
                # NCAAF: exact provider strings; no alias table.
                k=(r['home_team'],r['away_team'],r['kickoff'])
            if k not in unique or r['game_id'].startswith(sport+'-'):unique[k]=r
        games=list(unique.values())
        # A runner may arrive seconds before a fixed checkpoint target; wait
        # (bounded) rather than exiting with zero work. This changes neither
        # the target nor its deadline.
        wait_seconds = seconds_until_imminent_target(games, clock())
        if wait_seconds:
            print(json.dumps({'status': 'waiting_for_checkpoint_target',
                              'sport': sport,
                              'seconds': round(wait_seconds, 3)}))
            time.sleep(wait_seconds + 0.25)
        # The events feed is free: always consult it for quota headers and to
        # gate opener attempts on the provider actually listing the event.
        events,headers=fetch_events(sport=sport)
        ok, quota_info = quota_reserve_ok(sport, headers)
        if not ok:
            print(json.dumps({'status':'quota_reserved_for_nfl','sport':sport,
                              **quota_info}))
            try:
                from heartbeat import record_heartbeat
                record_heartbeat(connection, HEARTBEAT_COMPONENTS[sport], {
                    'credits_remaining': quota_info['remaining'],
                    'quota_stand_down': True,
                })
            except Exception as e:  # noqa: BLE001 - heartbeat is advisory only
                print(json.dumps({'heartbeat':'failed',
                                  'reason':str(e)[:120]}))
            return
        event_keys=provider_event_keys(events)
        captured={r['game_id'] for r in connection.execute(
            "SELECT DISTINCT game_id FROM public.%s WHERE status='captured'"
            % checkpoints_table(sport)).fetchall()}
        by_id={r['game_id']:r for r in games}
        checkpoints=[c for c in plan(games,clock())
                     if opener_eligible(c,by_id[c.game_id],event_keys,captured,sport=sport)]
        def fetch(checkpoint):
            game=by_id[checkpoint.game_id]
            # Prefer the provider event id from an earlier captured checkpoint:
            # the live events feed drops commenced games, so re-matching it at
            # Close systematically fails (2026-09-20: 14/14 Close 'failed').
            event_id=resolve_provider_event_id(connection, store.table, checkpoint.game_id)
            if event_id is None:
                matches=find_provider_event(game,events,checkpoint.kickoff,sport)
                if len(matches)!=1: raise ValueError('Provider event did not match exact matchup and kickoff')
                event_id=matches[0]['id']
            payload,_=fetch_event_odds(event_id,['spreads','totals'],bookmakers=books,regions=regions,sport=sport)
            if payload.get('id')!=event_id or instant(payload['commence_time'])!=checkpoint.kickoff:
                raise ValueError('Provider response event changed')
            return pair_quotes(payload,allowed_books=books,sport=sport)
        _summary = collect(checkpoints,store,fetch,clock,
                         remaining=quota(headers)['remaining'],max_requests=8,cost_per_request=2*n_regions)
        try:
            # Fan captured checkpoints out to the normalized quotes table the
            # picks engine and settlement read. Backfills old checkpoints too.
            _summary['materialize'] = materialize_quotes(connection,sport=sport)
        except Exception as e:  # noqa: BLE001 - best effort; next run retries
            _summary['materialize'] = {'failed': str(e)[:120]}
        print(json.dumps({'status':'shadow_collection_only','sport':sport,**_summary}))
        try:
            # Best-effort pipeline heartbeat; must never break collection.
            from heartbeat import record_heartbeat
            record_heartbeat(connection, HEARTBEAT_COMPONENTS[sport], {
                'credits_remaining': quota(headers).get('remaining'),
                'checkpoints': {k: _summary.get(k) for k in
                                ('captured', 'missed', 'unavailable', 'duplicate',
                                 'deferred', 'failed', 'requests',
                                 'credits_budgeted')},
            })
        except Exception as e:  # noqa: BLE001 - heartbeat is advisory only
            print(json.dumps({'heartbeat': 'failed',
                              'reason': str(e)[:120]}))

if __name__=='__main__':
    try:main()
    except Exception:
        import traceback
        traceback.print_exc()
        print(json.dumps({'status':'failed','reason':'Worker prerequisites or collection failed; no recommendations published'}))
        sys.exit(1)
