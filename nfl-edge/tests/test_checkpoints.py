import datetime as dt
import pathlib
import sys
import unittest
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'ops'))
from checkpoints import plan,collect,instant,safe_collection_error
from collect_checkpoints import (pair_quotes, provider_event_keys, opener_eligible,
                                 seconds_until_imminent_target)

class MemoryStore:
    def __init__(self): self.rows={}
    def claim(self,c,now):
        if c.key in self.rows:return False
        self.rows[c.key]={'status':'running'}; return True
    def finish(self,c,now,status,quotes,error): self.rows[c.key]={'status':status,'quotes':quotes,'ended_at':now,'error':error}

class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.kickoff=instant('2026-09-13T17:00:00Z')
        self.game={'game_id':'g1','kickoff':self.kickoff,'status':'scheduled'}
        self.now=self.kickoff-dt.timedelta(hours=3)
    def test_imminent_target_wait_is_bounded(self):
        # Regression: run 34769944280 reached the worker 19 seconds before
        # Close, then exited with zero requests because plan() omits future work.
        before_close = self.kickoff - dt.timedelta(minutes=5, seconds=19)
        self.assertEqual(seconds_until_imminent_target([self.game], before_close), 19)
        self.assertEqual(seconds_until_imminent_target(
            [self.game], self.kickoff-dt.timedelta(minutes=7, seconds=1)), 0)
        self.assertEqual(seconds_until_imminent_target(
            [self.game], self.kickoff-dt.timedelta(minutes=5)), 0)
        self.assertEqual(seconds_until_imminent_target(
            [{**self.game, 'status': 'final'}], before_close), 0)

    def test_deadlines_and_missed_checkpoints(self):
        checkpoints=plan([self.game],self.now)
        self.assertEqual([(c.name,c.state) for c in checkpoints],[('T-24','missed'),('T-3','due')])
        close=plan([self.game],self.kickoff)
        self.assertTrue(all(c.state=='missed' for c in close))
        with self.assertRaises(ValueError): plan([{**self.game,'kickoff':'2026-09-13T17:00:00'}],self.now)
    def test_wide_window_survives_realistic_collector_cadence(self):
        # Regression: all 14 T-24 checkpoints for the 2026-09-13 Sunday slate
        # were missed because no collector run fell inside the old 15-minute
        # window. A run landing 30 minutes after the T-24 target must capture.
        at_target=self.kickoff-dt.timedelta(hours=24)
        t24=[c for c in plan([self.game],at_target) if c.name=='T-24']
        self.assertEqual([(c.name,c.state) for c in t24],[('T-24','due')])
        self.assertEqual(t24[0].deadline,self.kickoff-dt.timedelta(hours=22,minutes=30))
        late=at_target+dt.timedelta(minutes=30)
        cps=[c for c in plan([self.game],late) if c.name=='T-24']
        self.assertEqual(cps[0].state,'due')
        self.assertEqual(cps[0].key,t24[0].key)  # same checkpoint, later run
        store=MemoryStore()
        collect(cps,store,lambda c:[{'observed_at':late.isoformat()}],lambda:late,100)
        self.assertEqual(store.rows[cps[0].key]['status'],'captured')
    def test_deadline_never_past_kickoff(self):
        # T-90 and Close windows are capped strictly before kickoff.
        for name,offset in (('T-90',dt.timedelta(minutes=45)),('Close',dt.timedelta(minutes=3))):
            now=self.kickoff-offset
            cps=[c for c in plan([self.game],now) if c.name==name]
            self.assertTrue(cps)
            self.assertLess(cps[0].deadline,self.kickoff)
            self.assertLessEqual(cps[0].deadline,cps[0].target+dt.timedelta(minutes=90))
    def test_duplicates_do_not_repeat_paid_requests(self):
        store=MemoryStore(); calls=[]
        fetch=lambda c: calls.append(c.key) or [{'observed_at':self.now.isoformat()}]
        cps=plan([self.game,self.game],self.now)
        collect(cps,store,fetch,lambda:self.now,remaining=100)
        collect(cps,store,fetch,lambda:self.now,remaining=100)
        self.assertEqual(len(calls),1)
        self.assertEqual(sorted(r['status'] for r in store.rows.values()),['captured','missed'])
    def test_quote_before_target_and_response_after_deadline_rejected(self):
        cps=[c for c in plan([self.game],self.now) if c.state=='due']
        store=MemoryStore()
        collect(cps,store,lambda c:[{'observed_at':(self.now-dt.timedelta(seconds=1)).isoformat()}],lambda:self.now,100)
        self.assertEqual(store.rows[cps[0].key]['status'],'unavailable')
        times=iter([self.now,self.now,self.now+dt.timedelta(minutes=91)])
        store=MemoryStore()
        collect(cps,store,lambda c:[{'observed_at':self.now.isoformat()}],lambda:next(times),100)
        self.assertEqual(store.rows[cps[0].key]['status'],'missed')
    def test_collection_failure_reason_is_safe_and_actionable(self):
        cp=[c for c in plan([self.game],self.now) if c.state=='due'][0]
        store=MemoryStore()
        def provider_failure(_):
            raise RuntimeError('Odds provider request failed with HTTP 404')
        collect([cp],store,provider_failure,lambda:self.now,100)
        self.assertEqual(store.rows[cp.key]['status'],'failed')
        self.assertEqual(store.rows[cp.key]['error'],
                         'Odds provider request failed with HTTP 404')
        secret='https://provider.test/?apiKey=do-not-store'
        self.assertEqual(safe_collection_error(RuntimeError(secret)),
                         'Collection failed; inspect private runtime health')
        self.assertNotIn('apiKey',safe_collection_error(RuntimeError(secret)))

    def test_quota_unknown_or_exhausted_does_not_claim_due_work(self):
        cps=[c for c in plan([self.game],self.now) if c.state=='due']
        for remaining in [None,11,0]:
            store=MemoryStore()
            counts=collect(cps,store,lambda c:self.fail('Paid request without quota'),lambda:self.now,remaining,cost_per_request=2)
            self.assertFalse(store.rows); self.assertEqual(counts['deferred'],1)
        store=MemoryStore()
        counts=collect(cps,store,lambda c:[{'observed_at':self.now.isoformat()}],lambda:self.now,12,cost_per_request=2)
        self.assertEqual(counts['credits_budgeted'],2)
    def test_reschedule_has_new_identity(self):
        old=plan([self.game],self.now)
        new=plan([{**self.game,'kickoff':self.kickoff+dt.timedelta(minutes=5)}],self.now+dt.timedelta(minutes=5))
        self.assertTrue(set(c.key for c in old).isdisjoint(c.key for c in new))

    def test_pair_quotes_uses_multiplicative_devig(self):
        # Over +110 / Under -130: p_over=100/210, p_under=130/230,
        # R=p_over+p_under; fair = p/R (tournament-selected multiplicative).
        markets = [
            {'key': 'totals', 'last_update': self.now.isoformat(), 'outcomes': [
                {'name': 'Over', 'price': 110, 'point': 44},
                {'name': 'Under', 'price': -130, 'point': 44}]},
        ]
        event = {'id': 'g2', 'home_team': 'Home', 'away_team': 'Away',
                 'bookmakers': [{'key': 'draftkings', 'title': 'DraftKings',
                                  'markets': markets}]}
        pairs = pair_quotes(event)
        self.assertEqual(len(pairs), 2)
        p_over, p_under = 100 / 210, 130 / 230
        r = p_over + p_under
        got = {q['selection']: q['fair_probability'] for q in pairs}
        self.assertAlmostEqual(got['Over'], p_over / r)
        self.assertAlmostEqual(got['Under'], p_under / r)

    def test_same_book_exact_full_game_pairs_and_push_rules(self):
        markets=[
            {'key':'totals','last_update':self.now.isoformat(),'outcomes':[
                {'name':'Over','price':110,'point':44},{'name':'Under','price':-130,'point':44}]},
            {'key':'spreads','last_update':self.now.isoformat(),'outcomes':[
                {'name':'Home','price':-105,'point':-3.5},{'name':'Away','price':-115,'point':3.5}]},
        ]
        event={'id':'g1','home_team':'Home','away_team':'Away','bookmakers':[{'key':'draftkings','title':'DraftKings','markets':markets}]}
        pairs=pair_quotes(event)
        self.assertEqual(len(pairs),4)
        self.assertEqual({q['market'] for q in pairs},{'FULL_GAME_TOTAL','FULL_GAME_SPREAD'})
        self.assertAlmostEqual(sum(q['fair_probability'] for q in pairs if q['market']=='FULL_GAME_TOTAL'),1)
        self.assertTrue(all(q['fair_method']=='paired_same_book_conditional_on_no_push' for q in pairs))
        self.assertIn('push',next(q['settlement_rules'] for q in pairs if q['market']=='FULL_GAME_TOTAL'))
        markets[1]['outcomes'][1]['point']=4.5
        self.assertFalse(any(q['market']=='FULL_GAME_SPREAD' for q in pair_quotes(event)))

if __name__=='__main__':unittest.main()


class OpenerTests(unittest.TestCase):
    def setUp(self):
        self.kickoff=instant('2026-09-20T17:00:00Z')  # a Sunday game
        self.game={'game_id':'g-open','kickoff':self.kickoff,'status':'scheduled',
                   'home_team':'KC','away_team':'BUF'}
    def test_opener_planned_for_mid_range_game(self):
        now=self.kickoff-dt.timedelta(days=3)
        cps=plan([self.game],now)
        self.assertEqual([(c.name,c.state) for c in cps],[('Opener','due')])
        cp=cps[0]
        self.assertEqual(cp.target,instant(now))
        self.assertEqual(cp.deadline,self.kickoff-dt.timedelta(hours=24))
    def test_opener_not_planned_outside_window(self):
        # 12h out: regular windows own it; 7 days out: nothing planned.
        near=plan([self.game],self.kickoff-dt.timedelta(hours=12))
        self.assertFalse(any(c.name=='Opener' for c in near))
        self.assertTrue(any(c.name=='T-24' for c in near))
        far=plan([self.game],self.kickoff-dt.timedelta(days=7))
        self.assertEqual(far,[])
    def test_opener_key_changes_daily(self):
        now=self.kickoff-dt.timedelta(days=3)
        a=plan([self.game],now)[0].key
        b=plan([self.game],now+dt.timedelta(days=1))[0].key
        self.assertNotEqual(a,b)
    def test_opener_expires_when_t24_takes_over(self):
        # Planned at 25h out, deadline is 1h away; still due, never missed-spend.
        cps=plan([self.game],self.kickoff-dt.timedelta(hours=25))
        self.assertEqual([(c.name,c.state) for c in cps],[('Opener','due')])
    def test_opener_gate(self):
        now=self.kickoff-dt.timedelta(days=3)
        cp=plan([self.game],now)[0]
        events=[{'home_team':'Kansas City Chiefs','away_team':'Buffalo Bills',
                 'commence_time':'2026-09-20T17:00:00Z'}]
        keys=provider_event_keys(events)
        # No provider event yet -> skip without claiming.
        self.assertFalse(opener_eligible(cp,self.game,set(),set()))
        # Provider lists it, nothing captured -> eligible.
        self.assertTrue(opener_eligible(cp,self.game,keys,set()))
        # Already captured once -> opener value is gone, skip.
        self.assertFalse(opener_eligible(cp,self.game,keys,{'g-open'}))
        # Regular checkpoints are never gated.
        t24=[c for c in plan([self.game],self.kickoff-dt.timedelta(hours=12))
             if c.name=='T-24'][0]
        game12={**self.game}
        self.assertTrue(opener_eligible(t24,game12,set(),{'g-open'}))
    def test_opener_accepts_stale_first_seen_quotes(self):
        # Opener at 3 days out; provider last updated the line 6 hours ago.
        # The old window-freshness filter rejected this (nothing was ever
        # "first seen"); the opener path must capture it.
        now = self.kickoff - dt.timedelta(days=3)
        cps = [c for c in plan([self.game], now) if c.name == 'Opener']
        self.assertTrue(cps)
        store = MemoryStore()
        stale = (now - dt.timedelta(hours=6)).isoformat()
        collect(cps, store,
                lambda c: [{'observed_at': stale}],
                lambda: now, 100)
        self.assertEqual(store.rows[cps[0].key]['status'], 'captured')
        self.assertEqual(len(store.rows[cps[0].key]['quotes']), 1)

    def test_opener_rejects_future_observed_quotes(self):
        now = self.kickoff - dt.timedelta(days=3)
        cps = [c for c in plan([self.game], now) if c.name == 'Opener']
        store = MemoryStore()
        future = (now + dt.timedelta(hours=1)).isoformat()
        collect(cps, store,
                lambda c: [{'observed_at': future}],
                lambda: now, 100)
        self.assertEqual(store.rows[cps[0].key]['status'], 'unavailable')

class FakeConnection:
    """Minimal stand-in for the psycopg connection used by materialize_quotes."""
    def __init__(self, pending):
        self.pending = pending  # rows returned by the SELECT
        self.inserts = []       # (sql, params) for INSERTs
    def execute(self, sql, params=None):
        if "nfl_edge_checkpoints c" in sql:
            return FakeResult(self.pending)
        self.inserts.append((sql, params))
        return FakeResult([])
class FakeResult:
    def __init__(self, rows): self._rows = rows
    def fetchall(self): return self._rows

def _quote_row(**kw):
    q = {"provider_event_id": "ev1", "market": "FULL_GAME_SPREAD",
         "selection": "Kansas City Chiefs", "line": -3.0,
         "american_odds": -110, "fair_probability": 0.5238,
         "observed_at": "2026-09-12T12:00:00+00:00",
         "sportsbook": "DraftKings", "sportsbook_key": "draftkings",
         "settlement_rules": "rules", "ny_licensed": True}
    q.update(kw)
    return q

class MaterializeTests(unittest.TestCase):
    def test_materializes_checkpoint_quotes(self):
        from collect_checkpoints import materialize_quotes
        conn = FakeConnection([{
            "checkpoint_key": "ck1", "game_id": "g1",
            "kickoff": dt.datetime(2026, 9, 13, 17, 0, tzinfo=dt.timezone.utc),
            "quotes": [_quote_row(), _quote_row(sportsbook_key="fanduel",
                                                sportsbook="FanDuel")],
            "home_team": "Kansas City Chiefs", "away_team": "Buffalo Bills"}])
        out = materialize_quotes(conn)
        self.assertEqual(out["checkpoints_materialized"], 1)
        self.assertEqual(len(conn.inserts), 2)
        params = conn.inserts[0][1]
        # column order: quote_id, checkpoint_key, provider_event_id,
        # home_team, away_team, kickoff, sportsbook, book_key, market,
        # selection, line, american_odds, fair_probability, observed_at,
        # settlement_rules, ny_licensed
        self.assertEqual(params[1], "ck1")
        self.assertEqual(params[3], "Kansas City Chiefs")
        self.assertEqual(params[4], "Buffalo Bills")
        self.assertEqual(params[7], "draftkings")
        self.assertEqual(params[8], "FULL_GAME_SPREAD")
        self.assertAlmostEqual(params[12], 0.5238)
        # deterministic quote_id: same inputs -> same id (idempotent)
        conn2 = FakeConnection(conn.pending)
        from collect_checkpoints import materialize_quotes as m2
        m2(conn2)
        self.assertEqual(conn.inserts[0][1][0], conn2.inserts[0][1][0])
        self.assertNotEqual(conn.inserts[0][1][0], conn.inserts[1][1][0])

    def test_skips_already_materialized_checkpoints(self):
        from collect_checkpoints import materialize_quotes
        conn = FakeConnection([])  # NOT EXISTS filtered everything
        out = materialize_quotes(conn)
        self.assertEqual(out["checkpoints_materialized"], 0)
        self.assertEqual(conn.inserts, [])

    def test_empty_quotes_array_materializes_checkpoint(self):
        from collect_checkpoints import materialize_quotes
        conn = FakeConnection([{
            "checkpoint_key": "ck2", "game_id": "g2",
            "kickoff": dt.datetime(2026, 9, 13, 17, 0, tzinfo=dt.timezone.utc),
            "quotes": [], "home_team": "A", "away_team": "B"}])
        out = materialize_quotes(conn)
        self.assertEqual(out["checkpoints_materialized"], 1)
        self.assertEqual(conn.inserts, [])

