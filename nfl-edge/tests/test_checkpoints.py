import datetime as dt
import pathlib
import sys
import unittest
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'ops'))
from checkpoints import plan,collect,instant
from collect_checkpoints import pair_quotes

class MemoryStore:
    def __init__(self): self.rows={}
    def claim(self,c,now):
        if c.key in self.rows:return False
        self.rows[c.key]={'status':'running'}; return True
    def finish(self,c,now,status,quotes,error): self.rows[c.key]={'status':status,'quotes':quotes,'ended_at':now}

class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.kickoff=instant('2026-09-13T17:00:00Z')
        self.game={'game_id':'g1','kickoff':self.kickoff,'status':'scheduled'}
        self.now=self.kickoff-dt.timedelta(hours=3)
    def test_deadlines_and_missed_checkpoints(self):
        checkpoints=plan([self.game],self.now)
        self.assertEqual([(c.name,c.state) for c in checkpoints],[('T-24','missed'),('T-3','due')])
        close=plan([self.game],self.kickoff)
        self.assertTrue(all(c.state=='missed' for c in close))
        with self.assertRaises(ValueError): plan([{**self.game,'kickoff':'2026-09-13T17:00:00'}],self.now)
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
        times=iter([self.now,self.now,self.now+dt.timedelta(minutes=16)])
        store=MemoryStore()
        collect(cps,store,lambda c:[{'observed_at':self.now.isoformat()}],lambda:next(times),100)
        self.assertEqual(store.rows[cps[0].key]['status'],'missed')
    def test_quota_unknown_or_exhausted_does_not_claim_due_work(self):
        cps=[c for c in plan([self.game],self.now) if c.state=='due']
        for remaining in [None,10,0]:
            store=MemoryStore()
            counts=collect(cps,store,lambda c:self.fail('Paid request without quota'),lambda:self.now,remaining)
            self.assertFalse(store.rows); self.assertEqual(counts['deferred'],1)
    def test_reschedule_has_new_identity(self):
        old=plan([self.game],self.now)
        new=plan([{**self.game,'kickoff':self.kickoff+dt.timedelta(minutes=5)}],self.now+dt.timedelta(minutes=5))
        self.assertTrue(set(c.key for c in old).isdisjoint(c.key for c in new))

    def test_same_book_exact_line_pair_is_required(self):
        outcomes=[{'name':'Over','price':110,'point':7.5},{'name':'Under','price':-130,'point':7.5}]
        market={'key':'totals_q1','last_update':self.now.isoformat(),'outcomes':outcomes}
        event={'id':'g1','bookmakers':[{'key':'draftkings','title':'DraftKings','markets':[market]}]}
        pairs=pair_quotes(event)
        self.assertEqual(len(pairs),2)
        self.assertAlmostEqual(sum(q['fair_probability'] for q in pairs),1)
        outcomes[1]['point']=10.5
        self.assertEqual(pair_quotes(event),[])

if __name__=='__main__':unittest.main()
