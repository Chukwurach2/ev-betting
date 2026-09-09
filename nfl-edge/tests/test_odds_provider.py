import pathlib
import sys
import unittest
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
from provider_oddsapi import normalize
class ProviderTests(unittest.TestCase):
    def event(self, timestamp='2026-09-09T14:00:00Z', point=7.5, key='totals_q1', book='draftkings'):
        return {'id':'g1','bookmakers':[{'key':book,'title':book,'markets':[{'key':key,'last_update':timestamp,'outcomes':[{'name':'Over','price':-110,'point':point},{'name':'Under','price':-110,'point':point}]}]}]}
    def test_only_supported_exact_contracts(self):
        self.assertEqual(len(normalize(self.event())),2)
        for e in [self.event(point=7),self.event(point=float('nan')),self.event(key='team_totals_q1'),self.event(book='bet365')]:
            self.assertEqual(normalize(e),[])
    def test_bad_or_missing_time_is_rejected(self):
        for stamp in ['bad',None,'2026-09-09T14:00:00']:
            self.assertEqual(normalize(self.event(timestamp=stamp)),[])
if __name__=='__main__':unittest.main()
