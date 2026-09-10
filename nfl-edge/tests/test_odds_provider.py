import pathlib
import sys
import traceback
import unittest
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).parents[1]/'model'))
from provider_oddsapi import fetch_events,normalize

class ProviderOddsTests(unittest.TestCase):
    def payload(self,key,outcomes):
        return {'id':'g1','home_team':'Home','away_team':'Away','bookmakers':[{
            'key':'draftkings','title':'DraftKings','markets':[{
                'key':key,'last_update':'2026-09-10T09:00:00Z','outcomes':outcomes}]}]}

    def test_full_game_totals_accept_integer_and_half_point_lines(self):
        for line in (44,44.5):
            q=normalize(self.payload('totals',[{'name':'Over','point':line,'price':-110},{'name':'Under','point':line,'price':-110}]))
            self.assertEqual(len(q),2)
            self.assertTrue(all(x.market=='FULL_GAME_TOTAL' for x in q))

    def test_full_game_spreads_preserve_signed_team_lines(self):
        q=normalize(self.payload('spreads',[{'name':'Home','point':-3.5,'price':-105},{'name':'Away','point':3.5,'price':-115}]))
        self.assertEqual({x.line for x in q},{-3.5,3.5})
        self.assertEqual({x.selection for x in q},{'Home','Away'})

    def test_rejects_unmapped_books_markets_and_quarter_lines(self):
        q1_integer=self.payload('totals_q1',[{'name':'Over','point':7,'price':-110},{'name':'Under','point':7,'price':-110}])
        self.assertEqual(normalize(q1_integer),[])
        unmapped=self.payload('h2h',[{'name':'Home','point':0,'price':-110},{'name':'Away','point':0,'price':-110}])
        self.assertEqual(normalize(unmapped),[])
        offshore=self.payload('totals',[{'name':'Over','point':44.5,'price':-110},{'name':'Under','point':44.5,'price':-110}])
        offshore['bookmakers'][0]['key']='offshore'
        self.assertEqual(normalize(offshore),[])

    def test_transport_error_does_not_leak_api_key(self):
        secret='secret-do-not-log'
        class RequestException(Exception):pass
        def fail(*args,**kwargs):raise RequestException(f'failed URL apix apiKey={secret}')
        requests=SimpleNamespace(RequestException=RequestException,get=fail)
        with patch.dict(sys.modules,{'requests':requests}):
            with self.assertRaises(RuntimeError) as caught:fetch_events(api_key=secret)
        self.assertEqual(str(caught.exception),'Odds provider request failed')
        self.assertNotIn(secret,''.join(traceback.format_exception(caught.exception)))
        bad=self.payload('totals',[{'name':'Over','point':44.25,'price':-110},{'name':'Under','point':44.25,'price':-110}])
        self.assertEqual(normalize(bad),[])
        bad['bookmakers'][0]['key']='offshore'
        self.assertEqual(normalize(bad),[])

if __name__=='__main__':unittest.main()
