import unittest
from unittest.mock import patch

from model import provider_oddsapi as provider


class ProviderOddsEventFilterTests(unittest.TestCase):
    def test_filters_a_single_sport_request_to_the_requested_events(self):
        with patch.object(provider, '_get', return_value=([], {})) as request:
            provider.fetch_odds(
                ['spreads', 'totals'], api_key='synthetic-key',
                bookmakers=['fanduel', 'draftkings'], event_ids=['event-a', 'event-b'],
            )
        request.assert_called_once()
        url, params, _ = request.call_args.args
        self.assertEqual(url, provider.BASE + '/sports/americanfootball_nfl/odds')
        self.assertEqual(params['eventIds'], 'event-a,event-b')
        self.assertEqual(params['markets'], 'spreads,totals')
        self.assertEqual(params['bookmakers'], 'fanduel,draftkings')

    def test_invalid_explicit_filters_never_send_a_request(self):
        with patch.object(provider, '_get') as request:
            for ids in ([], 'event-a', [''], [' event'], ['a,b'], ['a', 'a'], [None]):
                with self.subTest(ids=ids), self.assertRaises(ValueError):
                    provider.fetch_odds(['spreads'], api_key='synthetic-key', event_ids=ids)
        request.assert_not_called()

    def test_existing_unfiltered_call_keeps_its_parameters(self):
        with patch.object(provider, '_get', return_value=([], {})) as request:
            provider.fetch_odds(['spreads'], api_key='synthetic-key')
        self.assertNotIn('eventIds', request.call_args.args[1])

    def test_routes_unambiguous_events_and_excludes_unrequested_ids(self):
        first = {'id': 'a', 'sport_key': provider.SPORT, 'home_team': 'Home',
                 'away_team': 'Away', 'commence_time': '2026-11-15T18:00:00Z'}
        second = {**first, 'id': 'b', 'home_team': 'Other'}
        payload = {**first, 'commence_time': '2026-11-15T13:00:00-05:00',
                   'bookmakers': []}
        self.assertEqual(
            provider.match_nfl_slate_odds([first, second], [payload, {**first, 'id': 'c'}]),
            {'a': payload},
        )

    def test_withholds_conflicting_and_duplicate_events_independently(self):
        first = {'id': 'a', 'sport_key': provider.SPORT, 'home_team': 'Home',
                 'away_team': 'Away', 'commence_time': '2026-11-15T18:00:00Z'}
        second = {**first, 'id': 'b', 'home_team': 'Other'}
        for conflicting in (
            [{**first, 'away_team': 'Other'}],
            [{**first, 'sport_key': 'basketball_nba'}],
            [{**first, 'commence_time': '2026-11-15T18:10:00Z'}],
            [{**first, 'commence_time': '2026-11-15T18:00:00'}],
            [first, first],
        ):
            with self.subTest(conflicting=conflicting):
                self.assertEqual(
                    provider.match_nfl_slate_odds([first, second], [*conflicting, second]),
                    {'b': second},
                )
        self.assertEqual(provider.match_nfl_slate_odds([first, first, second], [first, second]),
                         {'b': second})
        with self.assertRaises(ValueError):
            provider.match_nfl_slate_odds([first], {'id': 'a'})
