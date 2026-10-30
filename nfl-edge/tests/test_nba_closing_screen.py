import unittest
from research.nba_closing_screen import screen, summarize


def game(gid, date, home, away, line='-2.5', hs=110, aws=100):
    margin = hs-aws+float(line)
    return dict(game_id=gid, date_et=date, start_time_utc=date+'T23:00:00Z',
                season='2024-25', season_type='regular', home_team_id=home,
                away_team_id=away, home_team=home, away_team=away,
                close_spread_home=line, home_score=str(hs), away_score=str(aws),
                ats_result='home' if margin > 0 else 'away' if margin < 0 else 'push',
                ats_margin=str(margin))


class ClosingScreenTest(unittest.TestCase):
    def population(self):
        return [game('a','2025-01-01','A','X'), game('b','2025-01-02','B','Y'),
                game('c','2025-01-03','A','B',line='-12.5')]

    def test_away_dog_and_fade_have_opposite_results(self):
        r = screen(self.population())
        self.assertEqual(r['b2b_underdog']['wins'],1)
        self.assertEqual(r['fade_b2b']['losses'],1)

    def test_input_order_does_not_leak_future_games(self):
        rows = self.population()
        self.assertEqual(screen(rows), screen(list(reversed(rows))))

    def test_first_games_and_december_are_excluded(self):
        r = screen([game('a','2024-12-01','A','B')])
        self.assertEqual(r['selected_game_count'],0)
        self.assertEqual(r['exclusions']['december_missing_cup_schedule'],1)

    def test_duplicate_and_wrong_ats_fail(self):
        row = self.population()[0]
        with self.assertRaises(ValueError): screen([row,row])
        row['ats_result']='away'
        with self.assertRaises(ValueError): screen([row])

    def test_timezone_and_integer_line(self):
        rows = self.population()
        rows[-1]['close_spread_home']='-10'
        rows[-1]['ats_margin']='0'
        rows[-1]['ats_result']='push'
        self.assertEqual(screen(rows)['b2b_underdog']['decisions'],0)
        rows[0]['date_et']='2025-01-02'
        with self.assertRaises(ValueError): screen(rows)

    def test_unit_conventions(self):
        r = summarize(['win']*74+['loss']*33)
        self.assertAlmostEqual(float(r['hypothetical_minus110_net_units']),74*100/110-33)
        self.assertAlmostEqual(r['cover_rate'],74/107)

    def test_unregistered_or_missing_season_fails_before_grading(self):
        for season in ('2025-26', '', None):
            row = self.population()[0]
            row['season'] = season
            row['ats_result'] = 'invalid'
            with self.assertRaisesRegex(ValueError, 'Protocol requires season'):
                screen([row])
        row = self.population()[0]
        del row['season']
        with self.assertRaisesRegex(ValueError, 'Protocol requires season'):
            screen([row])

    def test_relabelled_next_season_and_mixed_seasons_fail(self):
        row = game('later', '2025-12-01', 'A', 'B')
        with self.assertRaisesRegex(ValueError, 'outside registered season'):
            screen([row])
        rows = self.population()
        rows[-1]['season'] = '2025-26'
        with self.assertRaisesRegex(ValueError, 'Protocol requires season'):
            screen(rows)


if __name__ == '__main__': unittest.main()
