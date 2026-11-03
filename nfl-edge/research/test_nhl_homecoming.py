import unittest
from datetime import datetime, timedelta, timezone
from research.nhl_homecoming import audit_schedule, evaluate, start, weekly_sums, bootstrap, EASTERN


def fixture():
    #32teams,82games each. Repeated pairs rotate who hosts in4-game blocks.
    rows = []
    for day in range(82):
        for pair in range(16):
            h, a = str(pair*2), str(pair*2+1)
            if (day//4) % 2:
                h, a = a, h
            t = datetime(2025, 10, 1, 23, tzinfo=timezone.utc) + timedelta(days=day*2)
            rows.append(dict(game_id=f'{day}-{pair}', season_type='regular', home_team_id=h,
                             away_team_id=a, home_team=h, away_team=a,
                             start_time_utc=t.isoformat(), date_et=t.astimezone(EASTERN).date().isoformat(),
                             close_home_win_prob='.6', home_score='3', away_score='2', moneyline_winner='home'))
    return rows


class HomecomingTests(unittest.TestCase):
    def test_schedule_is_outcome_blind(self):
        rows = fixture()
        for r in rows:
            for k in ('home_score', 'away_score', 'moneyline_winner'):
                del r[k]
        regular, selected, audit = audit_schedule(rows)
        self.assertEqual(len(regular), 1312)
        self.assertEqual(audit['sample_gate'], 'sufficient')
        self.assertEqual(audit['treated_home_teams'], 32)
        self.assertIn('4-0', selected)
        self.assertNotIn('5-0', selected)

    def test_order_does_not_change_exposure(self):
        rows = fixture()
        self.assertEqual(audit_schedule(rows)[1], audit_schedule(list(reversed(rows)))[1])

    def test_no_future_schedule_changes_past_exposure(self):
        rows = fixture()
        before = audit_schedule(rows)[1]
        #Flip the final game designation; all earlier decisions stay unchanged.
        r = rows[-1]
        r['home_team_id'], r['away_team_id'] = r['away_team_id'], r['home_team_id']
        r['home_team'], r['away_team'] = r['away_team'], r['home_team']
        self.assertEqual(before-{'81-15'}, audit_schedule(rows)[1]-{'81-15'})

    def test_wrong_coverage_and_dates_rejected(self):
        rows = fixture()
        with self.assertRaises(ValueError): audit_schedule(rows[:-1])
        rows[0]['date_et'] = '2024-10-01'
        with self.assertRaises(ValueError): audit_schedule(rows)

    def test_duplicate_identity_and_bad_probability_rejected(self):
        rows = fixture()
        rows[1]['game_id'] = rows[0]['game_id']
        with self.assertRaises(ValueError): audit_schedule(rows)
        rows = fixture()
        rows[0]['close_home_win_prob'] = 'nan'
        with self.assertRaises(ValueError): audit_schedule(rows)

    def test_feasibility_abort_does_not_need_scores(self):
        rows = fixture()
        for r in rows:
            #Alternating home/away removes all roadtrip exposure.
            day = int(r['game_id'].split('-')[0])
            h, a = sorted((r['home_team_id'], r['away_team_id']), key=int)
            if day % 2: h, a = a, h
            r.update(home_team_id=h, away_team_id=a, home_team=h, away_team=a)
            del r['home_score']
        self.assertEqual(evaluate(rows)['status'], 'data_infeasible_no_outcomes_inspected')

    def test_invalid_final_rejected(self):
        rows = fixture()
        rows[0]['away_score'] = '3'
        with self.assertRaises(ValueError): evaluate(rows)
        rows[0]['away_score'] = '2'
        rows[0]['moneyline_winner'] = 'away'
        with self.assertRaises(ValueError): evaluate(rows)

    def test_empty_calendar_weeks_retained(self):
        records = [dict(start_time_utc=t, treated=True, residual=.1)
                   for t in ('2025-10-06T23:00:00Z', '2025-10-20T23:00:00Z')]
        weeks = weekly_sums(records)
        self.assertEqual(len(weeks), 3)
        self.assertEqual(weeks[1][1:], (0.0, 0, 0.0, 0))

    def test_bootstrap_fixed_zero_contrast(self):
        b = bootstrap([('2025-10-06', 1., 2, 2., 4)])
        self.assertEqual(b['valid_draws'], 20000)
        self.assertEqual(b['interval95'], [0., 0.])
        self.assertEqual(b, bootstrap([('2025-10-06', 1., 2, 2., 4)]))

    def test_timezone_required(self):
        with self.assertRaises(ValueError): start({'start_time_utc':'2025-10-01T12:00:00'})


if __name__ == '__main__':
    unittest.main()
