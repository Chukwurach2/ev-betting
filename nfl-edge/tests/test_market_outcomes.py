"""Unit tests for ops/market_outcomes.py (pure logic; no DB, no API)."""
import datetime as dt
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ops"))
import market_outcomes as mo


def _game(**kw):
    g = {"season": 2024, "homeTeam": "Ohio State", "awayTeam": "Michigan",
         "startDate": "2024-11-30T17:00:00.000Z", "completed": True,
         "homePoints": 24, "awayPoints": 20,
         "homeConference": "Big Ten", "awayConference": "Big Ten",
         "homeClassification": "fbs", "awayClassification": "fbs"}
    g.update(kw)
    return g


class TestMatching(unittest.TestCase):
    def setUp(self):
        games = [_game()]
        self.idx = {}
        for g in games:
            key = (g["season"], mo.norm_name(g["homeTeam"]),
                   mo.norm_name(g["awayTeam"]))
            self.idx.setdefault(key, []).append(g)

    def test_exact_match(self):
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, self.idx)
        self.assertIn(("e1", 2024), matched)
        self.assertEqual(integ["matched"], 1)

    def test_name_normalization(self):
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("  OHIO-state!! ", "michigan", ko)}
        matched, _ = mo.match_events(events, self.idx)
        self.assertIn(("e1", 2024), matched)

    def test_mascot_suffix_prefix_fallback(self):
        # provider: '{School} {Mascot}'; CFBD: school name only
        g = _game(homeTeam="Duke", awayTeam="Florida State")
        idx = {}
        key = (2024, mo.norm_name("Duke"), mo.norm_name("Florida State"))
        idx[key] = [g]
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Duke Blue Devils",
                                 "Florida State Seminoles", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertIn(("e1", 2024), matched)
        self.assertEqual(integ["matched"], 1)

    def test_kickoff_outside_tolerance(self):
        ko = dt.datetime(2024, 12, 5, 17, 0, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, self.idx)
        self.assertNotIn(("e1", 2024), matched)
        self.assertEqual(integ["unmatched_reasons"].get("kickoff_miss"), 1)

    def test_incomplete_game_excluded(self):
        g = _game(completed=False)
        idx = {(2024, "ohiostate", "michigan"): [g]}
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertEqual(integ["unmatched_reasons"].get("not_completed"), 1)
        self.assertNotIn(("e1", 2024), matched)

    def test_null_scores_excluded(self):
        g = _game(homePoints=None, awayPoints=None)
        idx = {(2024, "ohiostate", "michigan"): [g]}
        ko = dt.datetime(2024, 11, 30, 17, 5, tzinfo=dt.timezone.utc)
        events = {("e1", 2024): ("Ohio State", "Michigan", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertEqual(integ["unmatched_reasons"].get("null_scores"), 1)

    def test_load_games_filters_2025(self):
        games = [_game(season=2024), _game(season=2025)]
        import json, tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json",
                                         delete=False) as f:
            json.dump(games, f)
            path = f.name
        idx = mo.load_games(path)
        seasons = {k[0] for k in idx}
        self.assertEqual(seasons, {2024})
        os.unlink(path)


class TestOutcomes(unittest.TestCase):
    def _selected(self, line, prob, market="FULL_GAME_SPREAD"):
        return {("e1", 2024, 1, "early", "draftkings", market):
                {"line": line, "fair_prob": prob, "home": "Ohio State",
                 "away": "Michigan", "kickoff": "2024-11-30 17:00:00+00:00"}}

    def _matched(self, hp=24, ap=20):
        return {("e1", 2024): {"game": _game(homePoints=hp, awayPoints=ap),
                               "swapped": False}}

    def test_spread_cover(self):
        # stored line is |spread|: home -3.5 (favored, p=0.6), wins by 4 -> cover
        out = mo.analyze(self._selected(3.5, 0.6), self._matched(24, 20), [2024])
        self.assertEqual(out["n_played"], 1)
        self.assertEqual(out["by_window"]["early"]["rate"], 1.0)

    def test_spread_push(self):
        # home -4, wins by exactly 4 -> push
        out = mo.analyze(self._selected(4.0, 0.6), self._matched(24, 20), [2024])
        self.assertEqual(out["n_pushes"], 1)
        self.assertEqual(out["n_played"], 0)

    def test_spread_sign_recovery_favorite_no_cover(self):
        # Amendment A3: home -7 (p=0.65), wins by only 3 -> no cover
        out = mo.analyze(self._selected(7.0, 0.65), self._matched(27, 24),
                         [2024])
        self.assertEqual(out["by_window"]["early"]["rate"], 0.0)

    def test_spread_sign_recovery_underdog_cover(self):
        # Amendment A3: home +7 underdog (p=0.35), loses by 3 -> covers
        out = mo.analyze(self._selected(7.0, 0.35), self._matched(20, 23),
                         [2024])
        self.assertEqual(out["by_window"]["early"]["rate"], 1.0)

    def test_spread_sign_recovery_underdog_no_cover(self):
        # Amendment A3: home +7 underdog (p=0.35), loses by 10 -> no cover
        out = mo.analyze(self._selected(7.0, 0.35), self._matched(17, 27),
                         [2024])
        self.assertEqual(out["by_window"]["early"]["rate"], 0.0)

    def test_total_over(self):
        out = mo.analyze(self._selected(40.5, 0.55, "FULL_GAME_TOTAL"),
                         self._matched(24, 20), [2024])
        self.assertEqual(out["by_window"]["early"]["rate"], 1.0)

    def test_total_push(self):
        out = mo.analyze(self._selected(44.0, 0.5, "FULL_GAME_TOTAL"),
                         self._matched(24, 20), [2024])
        self.assertEqual(out["n_pushes"], 1)

    def test_matchup_and_conf_splits(self):
        out = mo.analyze(self._selected(3.5, 0.6), self._matched(), [2024])
        self.assertIn("FBSvFBS", out["by_matchup"])
        self.assertIn("P4", out["by_conference"])

    def test_fbs_fcs_class(self):
        sel = self._selected(21.5, 0.7)
        m = {("e1", 2024): {"game": _game(homeClassification="fbs",
                                         awayClassification="fcs",
                                         awayConference="Missouri Valley",
                                         homePoints=45, awayPoints=10),
                            "swapped": False}}
        out = mo.analyze(sel, m, [2024])
        self.assertIn("FBSvFCS", out["by_matchup"])
        # conf_group uses the home team: FBS Big Ten home -> P4
        self.assertIn("P4", out["by_conference"])

    def test_fcs_v_fcs_is_other(self):
        self.assertEqual(mo.matchup_class(_game(homeClassification="fcs",
                                               awayClassification="fcs")),
                        "other/unknown")

    def test_fbs_v_d2_is_other(self):
        self.assertEqual(mo.matchup_class(_game(homeClassification="fbs",
                                               awayClassification="ii")),
                        "other/unknown")


class TestIdentityAmendmentA2(unittest.TestCase):
    """Prereg amendment A2: alias table, &-handling, swapped orientation."""

    def _idx(self, games):
        idx = {}
        for g in games:
            key = (g["season"], mo.norm_name(g["homeTeam"]),
                   mo.norm_name(g["awayTeam"]))
            idx.setdefault(key, []).append(g)
        return idx

    def test_ampersand_becomes_and(self):
        self.assertEqual(mo.norm_name("William & Mary"), "williamandmary")

    def test_alias_table_spot_checks(self):
        # (provider home, provider away, CFBD home, CFBD away)
        pairs = [
            ("Appalachian State Mountaineers", "Troy Trojans",
             "App State", "Troy"),
            ("Tulane Green Wave", "UMass Minutemen",
             "Tulane", "Massachusetts"),
            ("Baylor Bears", "Albany", "Baylor", "UAlbany"),
            ("Mercer Bears", "Citadel Bulldogs", "Mercer", "The Citadel"),
            ("Louisiana Ragin Cajuns", "Southeastern Louisiana Lions",
             "Louisiana", "SE Louisiana"),
            ("Texas State Bobcats", "Houston Baptist Huskies",
             "Texas State", "Houston Christian"),
            ("Kentucky Wildcats", "Youngstown St Penguins",
             "Kentucky", "Youngstown State"),
            ("Appalachian State Mountaineers",
             "Southern Mississippi Golden Eagles",
             "App State", "Southern Miss"),
            ("Old Dominion Monarchs", "Texas A&M-Commerce Lions",
             "Old Dominion", "East Texas A&M"),
            ("Baylor Bears", "LIU Sharks",
             "Baylor", "Long Island University"),
            ("Kent State Golden Flashes", "St. Francis (PA) Red Flash",
             "Kent State", "Saint Francis"),
            ("Charlotte 49ers", "William and Mary Tribe",
             "Charlotte", "William & Mary"),
        ]
        total_matched = 0
        for ph, pa, ch, ca in pairs:
            g = _game(season=2022, homeTeam=ch, awayTeam=ca,
                      startDate="2022-09-03T23:00:00.000Z")
            idx = self._idx([g])
            ko = mo.parse_ts("2022-09-03T23:00:00+00:00")
            events = {("e1", 2022): (ph, pa, ko)}
            matched, integ = mo.match_events(events, idx)
            self.assertIn(("e1", 2022), matched,
                          f"alias failed for {ph} vs {pa}")
            self.assertFalse(matched[("e1", 2022)]["swapped"])
            total_matched += integ["matched"]
        self.assertEqual(total_matched, len(pairs))

    def test_swapped_orientation_realigns_points(self):
        # Provider lists LSU as home; CFBD lists Florida State as home
        # (neutral site). CFBD: FSU 24, LSU 23.
        g = _game(season=2022, homeTeam="Florida State", awayTeam="LSU",
                  startDate="2022-09-04T23:30:00.000Z",
                  homePoints=24, awayPoints=23)
        idx = self._idx([g])
        ko = mo.parse_ts("2022-09-04T23:30:00+00:00")
        events = {("e1", 2022): ("LSU Tigers", "Florida State Seminoles", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertIn(("e1", 2022), matched)
        self.assertTrue(matched[("e1", 2022)]["swapped"])
        self.assertEqual(integ["swapped_matched"], 1)
        # Spread on provider home (LSU) -2.5, stored as |2.5|: LSU margin =
        # 23-24 = -1, diff = -1 + -2.5 < 0 -> no cover (outcome 0).
        sel = {("e1", 2022, 1, "early", "draftkings", "FULL_GAME_SPREAD"):
               {"line": 2.5, "fair_prob": 0.6, "home": "LSU Tigers",
                "away": "Florida State Seminoles",
                "kickoff": "2022-09-04 23:30:00+00:00"}}
        out = mo.analyze(sel, matched, [2022])
        self.assertEqual(out["n_played"], 1)
        self.assertEqual(out["by_window"]["early"]["rate"], 0.0)
        # Totals are orientation-invariant: 24+23=47 over 44.5
        sel_t = {("e1", 2022, 1, "early", "draftkings", "FULL_GAME_TOTAL"):
                 {"line": 44.5, "fair_prob": 0.55, "home": "LSU Tigers",
                  "away": "Florida State Seminoles",
                  "kickoff": "2022-09-04 23:30:00+00:00"}}
        out_t = mo.analyze(sel_t, matched, [2022])
        self.assertEqual(out_t["by_window"]["early"]["rate"], 1.0)

    def test_swapped_uses_provider_home_conference(self):
        g = _game(season=2022, homeTeam="Florida State", awayTeam="LSU",
                  startDate="2022-09-04T23:30:00.000Z",
                  homeConference="ACC", awayConference="SEC")
        idx = self._idx([g])
        ko = mo.parse_ts("2022-09-04T23:30:00+00:00")
        events = {("e1", 2022): ("LSU Tigers", "Florida State Seminoles", ko)}
        matched, _ = mo.match_events(events, idx)
        sel = {("e1", 2022, 1, "early", "draftkings", "FULL_GAME_SPREAD"):
               {"line": 2.5, "fair_prob": 0.6, "home": "LSU Tigers",
                "away": "Florida State Seminoles",
                "kickoff": "2022-09-04 23:30:00+00:00"}}
        out = mo.analyze(sel, matched, [2022])
        # provider home is LSU (SEC) -> P4, not ACC
        self.assertIn("P4", out["by_conference"])

    def test_paired_efficiency_keys_on_event_and_season(self):
        # Same provider event id reused across seasons must not pair
        # early-2023 with late-2024.
        def sel(season, window):
            return {(f"e1", season, 1, window, "draftkings",
                     "FULL_GAME_SPREAD"):
                    {"line": 3.5, "fair_prob": 0.6, "home": "Ohio State",
                     "away": "Michigan",
                     "kickoff": f"{season}-11-30 17:00:00+00:00"}}
        selected = {}
        selected.update(sel(2023, "early"))
        selected.update(sel(2024, "late"))
        matched = {
            ("e1", 2023): {"game": _game(season=2023, homePoints=24,
                                        awayPoints=20), "swapped": False},
            ("e1", 2024): {"game": _game(season=2024, homePoints=24,
                                        awayPoints=20), "swapped": False},
        }
        out = mo.analyze(selected, matched, [2023, 2024])
        eff = out["closing_efficiency"]["FULL_GAME_SPREAD"]
        # (e1,2023) has early only, (e1,2024) has late only -> no pairs
        self.assertEqual(eff["paired_common_events"], 0)

    def test_unmatched_reason_phantom(self):
        idx = self._idx([_game(season=2022, homeTeam="Ohio State",
                               awayTeam="Michigan")])
        ko = mo.parse_ts("2022-09-01T23:30:00+00:00")
        events = {("e1", 2022): ("Wake Forest Demon Deacons",
                                 "Virginia Cavaliers", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertNotIn(("e1", 2022), matched)
        self.assertEqual(integ["unmatched_reasons"].get("phantom"), 1)

    def test_unmatched_reason_kickoff_miss(self):
        # Teams match but the game moved beyond +-36h (postponed).
        g = _game(season=2022, homeTeam="UCF", awayTeam="SMU",
                  startDate="2022-10-05T23:00:00.000Z")
        idx = self._idx([g])
        ko = mo.parse_ts("2022-10-02T17:00:00+00:00")  # 78h earlier
        events = {("e1", 2022): ("UCF Knights", "SMU Mustangs", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertNotIn(("e1", 2022), matched)
        self.assertEqual(integ["unmatched_reasons"].get("kickoff_miss"), 1)

    def test_unmatched_reason_team_mislabel(self):
        # Kickoff close, one team matches, other is a different school.
        g = _game(season=2023, homeTeam="UCLA",
                  awayTeam="North Carolina Central",
                  startDate="2023-09-16T21:00:00.000Z")
        idx = self._idx([g])
        ko = mo.parse_ts("2023-09-16T21:00:00+00:00")
        events = {("e1", 2023): ("UCLA Bruins", "North Carolina Tar Heels",
                                 ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertNotIn(("e1", 2023), matched)
        self.assertEqual(integ["unmatched_reasons"].get("team_mislabel"), 1)

    def test_longest_prefix_wins_no_false_match(self):
        # Provider's phantom Georgia-vs-Texas A&M championship listing must
        # NOT match CFBD's real Texas-vs-Georgia game: 'texasam...' resolves
        # to 'texasam' (Texas A&M), never to the shorter 'texas'.
        g = _game(season=2024, homeTeam="Texas", awayTeam="Georgia",
                  startDate="2024-12-07T21:00:00.000Z",
                  homePoints=19, awayPoints=22)
        # Texas A&M must exist in the school universe for longest-prefix
        # to prefer it over Texas.
        other = _game(season=2024, homeTeam="Texas A&M",
                      awayTeam="South Carolina",
                      startDate="2024-11-02T23:30:00.000Z")
        idx = self._idx([g, other])
        ko = mo.parse_ts("2024-12-07T21:00:00+00:00")
        events = {("e1", 2024): ("Georgia Bulldogs", "Texas A&M Aggies", ko)}
        matched, integ = mo.match_events(events, idx)
        self.assertNotIn(("e1", 2024), matched)
        self.assertEqual(integ["unmatched_reasons"].get("team_mislabel"), 1)

    def test_albany_state_not_shadowed_by_ualbany_alias(self):
        # 'albanystate' shadows 'albany' in the alias table: Albany State
        # (DII) must resolve to itself, not to UAlbany.
        g = _game(season=2022, homeTeam="Albany State",
                  awayTeam="Mississippi College",
                  startDate="2022-09-03T23:00:00.000Z")
        idx = self._idx([g])
        ko = mo.parse_ts("2022-09-03T23:00:00+00:00")
        events = {("e1", 2022): ("Albany State Golden Rams",
                                 "Mississippi College Choctaws", ko)}
        matched, _ = mo.match_events(events, idx)
        self.assertIn(("e1", 2022), matched)
        self.assertEqual(matched[("e1", 2022)]["game"]["homeTeam"],
                        "Albany State")


class TestSummarize(unittest.TestCase):
    def test_perfect(self):
        s = mo.summarize([(0.9, 1), (0.1, 0)])
        self.assertEqual(s["n"], 2)
        self.assertEqual(s["rate"], 0.5)
        self.assertLess(s["log_loss"], 0.2)

    def test_empty(self):
        self.assertEqual(mo.summarize([])["n"], 0)


if __name__ == "__main__":
    unittest.main()
