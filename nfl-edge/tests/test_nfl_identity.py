"""Tests for the NFL canonical identity join (nflverse <-> Odds API).

Guards the deterministic bar:
  1. Explicit alias table covers all 32 franchises; no guessing.
  2. Unknown Odds team names are recorded as 'no_alias', never resolved.
  3. Date matching: exact UTC date, then deterministic +/-1 day fallback.
  4. Swapped home/away is a separate, labeled attempt.
  5. Ambiguous matches are preserved, never auto-resolved.
  6. Unmatched states carry explicit reasons.
  7. Deterministic: same input -> identical output.
"""
import json
import pathlib
import sys
import unittest
from datetime import datetime, timezone

OPS = pathlib.Path(__file__).parents[1] / "ops"
sys.path.insert(0, str(OPS))

import nfl_canonical_identity as nci  # noqa: E402


def ko(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


NV_GAME = {
    "game_id": "2022_01_BAL_NYJ",
    "season": 2022,
    "game_type": "REG",
    "week": 1,
    "gameday": "2022-09-11",
    "away_team": "BAL",
    "home_team": "NYJ",
    "stadium": "MetLife Stadium",
}


def ev(eid, home, away, kickoff):
    return {"event_id": eid, "home": home, "away": away, "kickoff": ko(kickoff)}


class TestAliases(unittest.TestCase):
    def test_32_explicit_aliases(self):
        aliases = nci.load_aliases()
        self.assertEqual(len(aliases), 32, "must cover all 32 franchises explicitly")

    def test_alias_values_match_bundled_nflverse(self):
        aliases = nci.load_aliases()
        with open(OPS / "nflverse_schedules_2022_2024.json") as f:
            games = json.load(f)
        observed = {g["home_team"] for g in games} | {g["away_team"] for g in games}
        for full, abbr in aliases.items():
            self.assertIn(abbr, observed,
                          f"alias value {abbr} for {full} not in bundled nflverse data")

    def test_rams_is_LA_not_LAR(self):
        aliases = nci.load_aliases()
        self.assertEqual(aliases["Los Angeles Rams"], "LA")

    def test_known_names_resolve(self):
        aliases = nci.load_aliases()
        self.assertEqual(nci.normalize_odds("Kansas City Chiefs", aliases), ("KC", True))
        self.assertEqual(nci.normalize_odds("Washington Commanders", aliases), ("WAS", True))

    def test_unknown_names_never_guessed(self):
        aliases = nci.load_aliases()
        self.assertEqual(nci.normalize_odds("Kansas City", aliases), ("", False))
        self.assertEqual(nci.normalize_odds("Chiefs", aliases), ("", False))
        self.assertEqual(nci.normalize_odds("  kansas city chiefs  ", aliases), ("", False),
                         "alias lookup is exact; no case/whitespace guessing")


class TestMatchEvents(unittest.TestCase):
    ALIASES = {"Baltimore Ravens": "BAL", "New York Jets": "NYJ",
               "Kansas City Chiefs": "KC", "Philadelphia Eagles": "PHI"}

    def test_exact_utc_date_match(self):
        # Sunday 1pm ET = 17:00 UTC, same UTC date as gameday.
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 1)
        m = r["matched"][0]
        self.assertEqual(m["nflverse_game_id"], "2022_01_BAL_NYJ")
        self.assertEqual(m["match_type"], "alias_date")
        self.assertEqual(m["date_shift"], 0)

    def test_date_shift_match_sunday_night(self):
        # Sunday night game lands on Monday UTC; nflverse gameday is Sunday.
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-12T00:20:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 1)
        m = r["matched"][0]
        self.assertEqual(m["match_type"], "alias_date_shift")
        # Odds UTC date (Mon 9/12) is one day AFTER nflverse gameday (Sun 9/11)
        self.assertEqual(m["date_shift"], -1)
        self.assertEqual(m["gameday"], "2022-09-11")

    def test_swapped_home_away(self):
        g = dict(NV_GAME, home_team="BAL", away_team="NYJ")
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z")],
            [g], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 1)
        self.assertEqual(r["matched"][0]["match_type"], "swapped")

    def test_ambiguous_preserved_never_resolved(self):
        g1 = dict(NV_GAME)
        g2 = dict(NV_GAME, game_id="2022_01_BAL_NYJ_X")
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z")],
            [g1, g2], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 0)
        self.assertEqual(len(r["ambiguous"]), 1)
        self.assertEqual(len(r["ambiguous"][0]["candidates"]), 2)

    def test_no_alias_reason(self):
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore", "2022-09-11T17:00:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 0)
        self.assertEqual(len(r["unmatched_odds"]), 1)
        u = r["unmatched_odds"][0]
        self.assertEqual(u["reason"], "no_alias")
        self.assertIn("Baltimore", u["missing_aliases"])

    def test_outside_seasons_reason(self):
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2025-09-11T17:00:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["unmatched_odds"]), 1)
        self.assertEqual(r["unmatched_odds"][0]["reason"], "outside_seasons")

    def test_no_match_reason(self):
        # Teams resolve but no game on that date.
        r = nci.match_events(
            [ev("e1", "Philadelphia Eagles", "Kansas City Chiefs", "2022-10-02T17:00:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(r["unmatched_odds"][0]["reason"], "no_match")

    def test_unmatched_nflverse_preserved(self):
        r = nci.match_events([], [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["unmatched_nflverse"]), 1)
        self.assertEqual(r["unmatched_nflverse"][0]["nflverse_game_id"], "2022_01_BAL_NYJ")

    def test_deterministic(self):
        events = [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z"),
                  ev("e2", "New York Jets", "Baltimore Ravens", "2022-09-12T00:20:00Z")]
        r1 = nci.match_events(events, [NV_GAME], self.ALIASES, [2022])
        r2 = nci.match_events(events, [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(json.dumps(r1, sort_keys=True, default=str),
                         json.dumps(r2, sort_keys=True, default=str))

    def test_missing_from_source_registry_identifies_2022_week17(self):
        # The suspended Bills @ Bengals game is absent from the bundle by
        # construction; it must appear explicitly, never silently drop out.
        r = nci.match_events([], [], self.ALIASES, [2022, 2023, 2024])
        self.assertEqual(len(r["missing_from_source"]), 1)
        m = r["missing_from_source"][0]
        self.assertEqual(m["state"], "missing_from_source")
        self.assertEqual(m["season"], 2022)
        self.assertEqual(m["week"], 17)
        self.assertEqual(m["game_type"], "REG")
        self.assertEqual(m["home"], "CIN")
        self.assertEqual(m["away"], "BUF")
        self.assertEqual(m["gameday"], "2023-01-03")
        self.assertIsNone(m["nflverse_game_id"])
        self.assertTrue(m["reason"], "must carry an explicit reason")

    def test_missing_from_source_emitted_with_matches(self):
        # The registry is emitted regardless of match outcomes.
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z")],
            [NV_GAME], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 1)
        self.assertEqual(len(r["missing_from_source"]), 1)
        self.assertEqual(r["missing_from_source"][0]["state"], "missing_from_source")

    def test_missing_from_source_registry_is_a_copy(self):
        # Mutating the emitted list must not corrupt the module registry.
        r = nci.match_events([], [], self.ALIASES, [2022])
        r["missing_from_source"][0]["home"] = "XXX"
        r2 = nci.match_events([], [], self.ALIASES, [2022])
        self.assertEqual(r2["missing_from_source"][0]["home"], "CIN")

    def test_exact_date_preferred_over_fallback(self):
        # Both exact and shifted candidates exist: exact must win.
        g_exact = dict(NV_GAME)
        g_shift = dict(NV_GAME, game_id="2022_01_BAL_NYJ_S",
                       gameday="2022-09-12")
        r = nci.match_events(
            [ev("e1", "New York Jets", "Baltimore Ravens", "2022-09-11T17:00:00Z")],
            [g_exact, g_shift], self.ALIASES, [2022])
        self.assertEqual(len(r["matched"]), 1)
        self.assertEqual(r["matched"][0]["match_type"], "alias_date")


if __name__ == "__main__":
    unittest.main()
