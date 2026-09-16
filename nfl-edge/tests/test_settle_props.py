"""Unit tests for the free prop settlement job (ops/settle_props.py).

Pure-function tests only: name normalization, market mapping, settlement
arithmetic, checkpoint preference order, canonical id parsing, and the
idempotent insert statement shape. No network, no DB.
"""

import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ops"))
import settle_props as sp


class TestNormalizeName(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(sp.normalize_name("Jared Goff"), "jared goff")

    def test_abbreviated_matches_display(self):
        self.assertEqual(sp.normalize_name("J.Goff"), "jgoff")

    def test_suffixes_stripped(self):
        self.assertEqual(sp.normalize_name("Michael Thomas Jr."), "michael thomas")
        self.assertEqual(sp.normalize_name("Robert Griffin III"), "robert griffin")

    def test_punctuation(self):
        self.assertEqual(sp.normalize_name("Odell Beckham Jr."),
                         "odell beckham")


class TestMarketColumns(unittest.TestCase):
    def test_all_v1_markets_mapped(self):
        self.assertEqual(sp.MARKET_COLUMNS["player_pass_yds"], "passing_yards")
        self.assertEqual(sp.MARKET_COLUMNS["player_pass_attempts"], "attempts")
        self.assertEqual(sp.MARKET_COLUMNS["player_rush_attempts"], "carries")

    def test_no_extra_markets(self):
        self.assertEqual(set(sp.MARKET_COLUMNS),
                         {"player_pass_yds", "player_pass_attempts",
                          "player_rush_attempts"})


class TestSettlementArithmetic(unittest.TestCase):
    def test_over_is_settled(self):
        self.assertEqual(sp.settle_status(250.0, 224.5), "settled")

    def test_under_is_settled(self):
        self.assertEqual(sp.settle_status(200.0, 224.5), "settled")

    def test_exact_line_is_push(self):
        self.assertEqual(sp.settle_status(225.0, 225.0), "push")


class TestCanonicalId(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(sp.parse_canonical_game_id("2026_02_DET_BUF"),
                         (2026, 2, "DET", "BUF"))

    def test_bad_id_raises(self):
        with self.assertRaises(ValueError):
            sp.parse_canonical_game_id("not-a-game")


class TestCheckpointOrder(unittest.TestCase):
    def test_close_first(self):
        self.assertEqual(sp.CHECKPOINT_ORDER[0], "Close")

    def test_all_six_present(self):
        self.assertEqual(set(sp.CHECKPOINT_ORDER),
                         {"T-24", "T-12", "T-6", "T-3", "T-90m", "Close"})


class TestMatchPlayer(unittest.TestCase):
    def _row(self, name, team, season=2026, week=2):
        return {"player_display_name": name, "player_name": name,
                "season": str(season), "week": str(week), "team": team}

    def test_match_on_team(self):
        lookup = sp.build_lookup([self._row("Jared Goff", "DET")])
        hit = sp.match_player(lookup, 2026, 2, {"DET", "BUF"}, "jared goff")
        self.assertIsNotNone(hit)
        self.assertEqual(hit["team"], "DET")

    def test_wrong_teams_no_match(self):
        lookup = sp.build_lookup([self._row("Jared Goff", "DET")])
        self.assertIsNone(sp.match_player(lookup, 2026, 2, {"KC", "BUF"},
                                          "jared goff"))

    def test_missing_player_no_match(self):
        lookup = sp.build_lookup([self._row("Jared Goff", "DET")])
        self.assertIsNone(sp.match_player(lookup, 2026, 2, {"DET", "BUF"},
                                          "josh allen"))

    def test_ambiguous_duplicate_teams_no_match(self):
        # Two rows, same name, both teams in game -> ambiguous, no settle.
        rows = [self._row("Alex Smith", "DET"), self._row("Alex Smith", "BUF")]
        lookup = sp.build_lookup(rows)
        self.assertIsNone(sp.match_player(lookup, 2026, 2, {"DET", "BUF"},
                                          "alex smith"))


class TestInsertIdempotent(unittest.TestCase):
    def test_on_conflict_do_nothing(self):
        src = Path(__file__).resolve().parent.parent / "ops" / "settle_props.py"
        text = src.read_text()
        self.assertIn("ON CONFLICT DO NOTHING", text)

    def test_dry_run_default(self):
        src = Path(__file__).resolve().parent.parent / "ops" / "settle_props.py"
        text = src.read_text()
        self.assertIn('"--dry-run"', text)
        # live inserts are gated behind an explicit flag
        self.assertIn('"--live"', text)


if __name__ == "__main__":
    unittest.main()
