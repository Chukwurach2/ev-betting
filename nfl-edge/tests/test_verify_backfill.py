"""Tests for ops/verify_backfill.py raw-payload forensics.

Runs the verifier against a fake psycopg connection:

  * a provider single-sided raw group (normalize correctly stores nothing)
    must be a WARNING, not an error -- the run still passes;
  * a stored group that does not pair while the raw payload had both
    sides (a normalization bug) must be a hard ERROR.
"""
import contextlib
import io
import json
import os
import pathlib
import sys
import types
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import verify_backfill as vb

REQ = "2024-09-04T12:00:00+00:00"
RET = "2024-09-04T11:55:38+00:00"  # 262s drift, within tolerance


def _envelope(outcomes):
    return {
        "timestamp": RET,
        "previous_timestamp": "2024-09-03T12:00:00Z",
        "next_timestamp": "2024-09-05T12:00:00Z",
        "data": [
            {"id": "e1", "home_team": "Baltimore Ravens",
             "away_team": "Kansas City Chiefs",
             "commence_time": "2024-09-08T17:00:00Z",
             "bookmakers": [
                 {"key": "draftkings", "title": "DraftKings",
                  "markets": [{"key": "spreads", "outcomes": outcomes}]}]},
        ],
    }


def _run(payload, quote_rows):
    snap_row = ("2024-09-04T12:00:00+00:00", RET, REQ, "us,eu",
                "spreads,totals", ["draftkings", "pinnacle"], 40, payload)
    fake = types.ModuleType("psycopg")

    class FakeConn:
        def __init__(self):
            self.autocommit = False

        def execute(self, sql, params=()):
            class Cur:
                def fetchall(inner):
                    if "nfl_edge_market_history" in sql:
                        return [snap_row]
                    return quote_rows
            return Cur()

        def close(self):
            pass

    fake.connect = lambda *a, **k: FakeConn()
    sys.modules["psycopg"] = fake
    os.environ["NFL_EDGE_DATABASE_URL"] = "postgresql://fake/fake"
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            rc = vb.main([])
    finally:
        sys.modules.pop("psycopg", None)
    report, _ = json.JSONDecoder().raw_decode(buf.getvalue())
    return rc, report


class VerifierForensicsTests(unittest.TestCase):
    def test_provider_single_side_is_warning_not_error(self):
        payload = _envelope([
            {"name": "Baltimore Ravens", "price": -110, "point": -3.0},
        ])
        # normalize correctly stored nothing for the single-sided group;
        # one clean pinnacle pair elsewhere so the run is otherwise healthy.
        quotes = [
            ("e2", "pinnacle", "FULL_GAME_SPREAD", 2.5, RET, 0.5, False, -110),
            ("e2", "pinnacle", "FULL_GAME_SPREAD", 2.5, RET, 0.5, False, -110),
        ]
        rc, report = _run(payload, quotes)
        entry = report["snapshots"][0]
        self.assertEqual(rc, 0, report["errors"])
        self.assertEqual(entry["raw_single_sided_groups"], 1)
        self.assertEqual(entry["bad_pair_groups"], 0)
        self.assertTrue(any("single provider side" in w
                            for w in report["warnings"]))
        self.assertEqual(entry["payload_format"], "full-envelope")

    def test_stored_mispair_with_two_raw_sides_is_error(self):
        payload = _envelope([
            {"name": "Baltimore Ravens", "price": -110, "point": -3.0},
            {"name": "Kansas City Chiefs", "price": -110, "point": 3.0},
        ])
        # Bug simulation: raw had both sides, but only one quote stored.
        quotes = [
            ("e1", "draftkings", "FULL_GAME_SPREAD", 3.0, RET, 0.5, True, -110),
        ]
        rc, report = _run(payload, quotes)
        entry = report["snapshots"][0]
        self.assertEqual(rc, 1)
        self.assertEqual(entry["bad_pair_groups"], 1)
        self.assertEqual(entry["raw_single_sided_groups"], 0)
        self.assertTrue(any("mis-paired" in e for e in report["errors"]))

    def test_kickoff_range_reported(self):
        payload = _envelope([
            {"name": "Baltimore Ravens", "price": -110, "point": -3.0},
            {"name": "Kansas City Chiefs", "price": -110, "point": 3.0},
        ])
        quotes = [
            ("e1", "draftkings", "FULL_GAME_SPREAD", 3.0, RET, 0.5, True, -110),
            ("e1", "draftkings", "FULL_GAME_SPREAD", 3.0, RET, 0.5, True, -110),
        ]
        rc, report = _run(payload, quotes)
        entry = report["snapshots"][0]
        self.assertEqual(rc, 0, report["errors"])
        self.assertEqual(entry["kickoff_min"], "2024-09-08T17:00:00+00:00")
        self.assertEqual(entry["kickoff_max"], "2024-09-08T17:00:00+00:00")
        self.assertEqual(entry["events_with_quotes"], 1)


if __name__ == "__main__":
    unittest.main()
