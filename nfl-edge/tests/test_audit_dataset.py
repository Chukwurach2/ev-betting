"""Tests for ops/audit_dataset.py (pure logic + fake-DB run_audit paths)."""
import datetime as dt
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import audit_dataset as ad

REQ1 = "2024-09-04T12:00:00+00:00"
REQ2 = "2024-09-07T12:00:00+00:00"
REQ3 = "2024-09-08T15:30:00+00:00"
RET = "2024-09-04T11:55:38+00:00"


def _snap_row(req):
    # returned provider time: 262s before the requested instant (like prod)
    ret = (dt.datetime.fromisoformat(req) -
           dt.timedelta(seconds=262)).isoformat()
    return (req, ret, req, "us,eu", "spreads,totals",
            ["draftkings", "pinnacle"], 40)


class FakeConn:
    """Canned results dispatched on SQL substrings."""

    def __init__(self, snaps):
        self.snaps = snaps
        self.autocommit = False

    def execute(self, sql, params=()):
        snaps = self.snaps

        class Cur:
            def fetchall(inner):
                if "nfl_edge_market_history" in sql:
                    return snaps
                if "GROUP BY market" in sql:
                    return [("FULL_GAME_SPREAD", 100, -7.0, 7.0, 0.5,
                             -150, 130)]
                if "GROUP BY book_key" in sql:
                    return [(b, 10, 3, 1) for b in
                            ["pinnacle", "draftkings", "fanduel", "betmgm",
                             "betrivers", "williamhill_us", "fanatics",
                             "espnbet"]]
                return []

            def fetchone(inner):
                if "COUNT(*)" in sql:
                    if sql.strip().endswith("nfl_edge_historical_quotes"):
                        return (100,)
                    return (0,)
                return None

            def __iter__(inner):
                return iter(inner.fetchall())

        return Cur()

    def close(self):
        pass


def _run(snaps):
    return ad.run_audit(FakeConn(snaps), [2024], [1], "us,eu",
                        "spreads,totals")


class AuditLogicTests(unittest.TestCase):
    def test_expected_plan_count_and_first_instant(self):
        plan = ad.build_expected([2022, 2023, 2024], list(range(1, 19)))
        self.assertEqual(len(plan), 162)
        first = [p for p in plan
                 if p["season"] == 2024 and p["week"] == 1]
        self.assertEqual(
            sorted(p["requested_at"].isoformat() for p in first),
            ["2024-09-04T12:00:00+00:00",
             "2024-09-07T12:00:00+00:00",
             "2024-09-08T15:30:00+00:00"])

    def test_percentiles(self):
        self.assertEqual(ad._pct([1, 2, 3, 4, 5], 50), 3)
        self.assertEqual(ad._pct([], 90), None)

    def test_full_coverage_passes(self):
        rep = _run([_snap_row(REQ1), _snap_row(REQ2), _snap_row(REQ3)])
        self.assertEqual(rep["status"], "pass", rep["errors"])
        self.assertEqual(rep["coverage"]["received_snapshots"], 3)
        self.assertEqual(rep["coverage"]["missing_snapshots"], [])
        self.assertEqual(rep["quotes"]["total"], 100)

    def test_missing_snapshot_fails(self):
        rep = _run([_snap_row(REQ1), _snap_row(REQ3)])
        self.assertEqual(rep["status"], "fail")
        self.assertEqual(len(rep["coverage"]["missing_snapshots"]), 1)
        self.assertTrue(any("missing" in e for e in rep["errors"]))

    def test_absent_named_book_is_warning(self):
        class NoFanduel(FakeConn):
            def execute(self, sql, params=()):
                cur = super().execute(sql, params)
                orig = cur.fetchall

                def patched():
                    rows = orig()
                    if "GROUP BY book_key" in sql:
                        rows = [r for r in rows if r[0] != "fanduel"]
                    return rows
                cur.fetchall = patched
                return cur

        rep = ad.run_audit(NoFanduel(
            [_snap_row(REQ1), _snap_row(REQ2), _snap_row(REQ3)]),
            [2024], [1], "us,eu", "spreads,totals")
        self.assertEqual(rep["status"], "pass", rep["errors"])
        self.assertTrue(any("fanduel" in w for w in rep["warnings"]))
        self.assertIsNone(rep["books"]["named_books"]["fanduel"])

    def test_markdown_renders(self):
        rep = _run([_snap_row(REQ1), _snap_row(REQ2), _snap_row(REQ3)])
        md = ad.render_markdown(rep)
        self.assertIn("Status: PASS", md)
        self.assertIn("pinnacle: 10 quotes", md)


if __name__ == "__main__":
    unittest.main()
