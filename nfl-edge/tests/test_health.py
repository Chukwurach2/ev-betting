"""Tests for ops/health.py: threshold logic, idle season, credit warnings."""
import datetime as dt
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "ops"))
import health  # noqa: E402

NOW = dt.datetime(2026, 9, 12, 1, 30, tzinfo=dt.timezone.utc)


def hb(minutes_ago, detail=None):
    return {"last_ok_at": NOW - dt.timedelta(minutes=minutes_ago),
            "detail": detail}


def fresh_all(extra=None):
    h = {"schedule_sync": hb(10), "collector": hb(10),
         "picks": hb(10), "settlement": hb(60)}
    if extra:
        h.update(extra)
    return h


class EvaluateTests(unittest.TestCase):
    def test_all_fresh_in_season_ok(self):
        r = health.evaluate(fresh_all(), NOW, upcoming_games=16)
        self.assertEqual(r["status"], "ok")
        self.assertTrue(r["in_season"])
        self.assertTrue(all(c["status"] == "ok"
                            for c in r["components"].values()))

    def test_stale_collector_is_down(self):
        h = fresh_all()
        h["collector"] = hb(120)
        r = health.evaluate(h, NOW, upcoming_games=16)
        self.assertEqual(r["status"], "down")
        self.assertEqual(r["components"]["collector"]["status"], "stale")

    def test_stale_settlement_is_degraded_not_down(self):
        h = fresh_all()
        h["settlement"] = hb(60 * 40)
        r = health.evaluate(h, NOW, upcoming_games=16)
        self.assertEqual(r["status"], "degraded")

    def test_missing_core_component_is_down(self):
        h = fresh_all()
        del h["picks"]
        r = health.evaluate(h, NOW, upcoming_games=5)
        self.assertEqual(r["status"], "down")
        self.assertEqual(r["components"]["picks"]["status"], "missing")

    def test_offseason_reports_idle_not_down(self):
        r = health.evaluate({}, NOW, upcoming_games=0)
        self.assertEqual(r["status"], "ok")
        self.assertFalse(r["in_season"])
        self.assertTrue(all(c["status"] == "idle"
                            for c in r["components"].values()))

    def test_low_credits_degraded(self):
        h = fresh_all({"collector": hb(10, {"credits_remaining": 12})})
        r = health.evaluate(h, NOW, upcoming_games=16)
        self.assertEqual(r["status"], "degraded")
        self.assertEqual(r["components"]["odds_credits"]["status"], "low")

    def test_healthy_credits_no_flag(self):
        h = fresh_all({"collector": hb(10, {"credits_remaining": 400})})
        r = health.evaluate(h, NOW, upcoming_games=16)
        self.assertEqual(r["status"], "ok")
        self.assertNotIn("odds_credits", r["components"])

    def test_unknown_credits_no_flag(self):
        r = health.evaluate(fresh_all(), NOW, upcoming_games=16)
        self.assertEqual(r["status"], "ok")


if __name__ == "__main__":
    unittest.main()
