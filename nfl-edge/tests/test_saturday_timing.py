"""Tests for the one-time Saturday opportunity-lifetime experiment.

Covers the 2026-09-15 audit defects:
  1. Date gate restricts collection to 2026-09-19 (ET).
  2. Exactly 48 observation slots (workflow cron arithmetic).
  3. Per-run cost is per event (measured 1 credit/event, totals-only/us).
  4. Quota accounting spans the complete run (before-first vs after-last).
  5. Hard credit caps (per-run, per-day) enforced before any paid call.
  6. Explicit per-book snapshot states against a mechanical universe.
  7. The analyzer reads the dedicated timing tables (not the live table).
  8. Interval-censored lifetime bounds, never inferred minute precision.

Also guards the frozen v1.0 rule: the analyzer must import the rule's
constants from ops.pressure_shadow, never redefine them.
"""
import pathlib
import re
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

OPS = pathlib.Path(__file__).parents[1] / "ops"
sys.path.insert(0, str(OPS))

import saturday_timing as st  # noqa: E402
import opportunity_lifetime as ol  # noqa: E402
import pressure_shadow as frozen  # noqa: E402


def utc(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


class GateTests(unittest.TestCase):
    def test_experiment_date_allowed_inside_window(self):
        ok, status, slot, _ = st.gate(utc("2026-09-19T14:00:00Z"))
        self.assertTrue(ok)
        self.assertEqual(status, "ok")
        self.assertEqual(slot, utc("2026-09-19T14:00:00Z"))

    def test_last_slot_2145_et_allowed(self):
        ok, status, _, _ = st.gate(utc("2026-09-20T01:45:00Z"))
        self.assertTrue(ok, status)

    def test_2200_et_excluded(self):
        ok, status, _, _ = st.gate(utc("2026-09-20T02:00:00Z"))
        self.assertFalse(ok)
        self.assertEqual(status, "window_skip")

    def test_before_1000_et_excluded(self):
        ok, status, _, _ = st.gate(utc("2026-09-19T13:59:00Z"))
        self.assertFalse(ok)
        self.assertEqual(status, "window_skip")

    def test_other_saturdays_rejected(self):
        for iso in ("2026-09-26T14:00:00Z", "2026-09-12T14:00:00Z",
                    "2026-10-03T14:00:00Z"):
            ok, status, _, _ = st.gate(utc(iso))
            self.assertFalse(ok, iso)
            self.assertEqual(status, "date_gate_skip", iso)

    def test_gate_fires_before_any_api_call(self):
        # run_collection with a rejected date must not call fetch at all.
        calls = []

        def boom(*a, **k):
            calls.append(1)
            raise AssertionError("provider called despite gate")

        res = st.run_collection(fetch_events_fn=boom, fetch_odds_fn=boom,
                                now_utc=utc("2026-09-26T14:00:00Z"))
        self.assertEqual(res["status"], "date_gate_skip")
        self.assertEqual(calls, [])


class CapTests(unittest.TestCase):
    def _events(self, n):
        return [{"id": f"e{i:03d}", "commence_time": "2026-09-19T16:00:00Z",
                 "home_team": "X", "away_team": "Y"} for i in range(n)]

    def test_cap_truncation_is_deterministic(self):
        evs = self._events(100)
        kept, capped = st.cap_event_list(evs, 78)
        self.assertEqual(len(kept), 78)
        self.assertEqual(capped, 22)
        self.assertEqual(kept[0]["id"], "e000")

    def test_per_run_cap_enforced_before_paid_calls(self):
        evs = self._events(200)
        paid = []

        def fake_events(sport="ncaaf"):
            return evs, {"x-requests-remaining": "19000",
                         "x-requests-used": "100", "x-requests-last": "0"}

        def fake_odds(event_id, markets, regions="us", sport="ncaaf"):
            paid.append(event_id)
            return {"id": event_id, "bookmakers": []}, \
                {"x-requests-remaining": "19000",
                 "x-requests-used": "101", "x-requests-last": "1"}

        with patch.dict("os.environ", {"TIMING_MAX_CREDITS_PER_RUN": "10"}):
            res = st.run_collection(
                fetch_events_fn=fake_events, fetch_odds_fn=fake_odds,
                now_utc=utc("2026-09-19T14:00:00Z"),
                day_spend_fn=lambda: 0)
        # 10-credit cap minus margin 2 -> at most 8 paid calls.
        self.assertLessEqual(len(paid), 8)
        self.assertEqual(res["n_events_capped"], 200 - 8)
        self.assertEqual(res["status"], "ok")

    def test_day_cap_enforced_before_paid_calls(self):
        evs = self._events(70)
        paid = []

        def fake_events(sport="ncaaf"):
            return evs, {"x-requests-remaining": "19000",
                         "x-requests-used": "100", "x-requests-last": "0"}

        def fake_odds(event_id, markets, regions="us", sport="ncaaf"):
            paid.append(event_id)
            raise AssertionError("paid call despite day cap")

        with patch.dict("os.environ", {"TIMING_MAX_CREDITS_DAY": "50"}):
            res = st.run_collection(
                fetch_events_fn=fake_events, fetch_odds_fn=fake_odds,
                now_utc=utc("2026-09-19T14:00:00Z"),
                day_spend_fn=lambda: 4999)
        self.assertEqual(res["status"], "day_cap_skip")
        self.assertEqual(paid, [])

    def test_quota_standdown_before_paid_calls(self):
        evs = self._events(70)
        paid = []

        def fake_events(sport="ncaaf"):
            return evs, {"x-requests-remaining": "1500",
                         "x-requests-used": "100", "x-requests-last": "0"}

        def fake_odds(*a, **k):
            paid.append(1)
            raise AssertionError("paid call during standdown")

        res = st.run_collection(fetch_events_fn=fake_events,
                                fetch_odds_fn=fake_odds,
                                now_utc=utc("2026-09-19T14:00:00Z"))
        self.assertEqual(res["status"], "quota_standdown")
        self.assertEqual(paid, [])

    def test_complete_run_quota_accounting(self):
        evs = self._events(3)
        used = {"n": 100}

        def fake_events(sport="ncaaf"):
            return evs, {"x-requests-remaining": "19000",
                         "x-requests-used": "100", "x-requests-last": "0"}

        def fake_odds(event_id, markets, regions="us", sport="ncaaf"):
            used["n"] += 1  # 1 credit per event (measured)
            return {"id": event_id, "bookmakers": []}, \
                {"x-requests-remaining": str(19000 - used["n"]),
                 "x-requests-used": str(used["n"]), "x-requests-last": "1"}

        res = st.run_collection(fetch_events_fn=fake_events,
                                fetch_odds_fn=fake_odds,
                                now_utc=utc("2026-09-19T14:00:00Z"),
                                day_spend_fn=lambda: 0)
        # Complete-run delta: used after last paid call minus used before
        # the first paid call (the free events call is excluded).
        self.assertEqual(res["credits_used_before"], 100)
        self.assertEqual(res["credits_used_after"], 103)
        self.assertEqual(res["credits_consumed"], 3)


class SnapshotStateTests(unittest.TestCase):
    def test_available_absent_request_failed_unmapped(self):
        uni = ["dk", "fd", "mgm"]
        rows = st.classify_event_books(
            "e1", uni, seen_books=["dk", "fd"],
            books_with_quotes={"dk"}, fetch_ok=True, mapped=True)
        by_book = {r["book_key"]: r["state"] for r in rows}
        self.assertEqual(by_book, {"dk": "available", "fd": "absent",
                                   "mgm": "absent"})
        self.assertTrue(all(r["universe_member"] for r in rows))

        rows = st.classify_event_books(
            "e1", uni, seen_books=[], books_with_quotes=set(),
            fetch_ok=False, mapped=True)
        self.assertTrue(all(r["state"] == "request_failed" for r in rows))

        rows = st.classify_event_books(
            "e1", uni, seen_books=["dk"], books_with_quotes={"dk"},
            fetch_ok=True, mapped=False)
        self.assertTrue(all(r["state"] == "unmapped" for r in rows))

    def test_non_universe_books_recorded_not_silent(self):
        rows = st.classify_event_books(
            "e1", ["dk"], seen_books=["dk", "xx"],
            books_with_quotes={"dk", "xx"}, fetch_ok=True, mapped=True)
        by_book = {r["book_key"]: r for r in rows}
        self.assertEqual(by_book["xx"]["state"], "available")
        self.assertFalse(by_book["xx"]["universe_member"])


class FrozenRuleReuseTests(unittest.TestCase):
    def test_analyzer_imports_frozen_constants(self):
        # The analyzer must not redefine the rule; it reuses the frozen
        # module's constants by reference.
        self.assertIs(ol.frozen.THRESHOLD, frozen.THRESHOLD)
        self.assertEqual(ol.frozen.THRESHOLD, 0.00126)
        self.assertEqual(ol.frozen.MIN_BOOKS, 3)
        self.assertEqual(ol.frozen.LINE_TOL, 0.01)
        self.assertEqual(ol.frozen.RULE_VERSION, "v1.0")

    def _qs(self, fps, books=("dk", "fd", "mgm")):
        qs = []
        for b, fp in zip(books, fps):
            qs.append({"book_key": b, "selection": "Over", "line": 52.5,
                       "american_odds": -110, "fair_probability": fp})
            qs.append({"book_key": b, "selection": "Under", "line": 52.5,
                       "american_odds": -110,
                       "fair_probability": 1 - fp})
        return qs

    def test_strong_signal_detected(self):
        sig = ol.snapshot_signal(self._qs([0.53, 0.53, 0.53]))
        self.assertIsNotNone(sig)
        self.assertEqual(sig["direction"], 1)
        self.assertAlmostEqual(sig["consensus_line"], 52.5)

    def test_weak_signal_rejected(self):
        self.assertIsNone(ol.snapshot_signal(self._qs([0.5005] * 3)))

    def test_fewer_than_three_books_rejected(self):
        self.assertIsNone(ol.snapshot_signal(self._qs([0.53] * 2,
                                                      books=("dk", "fd"))))

    def test_favorable_contract_picks_best_price(self):
        qs = self._qs([0.53, 0.53, 0.53])
        qs[0]["american_odds"] = -105  # dk Over best price
        sig = ol.snapshot_signal(qs)
        c = ol.favorable_contract(sig, qs)
        self.assertEqual(c["book_key"], "dk")
        self.assertEqual(c["selection"], "Over")
        self.assertEqual(c["american_odds"], -105)


class LifetimeBoundsTests(unittest.TestCase):
    def times(self, n):
        t0 = utc("2026-09-19T14:00:00Z")
        from datetime import timedelta
        return [t0 + timedelta(minutes=15 * i) for i in range(n)]

    def test_interval_censored_bounds(self):
        lo, hi = ol.lifetime_bounds(0, self.times(4),
                                    [True, True, False, False])
        self.assertEqual((lo, hi), (15.0, 30.0))

    def test_immediate_disappearance(self):
        lo, hi = ol.lifetime_bounds(0, self.times(4),
                                    [True, False, False, False])
        self.assertEqual((lo, hi), (0.0, 15.0))

    def test_right_censored(self):
        lo, hi = ol.lifetime_bounds(0, self.times(4),
                                    [True, True, True, True])
        self.assertEqual(lo, 45.0)
        self.assertIsNone(hi)

    def test_no_point_estimates_in_summary(self):
        s = ol.summarize_bounds([(15.0, 30.0), (0.0, 15.0), (45.0, None)])
        self.assertEqual(s["n_interval_censored"], 2)
        self.assertEqual(s["n_right_censored"], 1)
        self.assertEqual(s["lower_bounds_min"]["median"], 7.5)
        self.assertEqual(s["upper_bounds_min"]["median"], 22.5)


class ContractIntactTests(unittest.TestCase):
    def test_deteriorated_price_ends_run(self):
        c = {"book_key": "dk", "selection": "Over", "line": 52.5,
             "american_odds": -110}
        states = [{"book_key": "dk", "state": "available"}]
        worse = [{"book_key": "dk", "selection": "Over", "line": 52.5,
                  "american_odds": -120}]
        same = [{"book_key": "dk", "selection": "Over", "line": 52.5,
                 "american_odds": -110}]
        better = [{"book_key": "dk", "selection": "Over", "line": 52.5,
                   "american_odds": -105}]
        self.assertTrue(ol.contract_intact_at(c, same, states))
        self.assertTrue(ol.contract_intact_at(c, better, states))
        self.assertFalse(ol.contract_intact_at(c, worse, states))
        self.assertFalse(ol.contract_intact_at(
            c, same, [{"book_key": "dk", "state": "absent"}]))
        self.assertFalse(ol.contract_intact_at(
            c, [], [{"book_key": "dk", "state": "request_failed"}]))


WORKFLOW_PATH = (pathlib.Path(__file__).parents[2] / ".github" / "workflows"
                 / "saturday-timing.yml")


@unittest.skipUnless(
    WORKFLOW_PATH.exists(),
    "saturday-timing.yml not extracted in CI (bootstrap keeps nfl-edge/ only)",
)
class WorkflowScheduleTests(unittest.TestCase):
    WORKFLOW = WORKFLOW_PATH

    def _cron_runs(self):
        import yaml
        doc = yaml.safe_load(self.WORKFLOW.read_text())
        # YAML parses the `on:` key as boolean True.
        crons = doc[True]["schedule"]
        return [c["cron"] for c in crons]

    def _expand(self, cron):
        # Expand the two cron lines into UTC run times for 2026-09-19.
        runs = []
        for c in cron:
            minute, hour, _dom, _mon, dow = c.split()
            # Saturday-UTC hours run on dow 6; the 20:00-21:45 ET tail
            # falls on Sunday-UTC hours (dow 0) but is still Saturday ET.
            self.assertIn(dow, ("6", "0"), "weekend-only schedule")
            hours = []
            for part in hour.split(","):
                if "-" in part:
                    lo, hi = part.split("-")
                    hours += list(range(int(lo), int(hi) + 1))
                else:
                    hours.append(int(part))
            for h in hours:
                for m in range(0, 60, 15):
                    runs.append((h, m))
        return sorted(set(runs))

    def test_exactly_48_runs(self):
        runs = self._expand(self._cron_runs())
        self.assertEqual(len(runs), 48, runs)

    def test_window_covers_1000_to_2145_et(self):
        runs = self._expand(self._cron_runs())
        # UTC times; 2026-09-19 is EDT (UTC-4).
        ets = sorted(((h - 4) % 24, m) for h, m in runs)
        self.assertEqual(ets[0], (10, 0))
        self.assertEqual(ets[-1], (21, 45))

    def test_collector_enforces_experiment_date(self):
        src = (OPS / "saturday_timing.py").read_text()
        self.assertIn('EXPERIMENT_DATE_ET = "2026-09-19"', src)
        self.assertIn("date_gate_skip", src)

    def test_analyzer_reads_timing_tables(self):
        src = (OPS / "opportunity_lifetime.py").read_text()
        self.assertIn("ncaaf_timing_quotes", src)
        self.assertIn("ncaaf_timing_runs", src)
        self.assertIn("ncaaf_timing_book_states", src)
        # Must not read the ordinary live quotes table.
        self.assertNotIn("odds_quotes_table", src)


if __name__ == "__main__":
    unittest.main()
