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


def _events(n, eid_prefix="e"):
    return [{"id": f"{eid_prefix}{i:03d}",
             "commence_time": "2026-09-19T16:00:00Z",
             "home_team": "X", "away_team": "Y"} for i in range(n)]


def _headers(used, last="0"):
    return {"x-requests-remaining": str(20000 - used),
            "x-requests-used": str(used), "x-requests-last": last}


def _bulk_event(eid, books_quotes, commence="2026-09-19T16:00:00Z"):
    """Fabricate one bulk /odds event. books_quotes maps book_key ->
    (over_odds, under_odds, line)."""
    bookmakers = []
    for b, (oo, uo, line) in books_quotes.items():
        bookmakers.append({
            "key": b, "title": b,
            "markets": [{
                "key": "totals",
                "last_update": "2026-09-19T13:55:00Z",
                "outcomes": [
                    {"name": "Over", "price": oo, "point": line},
                    {"name": "Under", "price": uo, "point": line},
                ],
            }],
        })
    return {"id": eid, "commence_time": commence,
            "home_team": "Team A", "away_team": "Team B",
            "bookmakers": bookmakers}


def _bulk_run(evs, bulk, paid, used0=100, fail_times=0):
    """Run one tick against fabricated bulk data.

    Returns (result, paid_call_count). fail_times: the bulk fetch raises
    this many times before succeeding (0 = success first try).
    """
    state = {"fails_left": fail_times}

    def fake_events(sport="ncaaf"):
        return evs, _headers(used0)

    def fake_odds(markets, regions="us", sport="ncaaf"):
        paid.append((markets, regions, sport))
        if state["fails_left"] > 0:
            state["fails_left"] -= 1
            raise RuntimeError("Odds provider request failed")
        return bulk, _headers(used0 + 1, last="1")

    res = st.run_collection(fetch_events_fn=fake_events,
                            fetch_odds_fn=fake_odds,
                            now_utc=utc("2026-09-19T14:00:00Z"),
                            day_spend_fn=lambda: 0)
    return res, paid


class CapTests(unittest.TestCase):
    def test_exactly_one_paid_call_per_tick(self):
        evs = _events(70)
        bulk = [_bulk_event(e["id"], {"draftkings": (-110, -110, 52.5)})
                for e in evs]
        paid = []
        res, _ = _bulk_run(evs, bulk, paid)
        self.assertEqual(res["status"], "ok")
        self.assertEqual(len(paid), 1)
        # Bulk call targets the bulk endpoint: markets as list, us region.
        self.assertEqual(paid[0], (["totals"], "us", "ncaaf"))
        self.assertEqual(res["cost_model"], "bulk_per_tick")
        self.assertEqual(res["cost_per_tick_credits"], 1)
        self.assertGreater(res["n_quotes"], 0)

    def test_per_run_cap_skip_when_cap_below_two_credits(self):
        evs = _events(70)
        paid = []

        def fake_odds(*a, **k):
            paid.append(1)
            raise AssertionError("paid call despite run cap")

        with patch.dict("os.environ", {"TIMING_MAX_CREDITS_PER_RUN": "1"}):
            res = st.run_collection(
                fetch_events_fn=lambda sport="ncaaf": (evs, _headers(100)),
                fetch_odds_fn=fake_odds,
                now_utc=utc("2026-09-19T14:00:00Z"),
                day_spend_fn=lambda: 0)
        self.assertEqual(res["status"], "run_cap_skip")
        self.assertEqual(paid, [])

    def test_day_cap_boundary_55(self):
        evs = _events(70)
        bulk = [_bulk_event(e["id"], {"draftkings": (-110, -110, 52.5)})
                for e in evs]

        def fake_events(sport="ncaaf"):
            return evs, _headers(100)

        # 53 + worst-case 2 = 55 <= 55: proceeds.
        with patch.dict("os.environ", {"TIMING_MAX_CREDITS_DAY": "55"}):
            paid = []

            def fake_odds(markets, regions="us", sport="ncaaf"):
                paid.append(1)
                return bulk, _headers(101, last="1")

            res = st.run_collection(
                fetch_events_fn=fake_events, fetch_odds_fn=fake_odds,
                now_utc=utc("2026-09-19T14:00:00Z"),
                day_spend_fn=lambda: 53)
            self.assertEqual(res["status"], "ok", res["status"])
            self.assertEqual(len(paid), 1)

        # 54 + worst-case 2 = 56 > 55: refused before any paid call.
        with patch.dict("os.environ", {"TIMING_MAX_CREDITS_DAY": "55"}):
            paid = []

            def fake_odds2(*a, **k):
                paid.append(1)
                raise AssertionError("paid call despite day cap")

            res = st.run_collection(
                fetch_events_fn=fake_events, fetch_odds_fn=fake_odds2,
                now_utc=utc("2026-09-19T14:00:00Z"),
                day_spend_fn=lambda: 54)
            self.assertEqual(res["status"], "day_cap_skip")
            self.assertEqual(paid, [])

    def test_credit_constants(self):
        self.assertEqual(st.EXPECTED_DAY_CREDITS, 48)
        self.assertEqual(st.DEFAULT_MAX_CREDITS_DAY, 55)
        self.assertEqual(st.COST_PER_TICK, 1)
        self.assertEqual(st.MAX_ATTEMPTS_PER_TICK, 2)

    def test_quota_standdown_before_paid_calls(self):
        evs = _events(70)
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
        evs = _events(3)
        bulk = [_bulk_event(e["id"], {"draftkings": (-110, -110, 52.5)})
                for e in evs]
        paid = []
        res, _ = _bulk_run(evs, bulk, paid, used0=100)
        # Complete-run delta: used after the bulk call minus used before
        # the first paid call (the free events call is excluded).
        self.assertEqual(res["credits_used_before"], 100)
        self.assertEqual(res["credits_used_after"], 101)
        self.assertEqual(res["credits_consumed"], 1)
        self.assertEqual(res["credits_spent_tracked"], 1)
        self.assertEqual(res["n_bulk_attempts"], 1)


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


class BulkStateTests(unittest.TestCase):
    def _by(self, states):
        return {(s["provider_event_id"], s["book_key"]): s["state"]
                for s in states}

    def _ev(self, eid):
        return {"id": eid, "commence_time": "2026-09-19T16:00:00Z",
                "home_team": "X", "away_team": "Y"}

    def test_available_absent_from_bulk(self):
        uni = ["dk", "fd"]
        evs = [self._ev("e1"), self._ev("e2")]
        bulk = [_bulk_event("e1", {"dk": (-110, -110, 52.5)})]
        quotes, states, stats = st.process_bulk_tick(
            bulk, evs, uni, "2026-09-19T14:00:00+00:00",
            "2026-09-19T14:00:01+00:00")
        by = self._by(states)
        self.assertEqual(by[("e1", "dk")], "available")
        self.assertEqual(by[("e1", "fd")], "absent")
        # e2 expected but missing from the bulk payload: absent, not
        # request_failed (the fetch succeeded).
        self.assertEqual(by[("e2", "dk")], "absent")
        self.assertEqual(by[("e2", "fd")], "absent")
        self.assertEqual(stats["n_expected_missing_from_bulk"], 1)
        self.assertEqual({q["provider_event_id"] for q in quotes}, {"e1"})

    def test_unexpected_event_unmapped(self):
        uni = ["dk"]
        evs = [self._ev("e1")]
        bulk = [_bulk_event("e1", {"dk": (-110, -110, 52.5)}),
                _bulk_event("eX", {"dk": (-110, -110, 52.5)})]
        quotes, states, stats = st.process_bulk_tick(
            bulk, evs, uni, "slot", "now")
        by = self._by(states)
        self.assertEqual(by[("e1", "dk")], "available")
        self.assertEqual(by[("eX", "dk")], "unmapped")
        self.assertEqual(stats["n_unexpected_events"], 1)
        # Quotes for the unexpected event are still stored (flagged).
        self.assertIn("eX", {q["provider_event_id"] for q in quotes})

    def test_non_universe_books_recorded_not_silent(self):
        uni = ["dk"]
        evs = [self._ev("e1")]
        bulk = [_bulk_event("e1", {"dk": (-110, -110, 52.5),
                                  "xx": (-110, -110, 52.5)})]
        _, states, _ = st.process_bulk_tick(bulk, evs, uni, "slot", "now")
        xx = [s for s in states if s["book_key"] == "xx"]
        self.assertEqual(len(xx), 1)
        self.assertEqual(xx[0]["state"], "available")
        self.assertFalse(xx[0]["universe_member"])

    def test_observed_at_carried_for_available_books(self):
        uni = ["dk", "fd"]
        evs = [self._ev("e1")]
        bulk = [_bulk_event("e1", {"dk": (-110, -110, 52.5)})]
        _, states, _ = st.process_bulk_tick(bulk, evs, uni, "slot", "now")
        by = {(s["provider_event_id"], s["book_key"]): s for s in states}
        self.assertIsNotNone(by[("e1", "dk")]["observed_at"])
        self.assertIsNone(by[("e1", "fd")]["observed_at"])


class RetryTests(unittest.TestCase):
    def test_single_retry_then_success(self):
        evs = _events(2)
        bulk = [_bulk_event(e["id"], {"draftkings": (-110, -110, 52.5)})
                for e in evs]
        paid = []
        res, _ = _bulk_run(evs, bulk, paid, fail_times=1)
        self.assertEqual(res["status"], "ok")
        self.assertEqual(len(paid), 2)  # exactly one retry, never more
        self.assertEqual(res["n_bulk_attempts"], 2)
        self.assertEqual(len(res["retries"]), 1)
        r = res["retries"][0]
        self.assertEqual(r["attempt"], 2)
        self.assertEqual(r["slot"], res["slot"])  # same tick timestamp
        self.assertTrue(r["reason"])
        self.assertEqual(r["outcome"], "succeeded")
        self.assertGreater(res["n_quotes"], 0)

    def test_two_failures_record_request_failed_tick(self):
        evs = _events(2)
        paid = []
        res, _ = _bulk_run(evs, [], paid, fail_times=99)
        self.assertEqual(res["status"], "request_failed")
        self.assertEqual(len(paid), 2)  # initial + exactly one retry
        self.assertEqual(res["n_bulk_attempts"], 2)
        self.assertEqual(res["n_quotes"], 0)
        # Whole tick marked request_failed: every expected event x
        # universe book; no individual-book absence inferred.
        uni = st.book_universe()
        self.assertEqual(len(res["_book_states"]), len(evs) * len(uni))
        self.assertTrue(all(s["state"] == "request_failed"
                            for s in res["_book_states"]))
        self.assertEqual(len(res["retries"]), 1)
        self.assertEqual(res["retries"][0]["outcome"], "failed")
        self.assertEqual(res["retries"][0]["slot"], res["slot"])
        self.assertTrue(res["error"])
        # Fail-closed credit accounting: the tick costs the retry.
        self.assertEqual(res["credits_spent_tracked"], 2)
        self.assertEqual(res["credits_consumed"], 2)

    def test_failed_tick_is_persistable(self):
        self.assertIn("request_failed", st.PERSISTABLE_STATUSES)
        for s in st.NON_TICK_STATUSES:
            self.assertNotIn(s, st.PERSISTABLE_STATUSES)


class SchemaContractTests(unittest.TestCase):
    # The exact columns persist() binds into ncaaf_timing_quotes, which
    # is the schema the analyzer reads.
    QUOTE_KEYS = {"provider_event_id", "book_key", "market", "selection",
                  "line", "american_odds", "fair_probability",
                  "observed_at", "collected_at"}

    def test_bulk_quotes_carry_analyzer_schema(self):
        evs = _events(2)
        bulk = [_bulk_event(e["id"],
                           {"draftkings": (-110, -110, 52.5),
                            "fanduel": (-105, -115, 52.5)})
                for e in evs]
        quotes, _, _ = st.process_bulk_tick(
            bulk, evs, st.book_universe(), "slot", "now")
        self.assertTrue(quotes)
        for q in quotes:
            self.assertTrue(self.QUOTE_KEYS <= set(q),
                            self.QUOTE_KEYS ^ set(q))
            self.assertEqual(q["market"], frozen.MARKET)  # FULL_GAME_TOTAL
            self.assertIn(q["selection"], ("Over", "Under"))
            self.assertIsNotNone(q["fair_probability"])

    def test_failed_tick_contributes_no_quotes(self):
        # The analyzer builds per-event slot series from
        # ncaaf_timing_quotes; a failed tick contributes no rows, so the
        # slot is skipped and lifetime_bounds widens the interval via
        # actual timestamps: a censored gap, never a fabricated
        # disappearance.
        evs = _events(2)
        paid = []
        res, _ = _bulk_run(evs, [], paid, fail_times=99)
        self.assertEqual(res["_quotes"], [])
        self.assertTrue(res["_book_states"])


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
