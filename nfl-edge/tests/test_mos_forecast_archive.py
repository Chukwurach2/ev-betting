"""Unit tests for the prospective MOS forecast archive.

DB-free and network-free: covers the frozen rule mechanics, the stadium
mapping contract, and the append-only guarantee (static source check).
"""
import json
import pathlib
import re
import sys
from datetime import datetime, timedelta, timezone

OPS = pathlib.Path(__file__).parents[1] / "ops"
sys.path.insert(0, str(OPS))

import mos_forecast_archive as mfa  # noqa: E402

UTC = timezone.utc
REPO = pathlib.Path(__file__).parents[2]

candidate_runtimes = mfa.candidate_runtimes
is_eligible = mfa.is_eligible
parse_kickoff = mfa.parse_kickoff
prediction_cutoff = mfa.prediction_cutoff
select_cycle = mfa.select_cycle


def dt(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


# ---------------------------------------------------------------- frozen rules

def test_cutoff_is_kickoff_minus_24h():
    ko = dt("2026-09-20 17:00")
    assert prediction_cutoff(ko) == dt("2026-09-19 17:00")


def test_eligibility_boundary():
    cutoff = dt("2026-09-19 17:00")
    # runtime + 4h <= cutoff  -> eligible (boundary inclusive)
    assert is_eligible(dt("2026-09-19 13:00"), cutoff)
    assert not is_eligible(dt("2026-09-19 13:01"), cutoff)
    assert not is_eligible(dt("2026-09-19 18:00"), cutoff)


def test_selection_is_max_eligible():
    cutoff = dt("2026-09-19 17:00")
    runtimes = [dt("2026-09-18 12:00"), dt("2026-09-19 06:00"),
                dt("2026-09-19 12:00"), dt("2026-09-19 18:00")]
    # 12:00 + 4h = 16:00 <= 17:00 eligible; 18:00 not eligible
    assert select_cycle(runtimes, cutoff) == dt("2026-09-19 12:00")


def test_selection_none_when_nothing_eligible():
    cutoff = dt("2026-09-19 17:00")
    assert select_cycle([dt("2026-09-19 18:00")], cutoff) is None
    assert select_cycle([], cutoff) is None


def test_selection_never_picks_by_fit():
    # The rule is mechanical on timestamps only; ordering of input must
    # not matter.
    cutoff = dt("2026-09-19 17:00")
    runtimes = [dt("2026-09-19 12:00"), dt("2026-09-18 00:00")]
    assert select_cycle(list(reversed(runtimes)), cutoff) == select_cycle(
        runtimes, cutoff)


# ---------------------------------------------------------------- kickoff parsing

def test_parse_kickoff_edt():
    # 1:00 PM ET on 2026-09-20 (EDT, UTC-4) -> 17:00 UTC
    assert parse_kickoff("2026-09-20", "13:00") == dt("2026-09-20 17:00")


def test_parse_kickoff_est():
    # 1:00 PM ET on 2026-12-20 (EST, UTC-5) -> 18:00 UTC
    assert parse_kickoff("2026-12-20", "13:00") == dt("2026-12-20 18:00")


def test_parse_kickoff_prime_time():
    # SNF 8:20 PM ET -> next-day 00:20 UTC
    assert parse_kickoff("2026-09-20", "20:20") == dt("2026-09-21 00:20")


# ---------------------------------------------------------------- cycle grid

def test_candidate_runtimes_grid_and_freshness():
    now = dt("2026-09-16 10:10")
    cands = candidate_runtimes(now, lookback_hours=12)
    # freshness: nothing newer than now - 30 min (09:40 -> grid 06:00)
    assert max(cands) == dt("2026-09-16 06:00")
    assert all(c.hour % 6 == 0 and c.minute == 0 for c in cands)
    assert min(cands) >= now - timedelta(hours=12) - timedelta(hours=6)


# ---------------------------------------------------------------- mapping contract

def _mapping():
    return json.loads((OPS / "nfl_stadium_mos.json").read_text())


def test_mapping_covers_all_2026_schedule_stadiums():
    sched = json.loads((OPS / "nflverse_schedules_2026.json").read_text())
    mapping = _mapping()
    stadia = {g["stadium"] for g in sched["games"]}
    missing = stadia - set(mapping.keys()) - {"_meta"}
    assert not missing, f"stadiums without mapping: {missing}"


def test_verified_stations_shape():
    mapping = _mapping()
    verified = {k: v for k, v in mapping.items()
                if k != "_meta" and v["status"] == "verified"}
    assert len(verified) == 30
    for name, v in verified.items():
        assert re.fullmatch(r"K[A-Z]{3}", v["mos_station"]), name
        assert v["verified_at"] == "2026-09-16", name
        assert v["unavailable_reason"] is None, name
    stations = [v["mos_station"] for v in verified.values()]
    assert len(set(stations)) == len(stations), "duplicate stations"


def test_unmatched_have_explicit_reason_and_no_station():
    mapping = _mapping()
    unmatched = {k: v for k, v in mapping.items()
                 if k != "_meta" and v["status"] == "unmatched"}
    assert len(unmatched) == 8
    for name, v in unmatched.items():
        assert v["mos_station"] is None, name
        assert v["unavailable_reason"], name
        assert v["verified_at"] is None, name


def test_no_silent_guesses():
    # Every entry is either verified (proven against the live IEM archive)
    # or explicitly unmatched. There is no third state.
    mapping = _mapping()
    for name, v in mapping.items():
        if name == "_meta":
            continue
        assert v["status"] in ("verified", "unmatched"), name


# ---------------------------------------------------------------- append-only guarantee

def test_collector_never_rewrites_archive():
    src = (OPS / "mos_forecast_archive.py").read_text()
    # No UPDATE/DELETE *SQL statements* anywhere in the collector: look for
    # the keywords at the start of a string literal passed to execute().
    # (Docstring mentions of the words are fine.)
    assert not re.search(
        r"execute\(\s*(?:f)?(?:\"\"\"|'''|\"|')\s*(UPDATE|DELETE)\b",
        src, re.IGNORECASE), "UPDATE/DELETE statement found"


def test_migration_has_immutability_trigger():
    sql = (OPS / "migrations" / "018_nfl_mos_forecast_archive.sql").read_text()
    assert "nfl_mos_archive_immutable" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    # migrate.py refuses destructive tokens; keep the file clean.
    for token in ("DROP TABLE", "DROP DATABASE", "TRUNCATE", "DELETE FROM"):
        assert token not in sql.upper(), token


def test_workflow_schedule_present():
    wf = (REPO / ".github" / "workflows" / "mos-forecast-archive.yml").read_text()
    # Cost-trim 2026-09-20: once daily at 10:10 UTC (was 4x/day
    # "10 4,10,16,22 * * *"); the workflow header documents the trim.
    assert "10 10 * * *" in wf
    assert "migrate.py" in wf
    assert "mos_forecast_archive.py" in wf
