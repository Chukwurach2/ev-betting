"""Pure diagnostics for retained provider event-list fixtures.

This module never resolves an event for collection and performs no I/O.  It
explains why the collector's existing exact matchup-and-kickoff rule would
accept or reject a recorded provider event list.  Production identity rules
remain unchanged and uncertain cases fail closed.
"""
from __future__ import annotations

import datetime as dt

UTC = dt.timezone.utc


def _instant(value):
    try:
        parsed = (value if isinstance(value, dt.datetime) else
                  dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def diagnose_provider_identity(expected_home, expected_away, expected_kickoff,
                               events):
    """Classify an exact provider-identity match from a retained fixture.

    Returned event ids are identifiers only; raw provider rows are never
    copied into the result.  A diagnostic status is evidence about the
    fixture, not permission to relax matching or retry a paid request.
    """
    kickoff = _instant(expected_kickoff)
    if (not isinstance(expected_home, str) or not expected_home.strip() or
            not isinstance(expected_away, str) or not expected_away.strip() or
            expected_home == expected_away or kickoff is None or
            not isinstance(events, list)):
        return {"status": "invalid_expected_identity", "exact_event_id": None}

    exact = []
    matchup = []
    reverse = []
    malformed = 0
    for event in events:
        if not isinstance(event, dict):
            malformed += 1
            continue
        event_id = event.get("id")
        home = event.get("home_team")
        away = event.get("away_team")
        event_kickoff = _instant(event.get("commence_time"))
        valid_id = isinstance(event_id, str) and bool(event_id.strip())
        if not valid_id:
            malformed += 1
        if (not isinstance(home, str) or not isinstance(away, str) or
                event_kickoff is None):
            if valid_id:
                malformed += 1
            continue
        row = {
            "event_id": event_id if valid_id else None,
            "kickoff_delta_seconds": int((event_kickoff - kickoff).total_seconds()),
        }
        if home == expected_home and away == expected_away:
            matchup.append(row)
            if event_kickoff == kickoff:
                exact.append(row)
        elif home == expected_away and away == expected_home:
            reverse.append(row)

    result = {
        "status": None,
        "exact_event_id": exact[0]["event_id"] if len(exact) == 1 else None,
        "exact_match_count": len(exact),
        "matchup_candidate_count": len(matchup),
        "reverse_candidate_count": len(reverse),
        "malformed_event_count": malformed,
        "closest_kickoff_delta_seconds": (
            min((row["kickoff_delta_seconds"] for row in matchup),
                key=lambda value: (abs(value), value)) if matchup else None),
        "candidate_event_ids": sorted(row["event_id"] for row in matchup if row["event_id"] is not None),
        "reverse_event_ids": sorted(row["event_id"] for row in reverse if row["event_id"] is not None),
    }
    if len(exact) == 1:
        result["status"] = ("exact_match" if exact[0]["event_id"] is not None
                            else "malformed_exact_match")
    elif len(exact) > 1:
        result["status"] = "ambiguous_exact_duplicates"
    elif matchup:
        result["status"] = "matchup_found_kickoff_mismatch"
    elif reverse:
        result["status"] = "teams_reversed"
    else:
        result["status"] = "matchup_absent"
    return result
