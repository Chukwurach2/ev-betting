#!/usr/bin/env python3
"""Live-event identity adapter for the NFL prop data layer.

Adapts the live `/v4/sports/americanfootball_nfl/events` payload into the
oe-dict shape that `nfl_canonical_identity.match_events()` expects, so the
tick-fresh ID resolution reuses the proven deterministic matcher unchanged
(explicit alias table, explicit states, no fuzzy matching).

Snapshot-scoping rule (hard, from the H-D incident 2026-09-16): the
provider re-issues event IDs across snapshots (median 2, max 5 per game).
A provider event_id is valid ONLY for the tick in which it was resolved.
This module never caches, persists, or reuses a resolved provider ID: the
collector calls resolve_tick() fresh every tick and stores the result on
the snapshot row only.
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nfl_canonical_identity import load_aliases, match_events  # noqa: E402

UTC = dt.timezone.utc


def normalize_live_event(provider_event):
    """Adapt one live events-list row to the matcher input shape.

    Live payload: {id, home_team, away_team, commence_time}.
    Matcher input (oe-dict): {event_id, home, away, kickoff} where
    kickoff is an aware UTC datetime parsed from the ISO-8601
    commence_time.

    Raises ValueError on a missing or unparsable commence_time — the
    collector records that tick's resolution as failed, never guessed.
    """
    if not isinstance(provider_event, dict):
        raise ValueError("provider event must be a dict")
    raw_id = provider_event.get("id")
    if not raw_id:
        raise ValueError("provider event has no id")
    commence = provider_event.get("commence_time")
    if not commence:
        raise ValueError(f"provider event {raw_id} has no commence_time")
    kickoff = _parse_commence_time(commence, raw_id)
    return {
        "event_id": raw_id,
        "home": provider_event.get("home_team"),
        "away": provider_event.get("away_team"),
        "kickoff": kickoff,
    }


def _parse_commence_time(commence, raw_id):
    if isinstance(commence, dt.datetime):
        kickoff = commence
    else:
        text = str(commence).strip()
        try:
            kickoff = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"provider event {raw_id} has unparsable commence_time: {text!r}"
            )
    if kickoff.tzinfo is None:
        raise ValueError(
            f"provider event {raw_id} commence_time has no timezone: {commence!r}"
        )
    return kickoff.astimezone(UTC)


def resolve_tick(events_payload, nv_games, seasons, aliases=None):
    """Resolve one tick's live events against canonical schedule rows.

    Pure function (no I/O): normalize each live event (§1.2 adapter), then
    run the shared deterministic matcher. Returns the raw match_events()
    results dict — the collector maps matched rows back onto its due
    (game, checkpoint) pairs; ambiguous and unmatched resolutions are
    recorded, never guessed.
    """
    if aliases is None:
        aliases = load_aliases()
    oe_events = []
    malformed = 0
    for e in events_payload:
        try:
            oe_events.append(normalize_live_event(e))
        except ValueError:
            # One malformed provider row must not kill the whole tick;
            # the row is skipped and counted, never guessed.
            malformed += 1
    if not oe_events and events_payload:
        # Every row malformed: fail loud, don't silently no-op.
        raise ValueError(
            f"resolve_tick: all {len(events_payload)} events-list rows malformed"
        )
    results = match_events(oe_events, nv_games, aliases, seasons)
    results["malformed_rows_skipped"] = malformed
    return results


def match_by_canonical(results):
    """Index matched rows by nflverse_game_id for O(1) per-game lookup."""
    index = {}
    for m in results.get("matched", []):
        # One canonical game maps to at most one event per tick: the events
        # list carries each game once. A second match for the same canonical
        # game is recorded, never silently preferred.
        index.setdefault(m["nflverse_game_id"], []).append(m)
    return index
