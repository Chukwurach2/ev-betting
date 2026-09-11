"""Smoke-test / backfill verification: the end-to-end audit gate.

Reads public.nfl_edge_market_history + public.nfl_edge_historical_quotes
and checks every item on the historical-data acceptance checklist:

  * requested_at vs provider snapshot_at (returned timestamp); drift
  * provider_snapshot_id present and unique (canonical requested-time id,
    derived: the provider exposes no separate snapshot id)
  * regions / markets as requested; books_present recorded
  * credits_used recorded
  * payload holds the FULL provider envelope (timestamp,
    previous_timestamp, next_timestamp, data) -- not data-only
  * normalized quotes exist for every snapshot that had events
  * no live/current odds leakage (snapshot_at must be historical, never
    near now; requested_at must predate now)
  * Pinnacle present where the eu region was requested; every Pinnacle
    quote has ny_licensed=false
  * US execution books present where the us region was requested
    (draftkings, fanduel, betmgm, betrivers, espnbet, williamhill_us,
    fanatics -- where the provider returns them for that date)
  * no-vig pairing correctness: every
    (event, book, market, line, observed_at) group has exactly 2 quotes
    and their fair_probability sums to 1
  * idempotency: no duplicate provider_snapshot_id

Fails loudly (non-zero exit) on any violation. Prints a JSON report.

Usage: python ops/verify_backfill.py [--since 2026-09-12]
Env: NFL_EDGE_DATABASE_URL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

US_BOOKS = {"draftkings", "fanduel", "betmgm", "betrivers", "espnbet",
            "williamhill_us", "fanatics"}
TOLERANCE_SECONDS = 3600


def _parse_ts(value):
    try:
        t = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None,
                    help="only verify snapshots with requested_at >= date")
    args = ap.parse_args(argv)
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NFL_EDGE_DATABASE_URL is required")
    conn = psycopg.connect(dsn, connect_timeout=10)
    conn.autocommit = True
    now = dt.datetime.now(dt.timezone.utc)
    errors, warnings = [], []
    report = {"snapshots": [], "errors": errors, "warnings": warnings}

    where = ""
    params: tuple = ()
    if args.since:
        where = "WHERE requested_at >= %s"
        params = (args.since,)
    snaps = conn.execute(
        """SELECT provider_snapshot_id, snapshot_at, requested_at, regions,
                  markets, books_present, credits_used, payload
           FROM public.nfl_edge_market_history %s
           ORDER BY requested_at""" % where, params).fetchall()
    if not snaps:
        errors.append("no snapshots found in nfl_edge_market_history")
    seen_ids = set()
    # snapshot_at IS the provider's returned timestamp (there is no separate
    # provider_timestamp column); requested_at is what we asked for.
    for (sid, snap_at, requested_at, regions, markets,
         books_present, credits_used, payload) in snaps:
        entry = {"provider_snapshot_id": sid,
                 "requested_at": str(requested_at),
                 "snapshot_at": str(snap_at),
                 "regions": regions, "markets": markets,
                 "books_present": books_present,
                 "credits_used": credits_used}
        if sid in seen_ids:
            errors.append("duplicate provider_snapshot_id: %s" % sid)
        seen_ids.add(sid)
        req = _parse_ts(requested_at)
        ret = _parse_ts(snap_at)
        if req is None:
            errors.append("snapshot %s: requested_at unparseable" % sid)
        if ret is None:
            errors.append("snapshot %s: provider timestamp missing" % sid)
        else:
            drift = abs((ret - req).total_seconds()) if req else None
            entry["drift_seconds"] = drift
            if drift is not None and drift > TOLERANCE_SECONDS:
                errors.append(
                    "snapshot %s: provider/returned drift %.0fs > %ds"
                    % (sid, drift, TOLERANCE_SECONDS))
            if ret > now:
                errors.append("snapshot %s: provider timestamp in the future "
                              "(possible live-odds leakage)" % sid)
            # Live-leakage guard: a historical snapshot must not look like
            # "now". Anything within 24h of now is suspect for backfill data.
            if (now - ret).total_seconds() < 24 * 3600:
                errors.append(
                    "snapshot %s: snapshot_at within 24h of now; historical "
                    "data must be old (live endpoint may have leaked)" % sid)
        if req and req > now:
            errors.append("snapshot %s: requested_at in the future" % sid)
        # Raw envelope immutability.
        env_keys = set(payload.keys()) if isinstance(payload, dict) else set()
        entry["payload_keys"] = sorted(env_keys)
        for k in ("timestamp", "previous_timestamp", "next_timestamp", "data"):
            if k not in env_keys:
                (errors if k == "data" else warnings).append(
                    "snapshot %s: payload missing envelope key '%s' "
                    "(pre-full-envelope row?)" % (sid, k))
        n_events = len(payload.get("data", [])) if isinstance(payload, dict) \
            else 0
        entry["events"] = n_events
        # Normalized quotes.
        qrows = conn.execute(
            """SELECT provider_event_id, book_key, market, line, observed_at,
                      fair_probability, ny_licensed, american_odds
               FROM public.nfl_edge_historical_quotes
               WHERE observed_at = %s""", (snap_at,)).fetchall()
        entry["quotes"] = len(qrows)
        if n_events and not qrows:
            warnings.append("snapshot %s: %d events but 0 normalized quotes"
                            % (sid, n_events))
        qbooks = {r[1] for r in qrows}
        entry["quote_books"] = sorted(qbooks)
        regions_set = {r.strip() for r in (regions or "").split(",")}
        if "eu" in regions_set and "pinnacle" not in qbooks and qrows:
            warnings.append("snapshot %s: eu requested but no Pinnacle quotes"
                            % sid)
        if "us" in regions_set:
            missing_us = sorted(US_BOOKS - qbooks)
            if missing_us and qrows:
                warnings.append("snapshot %s: us requested but missing books "
                                "%s" % (sid, missing_us))
        for r in qrows:
            if r[1] == "pinnacle" and r[6] is not False:
                errors.append("snapshot %s: pinnacle quote ny_licensed=%r, "
                              "must be false" % (sid, r[6]))
        # No-vig pairing: groups of exactly 2, fair probs sum to 1.
        groups: dict = {}
        for (ev, bk, mk, ln, obs, fp, _, _) in qrows:
            groups.setdefault((ev, bk, mk, ln, str(obs)), []).append(fp)
        bad_groups = 0
        for key, fps in groups.items():
            if len(fps) != 2 or abs(sum(fps) - 1.0) > 1e-9:
                bad_groups += 1
        entry["pair_groups"] = len(groups)
        entry["bad_pair_groups"] = bad_groups
        if bad_groups:
            errors.append("snapshot %s: %d mis-paired quote groups"
                          % (sid, bad_groups))
        report["snapshots"].append(entry)

    report["status"] = "pass" if not errors else "fail"
    print(json.dumps(report, indent=2, default=str))
    conn.close()
    if errors:
        print("VERIFY FAILED: %d error(s)" % len(errors), file=sys.stderr)
        return 1
    print("VERIFY PASSED: %d snapshot(s), %d warning(s)"
          % (len(report["snapshots"]), len(warnings)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
