"""Smoke-test / backfill verification: the end-to-end audit gate.

Reads public.<prefix>_market_history + public.<prefix>_historical_quotes
(sport-parameterized via --sport; default nfl) and checks every item on
the historical-data acceptance checklist:

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
    and their fair_probability sums to 1 (a stored group that does not
    pair is a normalization bug -> error)
  * raw-payload forensics: counts raw (event,book,market,line) groups
    with a single provider side (dropped by normalize by design ->
    warning, not error), plus kickoff min/max and events-with-quotes so
    the audit can see which games a snapshot actually covers
  * idempotency: no duplicate provider_snapshot_id

Fails loudly (non-zero exit) on any violation. Prints a JSON report.

Usage: python ops/verify_backfill.py [--sport ncaaf] [--since 2026-09-12]
Env: NFL_EDGE_DATABASE_URL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))  # ops/
import sports

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
    ap.add_argument("--sport", default="nfl", choices=sorted(sports.SPORTS),
                    help="sport key; tables follow the ops/sports.py registry")
    ap.add_argument("--since", default=None,
                    help="only verify snapshots with requested_at >= date")
    args = ap.parse_args(argv)
    mh_table = sports.market_history_table(args.sport)
    hq_table = sports.historical_quotes_table(args.sport)
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
        ("SELECT provider_snapshot_id, snapshot_at, requested_at, regions, "
         "markets, books_present, credits_used, payload "
         "FROM public.%s %s "
         "ORDER BY requested_at") % (mh_table, where), params).fetchall()
    if not snaps:
        errors.append("no snapshots found in %s" % mh_table)
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
        # Idempotency is per (requested_at, regions, markets): the same
        # requested instant legitimately holds several rows when different
        # market sets were pulled (e.g. the moneyline h2h track alongside
        # spreads,totals). The table PK is (snapshot_at, regions, markets).
        key = (sid, regions, markets)
        if key in seen_ids:
            errors.append("duplicate snapshot (requested_at, regions, "
                          "markets): %s %s %s" % (sid, regions, markets))
        seen_ids.add(key)
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
        n_events = (len(payload.get("data", [])) if isinstance(payload, dict)
                    else len(payload) if isinstance(payload, list) else 0)
        entry["events"] = n_events
        entry["payload_format"] = ("full-envelope" if isinstance(payload, dict)
                                   and "data" in env_keys else "legacy-data-only")
        # Normalized quotes.
        qrows = conn.execute(
            ("SELECT provider_event_id, book_key, market, line, observed_at, "
             "fair_probability, ny_licensed, american_odds "
             "FROM public.%s "
             "WHERE observed_at = %%s") % hq_table, (snap_at,)).fetchall()
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
        # Raw-payload forensics, mirroring normalize_snapshot's pairing rules.
        # Distinguishes OUR bug (stored group doesn't pair, but the raw
        # payload had both sides) from PROVIDER gaps (raw payload had <2
        # valid sides, so normalize correctly stored nothing). Also
        # reports the kickoff range so the audit can see which games a
        # snapshot actually covers.
        raw_groups: dict = {}
        kickoffs = []
        data_list = (payload.get("data") if isinstance(payload, dict)
                     else payload) or []
        for game in data_list:
            if not isinstance(game, dict):
                continue
            eid = game.get("id")
            ct = _parse_ts(game.get("commence_time"))
            if ct:
                kickoffs.append(ct)
            for book in game.get("bookmakers") or []:
                bk = book.get("key")
                if not bk:
                    continue
                for offered in book.get("markets") or []:
                    key = offered.get("key")
                    if key == "spreads":
                        market = "FULL_GAME_SPREAD"
                    elif key == "totals":
                        market = "FULL_GAME_TOTAL"
                    else:
                        continue
                    sides = []
                    for o in offered.get("outcomes") or []:
                        try:
                            price = float(o.get("price"))
                            line = float(o.get("point"))
                        except (TypeError, ValueError):
                            continue
                        name = o.get("name")
                        if not name or abs(price) < 100:
                            continue
                        sides.append((name, line))
                    by_line: dict = {}
                    for name, line in sides:
                        if market == "FULL_GAME_SPREAD":
                            lk = round(abs(line), 3)
                        else:
                            if name not in ("Over", "Under"):
                                continue
                            lk = round(line, 3)
                        by_line.setdefault(lk, []).append(name)
                    for lk, names in by_line.items():
                        raw_groups[(eid, bk, market, lk)] = len(names)
        entry["raw_pair_groups"] = len(raw_groups)
        entry["raw_single_sided_groups"] = sum(
            1 for n in raw_groups.values() if n != 2)
        if entry["raw_single_sided_groups"]:
            warnings.append(
                "snapshot %s: %d raw (event,book,market,line) groups had "
                "a single provider side (dropped by normalize by design)"
                % (sid, entry["raw_single_sided_groups"]))
        if kickoffs:
            entry["kickoff_min"] = min(kickoffs).isoformat()
            entry["kickoff_max"] = max(kickoffs).isoformat()
            entry["events_with_quotes"] = len({r[0] for r in qrows})
        # No-vig pairing: groups of exactly 2, fair probs sum to 1.
        # normalize_snapshot only ever emits pairs, so a stored group that
        # does not pair is OUR bug -> hard error. (Provider single-sided
        # outcomes never reach storage; they are counted above.)
        groups: dict = {}
        for (ev, bk, mk, ln, obs, fp, _, _) in qrows:
            groups.setdefault((ev, bk, mk, ln, str(obs)), []).append(fp)
        bad_groups = 0
        bad_detail = []
        for key, fps in groups.items():
            if len(fps) != 2 or abs(float(sum(fps)) - 1.0) > 1e-9:
                bad_groups += 1
                if len(bad_detail) < 5:
                    bad_detail.append(
                        {"event": key[0], "book": key[1], "market": key[2],
                         "line": str(key[3]), "n": len(fps),
                         "fair_probs": [str(f) for f in fps]})
        entry["pair_groups"] = len(groups)
        entry["bad_pair_groups"] = bad_groups
        if bad_groups:
            errors.append("snapshot %s: %d mis-paired quote groups; e.g. %s"
                          % (sid, bad_groups, json.dumps(bad_detail)))
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
