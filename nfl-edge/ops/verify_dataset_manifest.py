#!/usr/bin/env python3
"""Verify a versioned research-dataset manifest against the live tables.

Read-only on data tables (the fingerprint computation runs in a
read-only session). The ONLY writes this script ever performs are:

  * idempotent CREATE TABLE / trigger for `research_dataset_manifests`
    (from nfl-edge/ops/migrations/019_research_dataset_manifests.sql), and
  * with --record: INSERTing a NEW manifest row (never UPDATE/DELETE).

It compares row count, snapshot count, and the canonical-1 fingerprint
against the manifest row for (version, scope) and FAILS LOUDLY on any
mismatch -- the signal that an unrelated backfill touched the table.

Usage:
    NFL_EDGE_DATABASE_URL=... python nfl-edge/ops/verify_dataset_manifest.py \\
        --scope nfl-2022-2024-spread-total --version latest --out /tmp/verify.json
    # one-time v1 population (explicit opt-in):
    NFL_EDGE_DATABASE_URL=... python nfl-edge/ops/verify_dataset_manifest.py \\
        --scope nfl-2022-2024-spread-total --version v1 --record \\
        --algorithm-commit <sha> --out /tmp/verify.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fingerprint_dataset as fd  # noqa: E402

HERE = Path(__file__).resolve().parent
MIGRATION_019 = HERE / "migrations" / "019_research_dataset_manifests.sql"

#: Scope -> (sport, market_scope) for fingerprint_dataset.fingerprint().
KNOWN_SCOPES = {
    "nfl-2022-2024-spread-total": ("nfl", "spread_total"),
}


def ensure_manifest_table(conn) -> None:
    """Idempotently create research_dataset_manifests (DDL only)."""
    conn.execute(MIGRATION_019.read_text())


def get_manifest_row(conn, version: str, scope: str):
    """Fetch the manifest row: exact version, or latest for the scope."""
    if version == "latest":
        row = conn.execute(
            """SELECT version, scope, fingerprint, algorithm_commit,
                      row_count, snapshot_count, created_at, notes
               FROM public.research_dataset_manifests
               WHERE scope = %s
               ORDER BY id DESC LIMIT 1""",
            (scope,),
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT version, scope, fingerprint, algorithm_commit,
                      row_count, snapshot_count, created_at, notes
               FROM public.research_dataset_manifests
               WHERE version = %s AND scope = %s""",
            (version, scope),
        ).fetchone()
    if row is None:
        return None
    keys = ("version", "scope", "fingerprint", "algorithm_commit",
            "row_count", "snapshot_count", "created_at", "notes")
    return dict(zip(keys, row))


def record_manifest_row(conn, version: str, scope: str, computed: dict,
                        algorithm_commit: str, notes: str = "") -> dict:
    """INSERT a new manifest row; refuse if (version, scope) exists.

    Never updates. If a row already exists with a DIFFERENT fingerprint,
    that is a hard failure: the same version must never describe two
    datasets (mint a new version instead).
    """
    existing = get_manifest_row(conn, version, scope)
    if existing is not None:
        if existing["fingerprint"] != computed["dataset_fingerprint"]:
            raise RuntimeError(
                "manifest row %s/%s already exists with a DIFFERENT "
                "fingerprint; mint a new version instead of reusing %s"
                % (version, scope, version))
        return {"action": "already_present", "row": existing}
    row = conn.execute(
        """INSERT INTO public.research_dataset_manifests
               (version, scope, fingerprint, algorithm_commit,
                row_count, snapshot_count, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (version, scope, computed["dataset_fingerprint"], algorithm_commit,
         computed["n_quotes"], computed["n_snapshots"], notes),
    ).fetchone()
    return {"action": "inserted", "id": row[0]}


def compare(computed: dict, manifest: dict) -> list:
    """Return a list of mismatch descriptions (empty = verified)."""
    mismatches = []
    if manifest["row_count"] != computed["n_quotes"]:
        mismatches.append(
            "row_count: manifest=%d computed=%d"
            % (manifest["row_count"], computed["n_quotes"]))
    if manifest["snapshot_count"] != computed["n_snapshots"]:
        mismatches.append(
            "snapshot_count: manifest=%d computed=%d"
            % (manifest["snapshot_count"], computed["n_snapshots"]))
    if manifest["fingerprint"] != computed["dataset_fingerprint"]:
        mismatches.append(
            "fingerprint: manifest=%s computed=%s"
            % (manifest["fingerprint"],
               computed["dataset_fingerprint"]))
    return mismatches


def run(dsn: str, scope: str, version: str, record: bool,
        algorithm_commit: str, notes: str = "") -> dict:
    """Execute verify (and optional one-time record). Returns report."""
    if scope not in KNOWN_SCOPES:
        raise ValueError(f"unknown scope: {scope!r} "
                         f"(known: {sorted(KNOWN_SCOPES)})")
    sport, market_scope = KNOWN_SCOPES[scope]
    report = {
        "scope": scope,
        "sport": sport,
        "market_scope": market_scope,
        "method": fd.METHOD,
        "requested_version": version,
        "record_requested": record,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "api_credits_used": 0,
    }
    import psycopg  # lazy

    # 1. Ensure the manifest table exists (DDL on the manifest table only;
    #    never touches data tables).
    with psycopg.connect(dsn) as conn:
        ensure_manifest_table(conn)

    # 2. Recompute the fingerprint in a READ-ONLY session.
    with psycopg.connect(
            dsn, options="-c default_transaction_read_only=on") as conn:
        computed = fd.fingerprint(conn, sport, market_scope)
    report["computed"] = computed

    # 3. Optional one-time population of a new manifest row.
    if record:
        with psycopg.connect(dsn) as conn:
            rec = record_manifest_row(conn, version, scope, computed,
                                      algorithm_commit, notes)
        report["record"] = {
            "action": rec["action"],
            "algorithm_commit": algorithm_commit,
        }

    # 4. Compare against the manifest row (read-only session).
    with psycopg.connect(
            dsn, options="-c default_transaction_read_only=on") as conn:
        manifest = get_manifest_row(
            conn, version if version != "latest" else "latest", scope)
    if manifest is None:
        report["verified"] = False
        report["error"] = (
            "no manifest row for version=%s scope=%s; "
            "run once with --record to populate it" % (version, scope))
        return report
    # created_at may be a datetime: render for JSON.
    manifest["created_at"] = str(manifest["created_at"])
    report["manifest"] = manifest
    mismatches = compare(computed, manifest)
    report["mismatches"] = mismatches
    report["verified"] = not mismatches
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="nfl-2022-2024-spread-total")
    ap.add_argument("--version", default="latest")
    ap.add_argument("--record", action="store_true",
                    help="one-time: INSERT this computation as a new "
                         "manifest row (never updates)")
    ap.add_argument("--algorithm-commit", default="",
                    help="commit SHA of the fingerprint algorithm "
                         "(recorded in the manifest row)")
    ap.add_argument("--notes", default="")
    ap.add_argument("--out", default="/tmp/nfl_dataset_verify.json")
    args = ap.parse_args(argv)

    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL is required", file=sys.stderr)
        return 2
    if args.record and not args.algorithm_commit:
        args.algorithm_commit = os.environ.get("GITHUB_SHA", "unknown")

    try:
        report = run(dsn, args.scope, args.version, args.record,
                     args.algorithm_commit, args.notes)
    except Exception as exc:  # fail loudly, never silently
        print(f"::error::dataset manifest verification failed: {exc}",
              file=sys.stderr)
        return 3

    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))

    if not report.get("verified"):
        for m in report.get("mismatches", []):
            print(f"::error::MISMATCH: {m}", file=sys.stderr)
        if report.get("error"):
            print(f"::error::{report['error']}", file=sys.stderr)
        return 3
    print("VERIFIED: manifest %s/%s matches live tables"
          % (report["manifest"]["version"], args.scope))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
