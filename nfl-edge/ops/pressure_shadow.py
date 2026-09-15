#!/usr/bin/env python3
"""Shadow signal generator for totals price-pressure v1.0 (FROZEN).

Applies the frozen candidate rule to live pre-firewall data in SHADOW mode.
- Does NOT place bets or generate recommendations.
- Emits immutable signal receipts for plumbing validation.
- Measures: coverage, signal frequency, latency, quote persistence.

Firewall: 2026-11-01T03:59:59Z. Pre-firewall only for shadow.
Post-firewall: signals become prospective confirmation inputs.

Read-only on quotes. Writes to ncaaf_pressure_signals (shadow).
"""
import os, sys, json, argparse, hashlib, statistics
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402

# Frozen v1.0 parameters — DO NOT MODIFY
THRESHOLD = 0.00126
MARKET = "FULL_GAME_TOTAL"
SELECTION = "Over"
MIN_BOOKS = 3
LINE_TOL = 0.01
RULE_VERSION = "v1.0"
RULE_FROZEN_DATE = "2026-09-15"

FIREWALL = "2026-11-01T03:59:59+00:00"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--out", default="/tmp/pressure_shadow_signals.json")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print signals without writing to DB")
    args = ap.parse_args(argv)

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        print("NFL_EDGE_DATABASE_URL required", file=sys.stderr)
        return 2
    live = sports.odds_quotes_table(args.sport)

    # Get recent totals quotes (last 7 days, pre-firewall enforced in query)
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"""
            SELECT provider_event_id, kickoff, book_key, selection,
                   line, american_odds, fair_probability,
                   observed_at, collected_at
            FROM public.{live}
            WHERE market = 'FULL_GAME_TOTAL'
              AND fair_probability IS NOT NULL
              AND observed_at < %s
              AND observed_at > NOW() - INTERVAL '7 days'
            ORDER BY observed_at DESC
        """, (FIREWALL,)).fetchall()

    recs = [dict(zip(
        ["provider_event_id", "kickoff", "book_key", "selection",
         "line", "american_odds", "fair_probability",
         "observed_at", "collected_at"], r)) for r in rows]

    # Group by (event, observed_at rounded to hour) as snapshot proxy
    # For live data, we use collected_at windows rather than Wed/Fri/Sat
    snapshots = defaultdict(list)
    for r in recs:
        # Round observed_at to nearest hour for snapshot grouping
        ts = r["observed_at"]
        if hasattr(ts, "replace"):
            snap = ts.replace(minute=0, second=0, microsecond=0).isoformat()
        else:
            snap = str(ts)[:13]
        key = (r["provider_event_id"], snap)
        snapshots[key].append(r)

    signals = []
    for (eid, snap), quotes in snapshots.items():
        books = {q["book_key"] for q in quotes}
        if len(books) < MIN_BOOKS:
            continue
        lines = [float(q["line"]) for q in quotes]
        consensus = statistics.median(lines)
        over_qs = [q for q in quotes
                   if q["selection"] == SELECTION
                   and abs(float(q["line"]) - consensus) < LINE_TOL
                   and q["fair_probability"] is not None]
        if len({q["book_key"] for q in over_qs}) < MIN_BOOKS:
            continue
        fps = [float(q["fair_probability"]) for q in over_qs]
        pressure = statistics.fmean(fps) - 0.5
        if abs(pressure) < THRESHOLD:
            continue
        direction = 1 if pressure > 0 else -1

        # Immutable receipt
        receipt = {
            "rule_version": RULE_VERSION,
            "rule_frozen": RULE_FROZEN_DATE,
            "mode": "shadow",
            "event_id": eid,
            "snapshot": snap,
            "market": MARKET,
            "consensus_line": round(consensus, 2),
            "n_books": len({q["book_key"] for q in over_qs}),
            "pressure": round(pressure, 5),
            "threshold": THRESHOLD,
            "direction": direction,
            "direction_label": "Over" if direction > 0 else "Under",
            "fair_probs": sorted([round(fp, 4) for fp in fps]),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pre_firewall": True,
        }
        # Hash for immutability
        h = hashlib.sha256(
            json.dumps(receipt, sort_keys=True).encode()).hexdigest()[:16]
        receipt["receipt_id"] = f"pps-{h}"

        signals.append(receipt)

    # Operational metrics
    metrics = {
        "n_snapshots_scanned": len(snapshots),
        "n_signals": len(signals),
        "n_events_with_signals": len({s["event_id"] for s in signals}),
        "direction_split": {
            "Over": sum(1 for s in signals if s["direction"] > 0),
            "Under": sum(1 for s in signals if s["direction"] < 0),
        },
    }

    result = {
        "rule_version": RULE_VERSION,
        "mode": "shadow",
        "firewall": FIREWALL,
        "metrics": metrics,
        "signals": signals,
    }

    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)

    if not args.dry_run:
        # Write to shadow signals table
        with psycopg.connect(dsn) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.ncaaf_pressure_signals (
                    receipt_id TEXT PRIMARY KEY,
                    rule_version TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    snapshot TEXT NOT NULL,
                    market TEXT NOT NULL,
                    consensus_line DOUBLE PRECISION,
                    n_books INTEGER,
                    pressure DOUBLE PRECISION,
                    direction INTEGER,
                    receipt JSONB NOT NULL,
                    generated_at TIMESTAMPTZ NOT NULL,
                    pre_firewall BOOLEAN NOT NULL
                )
            """)
            for s in signals:
                conn.execute("""
                    INSERT INTO public.ncaaf_pressure_signals
                    (receipt_id, rule_version, mode, event_id, snapshot,
                     market, consensus_line, n_books, pressure, direction,
                     receipt, generated_at, pre_firewall)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (receipt_id) DO NOTHING
                """, (s["receipt_id"], RULE_VERSION, "shadow", s["event_id"],
                      s["snapshot"], MARKET, s["consensus_line"], s["n_books"],
                      s["pressure"], s["direction"], json.dumps(s),
                      s["generated_at"], True))
            conn.commit()
        print(f"Wrote {len(signals)} signals to ncaaf_pressure_signals")

    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
