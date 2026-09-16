#!/usr/bin/env python3
"""Verify frozen NFL spread/total rows are value-intact since insertion.

Two independent checks, both read-only:
1. quote_id recomputation: quote_id = sha256 of
   "observed_at|provider_event_id|book_key|market|selection|line|american_odds"
   (exact construction from backfill_history.py at the freeze commit).
   Any post-insert change to those 7 fields breaks the hash.
2. fair_probability re-derivation: re-pair quotes exactly as the backfill did
   (totals grouped by round(line,3) with Over+Under; spreads grouped by
   abs(line) with two sides) and check fair_probability == implied/denom.

Covers every field H-N6 consumes from totals quotes.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg  # noqa: E402

TABLE = "nfl_edge_historical_quotes"
FROZEN_MARKETS = ("FULL_GAME_SPREAD", "FULL_GAME_TOTAL")


def implied(odds):
    odds = float(odds)
    return 100.0 / (100.0 + odds) if odds > 0 else abs(odds) / (100.0 + abs(odds))


def main() -> None:
    url = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not url:
        sys.exit(10)
    out: dict = {}
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            f"""SELECT quote_id, provider_event_id, book_key, market, selection,
                       line, american_odds, fair_probability, observed_at
                FROM {TABLE}
                WHERE market IN ('FULL_GAME_SPREAD','FULL_GAME_TOTAL')"""
        ).fetchall()
    out["rows_checked"] = len(rows)

    # Check 1: quote_id recomputation with the exact original rendering.
    # Settled empirically (forensic runs 35045350102/35045460883):
    #   raw_ts = observed_at rendered "%Y-%m-%dT%H:%M:%SZ"  (raw provider
    #            envelope string; PG timestamptz round-trip changes it)
    #   line   = str(float(abs(line)))  (hash-time code hashed abs(line);
    #            commit 0ba8f600 later re-signed stored spread lines, and
    #            str(float) renders integral lines as "45.0" not "45")
    qid_mismatch = 0
    qid_mismatch_sample = []
    for (qid, eid, bkey, mkt, sel, line, price, fp, obs) in rows:
        raw_ts = obs.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        line_s = str(float(abs(float(line))))
        recomputed = hashlib.sha256(
            ("%s|%s|%s|%s|%s|%s|%s"
             % (raw_ts, eid, bkey, mkt, sel, line_s, price)).encode()
        ).hexdigest()
        if recomputed != qid:
            qid_mismatch += 1
            if len(qid_mismatch_sample) < 5:
                qid_mismatch_sample.append(
                    {"quote_id": qid, "recomputed": recomputed,
                     "market": mkt, "selection": sel, "line": str(line),
                     "american_odds": price, "observed_at": str(obs)})
    out["quote_id_mismatches"] = qid_mismatch
    out["quote_id_mismatch_sample"] = qid_mismatch_sample
    out["quote_id_full_match"] = (qid_mismatch == 0)

    # Check 2: fair_probability re-derivation with backfill pairing logic
    groups: dict = {}
    for (qid, eid, bkey, mkt, sel, line, price, fp, obs) in rows:
        if mkt == "FULL_GAME_TOTAL":
            gkey = (obs, eid, bkey, mkt, round(float(line), 3))
        else:
            gkey = (obs, eid, bkey, mkt, abs(float(line)))
        groups.setdefault(gkey, []).append((qid, sel, float(price), float(fp)))
    fp_mismatch = 0
    fp_mismatch_sample = []
    unpaired_groups = 0
    for gkey, members in groups.items():
        # replicate pairing: totals need Over+Under, spreads need 2 sides
        sels = {m[1] for m in members}
        if gkey[3] == "FULL_GAME_TOTAL":
            if sels != {"Over", "Under"}:
                unpaired_groups += 1
                continue
        elif len(members) != 2:
            unpaired_groups += 1
            continue
        denom = sum(implied(p) for _, _, p, _ in members)
        if not denom:
            continue
        for qid, sel, price, fp in members:
            expected = implied(price) / denom
            if abs(expected - fp) > 1e-9:
                fp_mismatch += 1
                if len(fp_mismatch_sample) < 5:
                    fp_mismatch_sample.append(
                        {"quote_id": qid, "stored_fp": fp,
                         "expected_fp": expected})
    out["unpaired_groups"] = unpaired_groups
    out["fair_prob_mismatches"] = fp_mismatch
    out["fair_prob_mismatch_sample"] = fp_mismatch_sample
    out["intact"] = (out["quote_id_full_match"] and fp_mismatch == 0
                     and unpaired_groups == 0)

    dest = sys.argv[1] if len(sys.argv) > 1 else "/tmp/nfl_value_integrity.json"
    with open(dest, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps({k: v for k, v in out.items()
                      if not k.endswith("_sample")}, indent=2))


if __name__ == "__main__":
    main()
