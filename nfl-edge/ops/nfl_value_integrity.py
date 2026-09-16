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

    # Check 1: quote_id recomputation.
    # The backfill hashed the RAW provider timestamp string
    # (envelope["timestamp"], e.g. "2024-09-04T11:55:38.123456Z"), not the
    # parsed timestamptz. The exact raw rendering is unknowable per se, so
    # try candidate renderings per distinct snapshot and see which (if any)
    # verifies every row of that snapshot.
    def candidates(obs):
        base = obs.strftime("%Y-%m-%dT%H:%M:%S")
        frac6 = obs.strftime(".%f") if obs.microsecond else ""
        frac3 = obs.strftime(".%f")[:4] if obs.microsecond else ""
        cands = [
            base + frac6 + "Z",
            base + frac3 + "Z",
            base + "Z",
            base + frac6 + "+00:00",
            base + frac3 + "+00:00",
            base + "+00:00",
            str(obs),
        ]
        # dedupe, preserve order
        seen = []
        for c in cands:
            if c not in seen:
                seen.append(c)
        return seen

    snaps: dict = {}
    for (qid, eid, bkey, mkt, sel, line, price, fp, obs) in rows:
        snaps.setdefault(obs, []).append((qid, eid, bkey, mkt, sel, line, price))
    snap_results = []
    total_matched = 0
    for obs, srows in sorted(snaps.items(), key=lambda kv: str(kv[0])):
        best = None
        for cand in candidates(obs):
            m = 0
            for (qid, eid, bkey, mkt, sel, line, price) in srows:
                h = hashlib.sha256(
                    ("%s|%s|%s|%s|%s|%s|%s"
                     % (cand, eid, bkey, mkt, sel, line, price)).encode()
                ).hexdigest()
                if h == qid:
                    m += 1
            if best is None or m > best[1]:
                best = (cand, m)
        snap_results.append(
            {"observed_at": str(obs), "n": len(srows),
             "best_candidate": best[0], "matched": best[1]})
        total_matched += best[1]
    out["snapshots"] = len(snaps)
    out["quote_id_matched"] = total_matched
    out["quote_id_total"] = len(rows)
    out["quote_id_full_match"] = (total_matched == len(rows))
    out["snapshots_not_fully_matched"] = [
        s for s in snap_results if s["matched"] != s["n"]
    ]
    out["candidate_histogram"] = {}
    for s in snap_results:
        if s["matched"] == s["n"]:
            # normalize candidate to a pattern label
            c = s["best_candidate"]
            label = ("frac6Z" if ".%f" not in c and "T" in c and c.endswith("Z")
                     and "." in c else c)
            out["candidate_histogram"][s["best_candidate"][:26]] = \
                out["candidate_histogram"].get(s["best_candidate"][:26], 0) + 1

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
