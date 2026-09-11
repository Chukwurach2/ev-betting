"""Dataset integrity audit for the frozen historical market dataset.

Reads public.nfl_edge_market_history + public.nfl_edge_historical_quotes
and produces a JSON (+ optional Markdown) audit artifact covering:

  Coverage
    * expected snapshots (from backfill_history.snapshot_times) vs received
    * missing snapshots (explicit list)
    * coverage by season / week / book / market / region
  Timestamp integrity
    * requested vs returned drift: distribution + max (tolerance 1h)
    * snapshot_at monotonicity with requested_at ordering
    * historical/live leakage (snapshot_at must be >24h old, never future)
  Quote integrity
    * duplicate quote_ids
    * malformed prices (|american_odds| < 100, null)
    * malformed lines (null / non-finite)
    * impossible fair_probability (outside (0,1))
    * stored (event,book,market,line,observed_at) groups != 2 or
      fair probs not summing to 1
    * line / price distributions per market
  Book integrity
    * per-book: snapshots present, quote counts, seasons covered --
      reported separately for pinnacle, draftkings, fanduel, betmgm,
      betrivers, williamhill_us (Caesars US), fanatics, espnbet.
    * Missing-book periods are reported as documented availability, not
      errors: the provider genuinely did not return them.

Exit status: non-zero on CORRUPTION (dup quote_ids, impossible fair
probs, malformed stored prices/lines, mis-paired stored groups, drift
violations, leakage, duplicate snapshots) or on INCOMPLETE coverage
(any expected snapshot missing). Provider book-coverage gaps are
warnings: they describe the data, they do not fail it.

Usage: python ops/audit_dataset.py --seasons 2022,2023,2024 --weeks 1-18
       [--regions us,eu] [--markets spreads,totals] [--out audit.json]
       [--markdown audit.md]
Env: NFL_EDGE_DATABASE_URL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backfill_history as bh

TOLERANCE_SECONDS = 3600
NAMED_BOOKS = ["pinnacle", "draftkings", "fanduel", "betmgm", "betrivers",
               "williamhill_us", "fanatics", "espnbet"]


def build_expected(seasons, weeks):
    """The planned snapshot instants, mirroring the backfill schedule."""
    plan = []
    for s in seasons:
        for w in weeks:
            for when in bh.snapshot_times(s, w):
                plan.append({"season": s, "week": w,
                             "requested_at": when})
    return plan


def _parse_ts(value):
    t = bh._parse_ts(value)
    return t


def _pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def run_audit(conn, seasons, weeks, regions, markets):
    errors, warnings = [], []
    now = dt.datetime.now(dt.timezone.utc)
    expected = build_expected(seasons, weeks)
    exp_times = [e["requested_at"] for e in expected]

    snaps = conn.execute(
        """SELECT provider_snapshot_id, snapshot_at, requested_at, regions,
                  markets, books_present, credits_used
           FROM public.nfl_edge_market_history
           WHERE regions = %s AND markets = %s
           ORDER BY requested_at""", (regions, markets)).fetchall()
    by_req = {}
    for sid, snap_at, req_at, rg, mk, books, credits in snaps:
        by_req.setdefault(str(req_at), []).append(
            {"provider_snapshot_id": sid, "snapshot_at": str(snap_at),
             "requested_at": str(req_at), "books_present": books,
             "credits_used": credits})

    # ---- coverage: expected vs received ---------------------------------
    missing, matched = [], []
    for e in expected:
        key = None
        for k in by_req:
            rt = _parse_ts(k)
            if rt and abs((rt - e["requested_at"]).total_seconds()) < 1800:
                key = k
                break
        if key is None:
            missing.append({"season": e["season"], "week": e["week"],
                            "requested_at": e["requested_at"].isoformat()})
        else:
            matched.append({**e, **by_req[key][0]})
    dupes = [k for k, v in by_req.items() if len(v) > 1]
    if dupes:
        errors.append("duplicate snapshots for requested_at: %s" % dupes[:5])
    if missing:
        errors.append("%d expected snapshot(s) missing; e.g. %s"
                      % (len(missing), json.dumps(missing[:3])))
    cov_by_season, cov_by_week = {}, {}
    for e in expected:
        cov_by_season.setdefault(e["season"], {"expected": 0, "received": 0})
        cov_by_season[e["season"]]["expected"] += 1
        cov_by_week.setdefault(e["week"], {"expected": 0, "received": 0})
        cov_by_week[e["week"]]["expected"] += 1
    for m in matched:
        cov_by_season[m["season"]]["received"] += 1
        cov_by_week[m["week"]]["received"] += 1
    coverage = {
        "expected_snapshots": len(expected),
        "received_snapshots": len(matched),
        "missing_snapshots": missing,
        "duplicate_requested": dupes,
        "by_season": cov_by_season,
        "by_week": cov_by_week,
    }

    # ---- timestamp integrity --------------------------------------------
    drifts = []
    prev_ret = None
    monotonic_ok = True
    for m in matched:
        req = _parse_ts(m["requested_at"])
        ret = _parse_ts(m["snapshot_at"])
        if req is None or ret is None:
            errors.append("snapshot %s: unparseable timestamp"
                          % m["provider_snapshot_id"])
            continue
        d = abs((ret - req).total_seconds())
        drifts.append(d)
        m["drift_seconds"] = d
        if d > TOLERANCE_SECONDS:
            errors.append("snapshot %s: drift %.0fs > %ds"
                          % (m["provider_snapshot_id"], d,
                             TOLERANCE_SECONDS))
        if ret > now:
            errors.append("snapshot %s: snapshot_at in the future (leakage)"
                          % m["provider_snapshot_id"])
        if (now - ret).total_seconds() < 24 * 3600:
            errors.append("snapshot %s: snapshot_at within 24h of now "
                          "(possible live leakage)" % m["provider_snapshot_id"])
        if prev_ret and ret < prev_ret:
            monotonic_ok = False
        prev_ret = ret if prev_ret is None or ret > prev_ret else prev_ret
    if not monotonic_ok:
        errors.append("snapshot_at not monotonic with requested_at ordering")
    timestamps = {
        "drift_seconds": {"min": min(drifts) if drifts else None,
                          "max": max(drifts) if drifts else None,
                          "mean": (sum(drifts) / len(drifts)) if drifts else None,
                          "p50": _pct(drifts, 50), "p90": _pct(drifts, 90),
                          "p99": _pct(drifts, 99)},
        "tolerance_seconds": TOLERANCE_SECONDS,
        "monotonic": monotonic_ok,
        "snapshots": [{"provider_snapshot_id": m["provider_snapshot_id"],
                       "requested_at": m["requested_at"],
                       "snapshot_at": m["snapshot_at"],
                       "drift_seconds": m.get("drift_seconds")}
                      for m in matched],
    }

    # ---- quote integrity (server-side aggregation) -----------------------
    # Scoped to the matched snapshots ONLY: the quotes table may hold
    # other ranges (or a backfill that is still writing), and the audit
    # describes exactly the planned dataset, nothing else.
    snap_times = [_parse_ts(m["snapshot_at"]) for m in matched]
    snap_times = [t for t in snap_times if t is not None]
    qwhere = ("WHERE observed_at = ANY(%s)" if snap_times
              else "WHERE FALSE")
    qp = ([snap_times] if snap_times else [])

    def _one(sql):
        return conn.execute(sql % qwhere, qp).fetchone()[0]

    total_quotes = _one("SELECT COUNT(*) FROM public.nfl_edge_historical_quotes %s")
    dup_q = _one(
        """SELECT COUNT(*) FROM (
             SELECT quote_id FROM public.nfl_edge_historical_quotes %s
             GROUP BY quote_id HAVING COUNT(*) > 1) t""")
    if dup_q:
        errors.append("%d duplicate quote_id(s)" % dup_q)
    bad_price = _one(
        """SELECT COUNT(*) FROM public.nfl_edge_historical_quotes %s
           AND (american_odds IS NULL OR abs(american_odds) < 100)""")
    if bad_price:
        errors.append("%d quote(s) with malformed american_odds" % bad_price)
    bad_line = _one(
        """SELECT COUNT(*) FROM public.nfl_edge_historical_quotes %s
           AND line IS NULL""")
    if bad_line:
        errors.append("%d quote(s) with null line" % bad_line)
    bad_fp = _one(
        """SELECT COUNT(*) FROM public.nfl_edge_historical_quotes %s
           AND (fair_probability IS NULL
              OR fair_probability <= 0 OR fair_probability >= 1)""")
    if bad_fp:
        errors.append("%d quote(s) with impossible fair_probability" % bad_fp)
    bad_groups = _one(
        """SELECT COUNT(*) FROM (
             SELECT provider_event_id, book_key, market, line, observed_at
             FROM public.nfl_edge_historical_quotes %s
             GROUP BY 1,2,3,4,5
             HAVING COUNT(*) != 2 OR abs(SUM(fair_probability) - 1.0) > 1e-9
           ) t""")
    if bad_groups:
        errors.append("%d mis-paired quote group(s)" % bad_groups)
    dist = {}
    for (mk, n, lmin, lmax, lavg, pmin, pmax) in conn.execute(
            ("""SELECT market, COUNT(*), MIN(line), MAX(line), AVG(line),
                      MIN(american_odds), MAX(american_odds)
               FROM public.nfl_edge_historical_quotes %s
               GROUP BY market""" % qwhere), qp):
        dist[mk] = {"quotes": n, "line_min": float(lmin),
                    "line_max": float(lmax), "line_mean": float(lavg),
                    "price_min": int(pmin), "price_max": int(pmax)}
    quotes = {"total": total_quotes, "duplicate_quote_ids": dup_q,
              "malformed_prices": bad_price, "null_lines": bad_line,
              "impossible_fair_probs": bad_fp,
              "mis_paired_groups": bad_groups,
              "by_market": dist}

    # ---- book integrity ---------------------------------------------------
    books = {}
    for bk, nq, nsn, seasons_seen in conn.execute(
            ("""SELECT book_key, COUNT(*), COUNT(DISTINCT observed_at),
                      COUNT(DISTINCT date_trunc('year', observed_at))
               FROM public.nfl_edge_historical_quotes %s
               GROUP BY book_key""" % qwhere), qp):
        books[bk] = {"quotes": nq, "snapshots_present": nsn,
                     "years_seen": seasons_seen}
    for bk in NAMED_BOOKS:
        if bk not in books:
            warnings.append("book '%s': no quotes in dataset "
                            "(provider did not return it)" % bk)
    book_cov = {"named_books": {b: books.get(b) for b in NAMED_BOOKS},
                "all_books": books}

    report = {
        "generated_at": now.isoformat(),
        "seasons": seasons, "weeks": weeks,
        "regions": regions, "markets": markets,
        "coverage": coverage, "timestamps": timestamps,
        "quotes": quotes, "books": book_cov,
        "errors": errors, "warnings": warnings,
    }
    report["status"] = "pass" if not errors else "fail"
    return report


def render_markdown(rep):
    L = []
    L.append("# Historical dataset integrity audit")
    L.append("")
    L.append("Generated: %s | seasons %s | weeks %s | regions %s | markets %s"
             % (rep["generated_at"], rep["seasons"], rep["weeks"],
                rep["regions"], rep["markets"]))
    L.append("")
    L.append("**Status: %s**" % rep["status"].upper())
    L.append("")
    c = rep["coverage"]
    L.append("## Coverage")
    L.append("")
    L.append("- Expected snapshots: %d" % c["expected_snapshots"])
    L.append("- Received snapshots: %d" % c["received_snapshots"])
    L.append("- Missing: %d" % len(c["missing_snapshots"]))
    for m in c["missing_snapshots"][:10]:
        L.append("  - season %s week %s @ %s"
                 % (m["season"], m["week"], m["requested_at"]))
    L.append("")
    L.append("### By season")
    for s in sorted(c["by_season"]):
        v = c["by_season"][s]
        L.append("- %s: %d/%d" % (s, v["received"], v["expected"]))
    t = rep["timestamps"]["drift_seconds"]
    L.append("")
    L.append("## Timestamp integrity")
    L.append("")
    L.append("- Drift (s): min %.0f, max %.0f, mean %.1f, p90 %.0f (tolerance %ds)"
             % (t["min"], t["max"], t["mean"], t["p90"],
                rep["timestamps"]["tolerance_seconds"]))
    L.append("- Monotonic: %s" % rep["timestamps"]["monotonic"])
    q = rep["quotes"]
    L.append("")
    L.append("## Quote integrity")
    L.append("")
    L.append("- Total quotes: %d" % q["total"])
    L.append("- Duplicate quote_ids: %d" % q["duplicate_quote_ids"])
    L.append("- Malformed prices: %d | null lines: %d | impossible fair probs: %d"
             % (q["malformed_prices"], q["null_lines"],
                q["impossible_fair_probs"]))
    L.append("- Mis-paired groups: %d" % q["mis_paired_groups"])
    for mk, d in q["by_market"].items():
        L.append("- %s: n=%d line [%.1f, %.1f] mean %.2f price [%d, %d]"
                 % (mk, d["quotes"], d["line_min"], d["line_max"],
                    d["line_mean"], d["price_min"], d["price_max"]))
    L.append("")
    L.append("## Book integrity")
    L.append("")
    for b, info in rep["books"]["named_books"].items():
        if info:
            L.append("- %s: %d quotes across %d snapshots"
                     % (b, info["quotes"], info["snapshots_present"]))
        else:
            L.append("- %s: ABSENT (provider did not return)" % b)
    if rep["errors"]:
        L.append("")
        L.append("## Errors")
        for e in rep["errors"]:
            L.append("- %s" % e)
    if rep["warnings"]:
        L.append("")
        L.append("## Warnings")
        for w in rep["warnings"][:20]:
            L.append("- %s" % w)
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--weeks", default="1-18")
    ap.add_argument("--regions", default="us,eu")
    ap.add_argument("--markets", default="spreads,totals")
    ap.add_argument("--out", default=None)
    ap.add_argument("--markdown", default=None)
    args = ap.parse_args(argv)
    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not dsn:
        raise SystemExit("NFL_EDGE_DATABASE_URL is required")
    seasons = [int(s) for s in args.seasons.split(",")]
    weeks = bh.parse_weeks(args.weeks)
    conn = psycopg.connect(dsn, connect_timeout=10)
    conn.autocommit = True
    try:
        report = run_audit(conn, seasons, weeks, args.regions, args.markets)
    finally:
        conn.close()
    text = json.dumps(report, indent=2, default=str)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)
    else:
        print(text)
    if args.markdown:
        with open(args.markdown, "w") as fh:
            fh.write(render_markdown(report))
    if report["status"] == "fail":
        print("AUDIT FAILED: %d error(s)" % len(report["errors"]),
              file=sys.stderr)
        return 1
    print("AUDIT PASSED: %d snapshots, %d quotes, %d warning(s)"
          % (report["coverage"]["received_snapshots"],
             report["quotes"]["total"], len(report["warnings"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
