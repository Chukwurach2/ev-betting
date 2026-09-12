"""Moneyline-pilot v1, Phase 1: feasibility sample calls.

Preregistered: docs/preregistrations/moneyline-pilot-v1.md (HARD CAP:
60 credits for this phase). Makes 3 sample historical calls
(markets=h2h, regions=us,eu) at known-good snapshot instants and evaluates
the feasibility gate:

  ALL of: HTTP 200 on every call; h2h quotes present for a majority of
  events; Pinnacle present in >= 1 sample; measured cost <= 25
  credits/call.

Otherwise verdict `infeasible` and the pilot does not run.

Writes JSON to --out. Intended to run in CI with THE_ODDS_API_KEY.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request

SPORT = "americanfootball_nfl"
BASE = "https://api.the-odds-api.com/v4"
SAMPLE_TIMES = [
    "2024-09-04T12:00:00Z",
    "2024-10-12T12:00:00Z",
    "2024-11-16T12:00:00Z",
]
MAX_CREDITS_PER_CALL = 25


def _sample(api_key, when):
    params = {
        "apiKey": api_key,
        "date": when,
        "regions": "us,eu",
        "markets": "h2h",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    url = BASE + "/historical/sports/%s/odds?%s" % (
        SPORT, urllib.parse.urlencode(params))
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.headers.items()}
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return {"requested": when, "http_status": e.code, "ok": False,
                "error": "HTTP %d" % e.code}
    try:
        env = json.loads(body)
    except ValueError:
        return {"requested": when, "http_status": status, "ok": False,
                "error": "non-JSON body"}
    data = env.get("data") if isinstance(env, dict) else None
    if not isinstance(data, list):
        return {"requested": when, "http_status": status, "ok": False,
                "error": "unexpected envelope"}
    n_events = len(data)
    n_with_h2h = 0
    books = set()
    pinnacle_events = 0
    for game in data:
        has_h2h = False
        pin_here = False
        for b in game.get("bookmakers") or []:
            key = b.get("key")
            if key:
                books.add(key)
            for m in b.get("markets") or []:
                if m.get("key") == "h2h" and (m.get("outcomes") or []):
                    has_h2h = True
                    if key == "pinnacle":
                        pin_here = True
        if has_h2h:
            n_with_h2h += 1
        if pin_here:
            pinnacle_events += 1
    try:
        cost = int(headers.get("x-requests-last", ""))
    except (TypeError, ValueError):
        cost = None
    return {
        "requested": when,
        "http_status": status,
        "ok": True,
        "returned_timestamp": env.get("timestamp"),
        "n_events": n_events,
        "n_events_with_h2h": n_with_h2h,
        "books_present": sorted(books),
        "pinnacle_events": pinnacle_events,
        "credits_last": cost,
        "credits_remaining": headers.get("x-requests-remaining"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    api_key = os.environ.get("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("error: THE_ODDS_API_KEY is required")
    samples = [_sample(api_key, w) for w in SAMPLE_TIMES]
    checks = {
        "all_http_200": all(s.get("http_status") == 200 for s in samples),
        "h2h_majority_each": all(
            s.get("ok") and s["n_events"]
            and s["n_events_with_h2h"] > s["n_events"] / 2
            for s in samples),
        "pinnacle_present": any(s.get("pinnacle_events", 0) > 0
                                for s in samples),
        "cost_within_cap": all(
            s.get("credits_last") is not None
            and s["credits_last"] <= MAX_CREDITS_PER_CALL
            for s in samples),
    }
    feasible = all(checks.values())
    result = {
        "preregistration": "docs/preregistrations/moneyline-pilot-v1.md",
        "phase": "feasibility",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "role": "SHADOW",
        "samples": samples,
        "checks": checks,
        "feasible": feasible,
        "verdict": "feasible_proceed_to_pilot" if feasible else "infeasible",
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print("verdict: %s" % result["verdict"], flush=True)
    for s in samples:
        print("  %s -> http=%s events=%s h2h=%s pinnacle_events=%s credits=%s"
              % (s["requested"], s.get("http_status"), s.get("n_events"),
                 s.get("n_events_with_h2h"), s.get("pinnacle_events"),
                 s.get("credits_last")),
              flush=True)


if __name__ == "__main__":
    main()
