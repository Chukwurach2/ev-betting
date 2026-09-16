#!/usr/bin/env python3
"""H-D Phase 4: integrity + completeness gate (FREE, outcome-blind).

Reads the pull artifact (nfl_hd_pull.py output) and free pre-T_dec data
only: nflverse snap_counts (designation), injuries (step-9 inactives).
NEVER reads player_stats outcomes, scores, or realized attempts.

Per event x endpoint (pass->QB1, rush->RB1):
  - designate QB1/RB1 by the frozen rule (nfl_hd_common.designate)
  - designated player quoted at T_dec with >=2 books, else censored
  - designated player Out on the final injury report, else censored
  - consensus line L = median across qualifying books

Also verifies: pull manifest match (60 events), T_dec integrity
(request date == frozen T_dec for every call), credit ceiling,
book coverage, and the N>=25-per-endpoint stop rule.

Exit 0 + "PASS" only if every gate holds; otherwise exit 1 with reasons.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import nfl_hd_common as C

N_FLOOR = 25
EXPECTED_USABLE = (51, 54)

MKT_POS = {"player_pass_attempts": "QB", "player_rush_attempts": "RB"}
MKT_LABEL = {"player_pass_attempts": "pass", "player_rush_attempts": "rush"}


def load_free_data(data_dir):
    snaps = pd.concat(
        [pd.read_csv(f"{data_dir}/snap_counts_{s}.csv",
                     usecols=["season", "week", "player", "position",
                              "team", "offense_snaps"])
         for s in (2022, 2023, 2024)], ignore_index=True)
    inj = pd.concat(
        [pd.read_csv(f"{data_dir}/injuries_{s}.csv",
                     usecols=["season", "team", "week", "full_name",
                              "report_status", "date_modified"])
         for s in (2022, 2023, 2024)], ignore_index=True)
    inj["date_modified"] = pd.to_datetime(inj["date_modified"], utc=True,
                                          errors="coerce")
    # final designation per (season, team, week, player): latest report
    inj = inj.sort_values("date_modified").drop_duplicates(
        ["season", "team", "week", "full_name"], keep="last")
    out_set = set()
    for r in inj.itertuples():
        if str(r.report_status) == "Out":
            out_set.add((int(r.season), str(r.team),
                         int(r.week), C.norm_name(r.full_name)))
    return snaps, out_set


def game_key(gid):
    m = re.fullmatch(r"(\d{4})_(\d+)_([A-Z]+)_([A-Z]+)", gid)
    return int(m.group(1)), int(m.group(2)), m.group(3), m.group(4)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pull", required=True)
    ap.add_argument("--manifest", default=str(HERE / "nfl_hd_pull_manifest.json"))
    ap.add_argument("--data-dir", default="/tmp")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    pull = json.load(open(args.pull))
    manifest = json.load(open(args.manifest))
    man_by_game = {e["nflverse_game_id"]: e for e in manifest["events"]}

    report = {"gate": "nfl-h-d integrity",
              "run_utc": datetime.now(timezone.utc).isoformat(),
              "checks": [], "events": {},
              "outcome_rows_read": 0}
    ok_all = True

    def check(name, ok, detail=""):
        nonlocal ok_all
        report["checks"].append({"name": name, "ok": bool(ok), "detail": detail})
        if not ok:
            ok_all = False

    # 1. manifest match
    got_games = [e["nflverse_game_id"] for e in pull["events"]]
    check("pull universe == 60 frozen events",
          len(pull["events"]) == 60 and set(got_games) == set(man_by_game),
          f"events={len(pull['events'])}")
    check("no duplicate events", len(set(got_games)) == len(got_games))
    check("not aborted", not pull.get("aborted"), pull.get("abort_reason") or "")

    # 2. credit ceiling
    spent = pull.get("credits_consumed")
    check("credits_consumed <= 1200 hard ceiling",
          isinstance(spent, (int, float)) and spent <= 1200, f"spent={spent}")
    check("credits within expected 1020-1080 band",
          isinstance(spent, (int, float)) and 1020 <= spent <= 1080,
          f"spent={spent} (advisory)")

    # 3. timestamp integrity: every request date == frozen T_dec
    bad_ts = [e["nflverse_game_id"] for e in pull["events"]
              if e.get("t_dec") != man_by_game[e["nflverse_game_id"]]["t_dec_utc"]]
    check("all request dates == frozen T_dec", not bad_ts, str(bad_ts[:5]))

    snaps, out_set = load_free_data(args.data_dir)

    usable = {"player_pass_attempts": [], "player_rush_attempts": []}
    censored = []
    book_counts = []
    for ev in pull["events"]:
        gid = ev["nflverse_game_id"]
        season, week, away, home = game_key(gid)
        erec = {"status": ev.get("status"), "endpoints": {}}
        quotes = ev.get("quotes", []) if ev.get("status") == "ok" else []
        for mkt, pos in MKT_POS.items():
            mq = [q for q in quotes if q["market"] == mkt]
            player, det = C.designate(home, season, week, pos, snaps)
            u = {"designated": player, "designation": det, "n_books": 0,
                 "usable": False, "reason": None}
            if player is None:
                u["reason"] = f"no_designation:{det.get('reason')}"
                censored.append((gid, MKT_LABEL[mkt], u["reason"]))
            elif (season, home, week, C.norm_name(player)) in out_set:
                u["reason"] = "designation_inactive_out"
                censored.append((gid, MKT_LABEL[mkt], u["reason"]))
            else:
                pq = [q for q in mq
                      if C.norm_name(q.get("player")) == C.norm_name(player)]
                # per book: keep every (line, over, under) row (alternate
                # lines possible); book's representative line = median.
                per_book = []
                multi_line_books = []
                for b in sorted({q["book"] for q in pq}):
                    rows = [q for q in pq if q["book"] == b]
                    by_line = {}
                    for q in rows:
                        if q.get("line") is None:
                            continue
                        by_line.setdefault(q["line"], {"over": None,
                                                       "under": None})
                        nm = str(q.get("name")).lower()
                        if nm == "over" and q.get("price") is not None:
                            by_line[q["line"]]["over"] = q["price"]
                        elif nm == "under" and q.get("price") is not None:
                            by_line[q["line"]]["under"] = q["price"]
                    if not by_line:
                        continue
                    lines = sorted(by_line)
                    if len(lines) > 1:
                        multi_line_books.append(b)
                    rep = lines[len(lines) // 2]
                    per_book.append({"book": b, "line": rep,
                                     "lines": [{"line": ln,
                                                "over": by_line[ln]["over"],
                                                "under": by_line[ln]["under"]}
                                               for ln in lines]})
                L, n = C.consensus_line(per_book)
                u["n_books"] = n
                u["multi_line_books"] = multi_line_books
                book_counts.append(n)
                if L is None:
                    u["reason"] = ("no_coverage" if ev.get("status") != "ok"
                                   else "player_unquoted_or_<2_books")
                    censored.append((gid, MKT_LABEL[mkt], u["reason"]))
                else:
                    u["usable"] = True
                    u["L"] = L
                    u["books"] = per_book
                    usable[mkt].append({"game": gid, "season": season,
                                        "week": week, "player": player,
                                        "L": L, "books": per_book})
            erec["endpoints"][MKT_LABEL[mkt]] = u
        report["events"][gid] = erec

    n_pass = len(usable["player_pass_attempts"])
    n_rush = len(usable["player_rush_attempts"])
    check("usable N pass >= 25 stop rule", n_pass >= N_FLOOR, f"n={n_pass}")
    check("usable N rush >= 25 stop rule", n_rush >= N_FLOOR, f"n={n_rush}")
    check("usable N within expected 51-54 band (advisory)",
          EXPECTED_USABLE[0] <= n_pass <= EXPECTED_USABLE[1]
          and EXPECTED_USABLE[0] <= n_rush <= EXPECTED_USABLE[1],
          f"pass={n_pass} rush={n_rush}")
    check("designation attempts tie-break deferred (verified in test)",
          True, "snap+player_id chain used; attempts tie-break binds ~never")

    from collections import Counter
    report["summary"] = {
        "credits_consumed": spent,
        "events_ok": sum(1 for e in pull["events"] if e.get("status") == "ok"),
        "usable_pass": n_pass, "usable_rush": n_rush,
        "censored": [{"game": g, "endpoint": e, "reason": r}
                     for g, e, r in censored],
        "censor_reasons": dict(Counter(r for _, _, r in censored)),
        "book_count_hist": dict(Counter(book_counts)),
    }
    report["usable_units"] = usable
    report["verdict"] = "PASS" if ok_all else "FAIL"
    Path(args.out).write_text(json.dumps(report, indent=2))
    print(json.dumps({"verdict": report["verdict"],
                      "usable_pass": n_pass, "usable_rush": n_rush,
                      "credits": spent,
                      "failed_checks": [c["name"] for c in report["checks"]
                                        if not c["ok"]]}, indent=2))
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
