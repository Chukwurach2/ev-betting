"""Fixed descriptive NFL rematch calibration; never executable-price returns."""
import argparse
import csv
import hashlib
import io
import json
import math
import random
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

EXPOSED = frozenset(("IL4EwN7l", "fH8rHjXi"))
MINIMUMS = (30, 80, 8)


def integer(value):
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d != d.to_integral_value():
            raise ValueError("noninteger")
        return int(d)
    except (InvalidOperation, TypeError):
        raise ValueError("invalid integer") from None


def audit_schedule(rows, expected=(272, 32, 17)):
    """Read ONLY schedule/identity fields; scores/probabilities cannot affect labels."""
    games, ids, starts, names, reverse, counts, pairs = [], set(), set(), {}, {}, Counter(), Counter()
    for row in rows:
        if integer(row["season"]) != 2025:
            raise ValueError("unexpected season")
        if row["season_type"] == "postseason":
            continue
        if row["season_type"] != "regular":
            raise ValueError("unknown season type")
        game_id = row["game_id"]
        if not game_id or game_id in ids:
            raise ValueError("duplicate/missing game ID")
        ids.add(game_id)
        at = datetime.fromisoformat(row["start_time_utc"].replace("Z", "+00:00"))
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("timezone required")
        if at.astimezone(ZoneInfo("America/New_York")).date().isoformat() != row["date_et"]:
            raise ValueError("Eastern date mismatch")
        week = integer(row["week"])
        if not 1 <= week <= 18:
            raise ValueError("invalid week")
        home, away = row["home_team_id"], row["away_team_id"]
        if not home or not away or home == away:
            raise ValueError("invalid team IDs")
        for side, team in (("home", home), ("away", away)):
            name = row[side + "_team"]
            if not name or names.get(team, name) != name or reverse.get(name, team) != team:
                raise ValueError("team ID/name conflict")
            names[team], reverse[name] = name, team
            key = (team, at)
            if key in starts:
                raise ValueError("duplicate team start")
            starts.add(key)
            counts[team] += 1
        pair = tuple(sorted((home, away)))
        pairs[pair] += 1
        if pairs[pair] > 2:
            raise ValueError("more than two pair meetings")
        games.append(dict(game_id=game_id, at=at, week=week, pair=pair))
    if len(games) != expected[0] or len(counts) != expected[1] or set(counts.values()) != {expected[2]}:
        raise ValueError("incomplete regular-season coverage")
    seen, labels = set(), {}
    for game in sorted(games, key=lambda x: (x["at"], x["game_id"])):
        labels[game["game_id"]] = dict(week=game["week"], treated=game["pair"] in seen)
        seen.add(game["pair"])
    return dict(regular_games=len(games), teams=len(counts), games_per_team=expected[2],
                postseason_excluded=len(rows) - len(games), labels=labels,
                treated_schedule=sum(x["treated"] for x in labels.values()))


def percentile(values, q):
    pos = (len(values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)


def bootstrap(weeks, attempts=20000, seed=20261104):
    rng, draws, skipped = random.Random(seed), [], 0
    for _ in range(attempts):
        sample = [(start + offset) % 18 + 1 for start in [rng.randrange(18) for _ in range(6)] for offset in range(3)]
        denominator = sum(weeks[w]["weight"] for w in sample)
        if not denominator:
            skipped += 1
            continue
        draws.append(sum(weeks[w]["weighted_contrast"] for w in sample) / denominator)
    draws.sort()
    return dict(valid_draws=len(draws), skipped_zero_weight=skipped,
                interval=[percentile(draws, .025), percentile(draws, .975)] if draws else None)


def evaluate(rows, audit, minimums=MINIMUMS):
    groups = {w: {True: [], False: []} for w in range(1, 19)}
    excluded = Counter()
    for row in rows:
        if row["season_type"] != "regular":
            continue
        p = float(row["close_home_win_prob"])
        home, away = integer(row["home_score"]), integer(row["away_score"])
        if not math.isfinite(p) or not 0 < p < 1 or min(home, away) < 0:
            raise ValueError("invalid probability/score")
        if row["game_id"] in EXPOSED:
            excluded["search_exposed"] += 1
            continue
        if p == .5:
            excluded["even_probability"] += 1
            continue
        if home == away:
            excluded["tied_final"] += 1
            continue
        label = audit["labels"][row["game_id"]]
        favorite_p = p if p > .5 else 1 - p
        win = int((home > away) == (p > .5))
        groups[label["week"]][label["treated"]].append((win, favorite_p))
    weeks, comparable, t_count, c_count = {}, [], 0, 0
    totals = {True: [], False: []}
    for week in range(1, 19):
        t, c = groups[week][True], groups[week][False]
        totals[True].extend(t)
        totals[False].extend(c)
        contrast = sum(w - p for w, p in t) / len(t) - sum(w - p for w, p in c) / len(c) if t and c else None
        weight = len(t) if contrast is not None else 0
        weeks[week] = dict(treated=len(t), controls=len(c), contrast=contrast, weight=weight,
                           weighted_contrast=weight * contrast if weight else 0)
        if weight:
            comparable.append(week)
            t_count += len(t)
            c_count += len(c)
    def summary(xs):
        return dict(games=len(xs), wins=sum(w for w, _ in xs),
                    mean_probability=sum(p for _, p in xs) / len(xs) if xs else None,
                    mean_residual=sum(w - p for w, p in xs) / len(xs) if xs else None,
                    brier=sum((w - p) ** 2 for w, p in xs) / len(xs) if xs else None)
    report = dict(id="nfl-rematch-favorite-calibration-v1", status="insufficient_data", exclusions=dict(excluded),
                  treated=summary(totals[True]), controls=summary(totals[False]), weeks=weeks,
                  comparable_weeks=comparable, comparable_treated=t_count, comparable_controls=c_count,
                  primary_contrast=None, uncertainty=None, inference="descriptive development only; no ROI/CLV/pick")
    if t_count < minimums[0] or c_count < minimums[1] or len(comparable) < minimums[2]:
        return report
    contrast = sum(w["weighted_contrast"] for w in weeks.values()) / t_count
    uncertainty = bootstrap(weeks)
    report.update(primary_contrast=contrast, uncertainty=uncertainty)
    if uncertainty["valid_draws"] < 19000:
        report["status"] = "insufficient_bootstrap_draws"
    else:
        report["status"] = "descriptive_lead" if contrast <= -.03 and uncertainty["interval"][1] < 0 else "not_supported"
    return report


def exclusive_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("x", encoding="utf8") as stream:
        path.chmod(0o600)
        json.dump(data, stream, indent=2, allow_nan=False)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("private_output_directory", type=Path)
    args = parser.parse_args()
    raw = args.source.read_bytes()
    exclusive_json(args.private_output_directory / "source-receipt.json", dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    audit = audit_schedule(rows)
    exclusive_json(args.private_output_directory / "schedule-audit.json", audit)
    report = evaluate(rows, audit)
    exclusive_json(args.private_output_directory / "report.json", report)
    print(json.dumps({k: report[k] for k in ("id", "status", "primary_contrast", "uncertainty")}))


if __name__ == "__main__":
    main()
