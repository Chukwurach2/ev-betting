#!/usr/bin/env python3
"""Candidate #2: Pinnacle incremental information test.

Question: Does Pinnacle's Wed->Fri totals change predict Fri->Sat consensus
movement BEYOND what's already in non-Pinnacle consensus change and the
candidate #1 pressure signal?

Regression: dC2 ~ b0 + b1*dP + b2*dC + b3*pressure_Fri

Advancement requires b1 significant after controls. Otherwise close.

Preregistered: nfl-edge/docs/ncaaf-investment-card-pinnacle-incremental.md
Prior: LOW. Burned 2022-24. Zero API credits. 2025 sealed.
"""
import os, sys, json, argparse, math, statistics
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sports  # noqa: E402
from market_structure import build_plan, match_window, FROZEN_FINGERPRINT

WINDOWS = ["early", "mid", "late"]  # Wed, Fri, Sat
PINNACLE_KEYS = {"pinnacle"}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="ncaaf")
    ap.add_argument("--seasons", default="2022,2023,2024")
    ap.add_argument("--out", default="/tmp/pinnacle_incremental.json")
    args = ap.parse_args(argv)

    seasons = [int(s) for s in args.seasons.split(",")]
    assert 2025 not in seasons

    import psycopg
    dsn = os.environ.get("NFL_EDGE_DATABASE_URL")
    hq = sports.historical_quotes_table(args.sport)
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(f"""
            SELECT provider_event_id, book_key, selection, line,
                   fair_probability, observed_at
            FROM public.{hq}
            WHERE market = 'FULL_GAME_TOTAL'
              AND fair_probability IS NOT NULL
        """).fetchall()

    plan = build_plan(args.sport, seasons)
    # (event, season, week, window) -> {"pinnacle": [...], "others": [...]}
    # Each list: (book, line, selection, fair_prob)
    events = defaultdict(lambda: {"pinnacle": [], "others": []})
    for r in rows:
        eid, book, sel, line, fp, obs = r
        t = match_window(obs, plan)
        if t is None:
            continue
        season, week, window = plan[t]
        if season not in seasons:
            continue
        key = (eid, season, week, window)
        entry = (book, float(line), sel, float(fp))
        if book.lower() in PINNACLE_KEYS:
            events[key]["pinnacle"].append(entry)
        else:
            events[key]["others"].append(entry)

    # Build regression dataset
    # For each (event, season, week) with all three windows:
    data = []  # (dP, dC, pressure_Fri, dC2)
    for (eid, season, week, window), d in events.items():
        if window != "early":
            continue
        # Need mid and late too
        mid = events.get((eid, season, week, "mid"))
        late = events.get((eid, season, week, "late"))
        if not mid or not late:
            continue

        def consensus(entries, sel="Over"):
            lines = [ln for _, ln, s, _ in entries if s == sel]
            return statistics.median(lines) if lines else None

        def n_books(entries):
            return len({b for b, _, _, _ in entries})

        # Pinnacle Wed->Fri change
        p_early = consensus(d["pinnacle"])
        p_mid = consensus(mid["pinnacle"])
        if p_early is None or p_mid is None:
            continue
        dP = p_mid - p_early

        # Non-Pinnacle consensus Wed->Fri change
        c_early = consensus(d["others"])
        c_mid = consensus(mid["others"])
        c_late = consensus(late["others"])
        if c_early is None or c_mid is None or c_late is None:
            continue
        if n_books(mid["others"]) < 3:
            continue
        dC = c_mid - c_early
        dC2 = c_late - c_mid

        # Candidate #1 pressure at Fri (control)
        over_mid = [fp for _, ln, s, fp in mid["others"]
                    if s == "Over" and abs(ln - c_mid) < 0.01]
        if len(over_mid) < 3:
            continue
        pressure_fri = statistics.fmean(over_mid) - 0.5

        data.append((dP, dC, pressure_fri, dC2))

    n = len(data)
    if n < 50:
        result = {"n": n, "insufficient": True}
    else:
        # OLS: dC2 ~ b0 + b1*dP + b2*dC + b3*pressure
        # Using normal equations
        import math
        # Design matrix with intercept
        X = [[1, dp, dc, pr] for dp, dc, pr, _ in data]
        y = [d[3] for d in data]

        # X'X and X'y
        p = 4
        XtX = [[sum(X[i][a] * X[i][b] for i in range(n))
                for b in range(p)] for a in range(p)]
        Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(p)]

        # Solve via Gaussian elimination
        def solve(A, b):
            n_ = len(A)
            M = [row[:] + [b[i]] for i, row in enumerate(A)]
            for col in range(n_):
                # Pivot
                piv = max(range(col, n_), key=lambda r: abs(M[r][col]))
                M[col], M[piv] = M[piv], M[col]
                pv = M[col][col]
                if abs(pv) < 1e-12:
                    return None
                for r in range(n_):
                    if r != col:
                        f = M[r][col] / pv
                        for c in range(col, n_ + 1):
                            M[r][c] -= f * M[col][c]
            return [M[i][n_] / M[i][i] for i in range(n_)]

        beta = solve(XtX, Xty)
        if beta is None:
            result = {"n": n, "singular": True}
        else:
            # Residuals and SE
            yhat = [sum(X[i][j] * beta[j] for j in range(p)) for i in range(n)]
            resid = [y[i] - yhat[i] for i in range(n)]
            sse = sum(r * r for r in resid)
            df = n - p
            mse = sse / df if df > 0 else 0

            # Var(beta) = mse * inv(X'X)
            # Invert XtX
            def invert(A):
                n_ = len(A)
                M = [row[:] + [1 if i == j else 0 for j in range(n_)]
                     for i, row in enumerate(A)]
                for col in range(n_):
                    piv = max(range(col, n_), key=lambda r: abs(M[r][col]))
                    M[col], M[piv] = M[piv], M[col]
                    pv = M[col][col]
                    if abs(pv) < 1e-12:
                        return None
                    for c in range(2 * n_):
                        M[col][c] /= pv
                    for r in range(n_):
                        if r != col:
                            f = M[r][col]
                            for c in range(2 * n_):
                                M[r][c] -= f * M[col][c]
                return [row[n_:] for row in M]

            inv = invert(XtX)
            if inv is None:
                result = {"n": n, "singular": True}
            else:
                se = [math.sqrt(mse * inv[j][j]) if inv[j][j] > 0 else float('inf')
                      for j in range(p)]
                tstats = [beta[j] / se[j] if se[j] not in (0, float('inf')) else 0
                          for j in range(p)]
                # Two-sided p-value approx (normal)
                def pval(t):
                    # |t| > 1.96 => p < 0.05 (normal approx)
                    import math
                    return 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
                pvals = [pval(t) for t in tstats]

                names = ["intercept", "dP_pinnacle", "dC_consensus", "pressure_Fri"]
                result = {
                    "n": n,
                    "coefficients": {
                        names[j]: {
                            "beta": round(beta[j], 5),
                            "se": round(se[j], 5),
                            "t": round(tstats[j], 3),
                            "p": round(pvals[j], 4),
                        } for j in range(p)
                    },
                    "r_squared": round(1 - sse / sum((yi - sum(y)/n)**2 for yi in y), 4),
                    "verdict": (
                        "ADVANCE" if pvals[1] < 0.05 and abs(beta[1]) > 0.05
                        else "CLOSE"
                    ),
                    "verdict_reason": (
                        f"b1 (Pinnacle) p={pvals[1]:.4f}, |beta|={abs(beta[1]):.4f}. "
                        "Requires p<0.05 and |beta|>0.05 for advancement."
                    ),
                }

    result["preregistration"] = "nfl-edge/docs/ncaaf-investment-card-pinnacle-incremental.md"
    result["dataset_fingerprint"] = FROZEN_FINGERPRINT
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
