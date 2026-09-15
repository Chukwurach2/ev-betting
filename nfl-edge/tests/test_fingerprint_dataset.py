"""Tests for ops/fingerprint_dataset.py (deterministic hashing via fake DB)."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import fingerprint_dataset as fd


def _quote(i, line="-3.5", prob="0.524"):
    return (
        f"qid-{i:03d}", "the_odds_api", f"ev-{i:03d}", "Home", "Away",
        "2024-09-01T12:00:00+00:00", "DraftKings", "draftkings",
        "FULL_GAME_SPREAD", "home", line, -110, prob,
        "2024-09-04T11:55:00+00:00", "historical_backfill", True,
    )


class FakeConn:
    def __init__(self, quotes, snaps):
        self.quotes = quotes
        self.snaps = snaps

    def execute(self, sql, params=()):
        quotes, snaps = self.quotes, self.snaps

        class Cur:
            def fetchall(inner):
                if "historical_quotes" in sql:
                    # simulate the SQL ORDER BY over all selected columns
                    return sorted(quotes, key=lambda r: tuple(str(v) for v in r))
                if "market_history" in sql and "COUNT" not in sql:
                    return snaps
                return []

            def fetchone(inner):
                if "COUNT" in sql and "historical_quotes" in sql:
                    return (len(quotes),)
                if "COUNT" in sql:
                    return (len(snaps),)
                return (0,)

        return Cur()


def _conn(rows):
    snaps = [("2024-09-04T12:00:00+00:00", "us,eu", "spreads,totals",
              "ps1", ["draftkings"], 40)]
    return FakeConn(rows, snaps)


class FingerprintTest(unittest.TestCase):
    def test_deterministic(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        b = fd.fingerprint(_conn(rows), "ncaaf")
        self.assertEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])
        self.assertEqual(len(a["dataset_fingerprint"]), 32)

    def test_order_independent(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        b = fd.fingerprint(_conn(list(reversed(rows))), "ncaaf")
        self.assertEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])

    def test_mutation_changes_fingerprint(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        mutated = list(rows)
        mutated[2] = _quote(2, line="-4.0")
        b = fd.fingerprint(_conn(mutated), "ncaaf")
        self.assertNotEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])

    def test_counts_reported(self):
        rows = [_quote(i) for i in range(7)]
        r = fd.fingerprint(_conn(rows), "ncaaf")
        self.assertEqual(r["n_quotes"], 7)
        self.assertEqual(r["n_snapshots"], 1)


if __name__ == "__main__":
    unittest.main()
