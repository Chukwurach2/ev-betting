"""Tests for ops/fingerprint_dataset.py (canonical-1 serialization).

The serialization tests pin the canonical rendering byte-for-byte, so a
future driver or refactor cannot silently change the fingerprint.
"""
import hashlib
import pathlib
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import fingerprint_dataset as fd


def _quote(i, market="FULL_GAME_SPREAD", line=Decimal("-3.5"), prob=Decimal("0.524")):
    return (
        f"qid-{i:03d}", "the_odds_api", f"ev-{i:03d}", "Home", "Away",
        datetime(2024, 9, 1, 12, 0, 0, tzinfo=timezone.utc), "DraftKings",
        "draftkings", market, "home", line, -110, prob,
        datetime(2024, 9, 4, 11, 55, 38, tzinfo=timezone.utc),
        "historical_backfill", True,
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
                    rows = quotes
                    # scope filter is inlined as frozen literals
                    if "FULL_GAME_SPREAD" in sql:
                        rows = [r for r in rows
                                if r[8] in ("FULL_GAME_SPREAD",
                                            "FULL_GAME_TOTAL")]
                    return rows
                if "market_history" in sql and "COUNT" not in sql:
                    return snaps
                return []

            def fetchone(inner):
                if "COUNT" in sql and "historical_quotes" in sql:
                    rows = quotes
                    if "FULL_GAME_SPREAD" in sql:
                        rows = [r for r in rows
                                if r[8] in ("FULL_GAME_SPREAD",
                                            "FULL_GAME_TOTAL")]
                    return (len(rows),)
                if "COUNT" in sql:
                    return (len(snaps),)
                return (0,)

        return Cur()


def _conn(rows):
    snaps = [(datetime(2024, 9, 4, 12, 0, tzinfo=timezone.utc), "us,eu",
              "spreads,totals", "ps1", ["draftkings", "pinnacle"], 40)]
    return FakeConn(rows, snaps)


class FingerprintTest(unittest.TestCase):
    def test_deterministic(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        b = fd.fingerprint(_conn(rows), "ncaaf")
        self.assertEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])
        self.assertEqual(len(a["dataset_fingerprint"]), 64)

    def test_order_independent(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        b = fd.fingerprint(_conn(list(reversed(rows))), "ncaaf")
        self.assertEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])

    def test_mutation_changes_fingerprint(self):
        rows = [_quote(i) for i in range(5)]
        a = fd.fingerprint(_conn(rows), "ncaaf")
        mutated = list(rows)
        mutated[2] = _quote(2, line=Decimal("-4.0"))
        b = fd.fingerprint(_conn(mutated), "ncaaf")
        self.assertNotEqual(a["dataset_fingerprint"], b["dataset_fingerprint"])

    def test_counts_reported(self):
        rows = [_quote(i) for i in range(7)]
        r = fd.fingerprint(_conn(rows), "ncaaf")
        self.assertEqual(r["n_quotes"], 7)
        self.assertEqual(r["n_snapshots"], 1)

    # ---- canonical-1 serialization pins ---------------------------------

    def test_render_timestamps(self):
        self.assertEqual(
            fd.render_value(datetime(2024, 9, 4, 11, 55, 38,
                                     tzinfo=timezone.utc)),
            "2024-09-04T11:55:38Z")
        # naive datetimes are assumed UTC
        self.assertEqual(
            fd.render_value(datetime(2024, 9, 4, 11, 55, 38)),
            "2024-09-04T11:55:38Z")
        # fractional seconds only when nonzero, trailing zeros stripped
        self.assertEqual(
            fd.render_value(datetime(2024, 9, 4, 11, 55, 38, 123456,
                                     tzinfo=timezone.utc)),
            "2024-09-04T11:55:38.123456Z")
        self.assertEqual(
            fd.render_value(datetime(2024, 9, 4, 11, 55, 38, 123000,
                                     tzinfo=timezone.utc)),
            "2024-09-04T11:55:38.123Z")

    def test_render_numerics(self):
        self.assertEqual(fd.render_value(Decimal("45.0")), "45")
        self.assertEqual(fd.render_value(Decimal("3.50")), "3.5")
        self.assertEqual(fd.render_value(Decimal("-3.5")), "-3.5")
        self.assertEqual(fd.render_value(45.0), "45")   # float agrees
        self.assertEqual(fd.render_value(3.5), "3.5")
        self.assertEqual(fd.render_value(-110), "-110")  # int
        self.assertEqual(fd.render_value(Decimal("0.524")), "0.524")

    def test_render_null_and_bool(self):
        # NULL is a NUL sentinel: distinct from "" and from any text.
        self.assertEqual(fd.render_value(None), "\x00")
        self.assertNotEqual(fd.render_value(None), fd.render_value(""))
        # bool is checked before int (Python bool subclasses int).
        self.assertEqual(fd.render_value(True), "true")
        self.assertEqual(fd.render_value(False), "false")

    def test_render_list(self):
        self.assertEqual(fd.render_value(["pinnacle", "draftkings"]),
                         "draftkings,pinnacle")

    def test_byte_exact_preimage(self):
        # Hand-build the expected preimage for two rows and pin the hash.
        rows = [_quote(0), _quote(1)]
        expected_lines = []
        for r in rows:
            rendered = (
                "qid-000" if r[0] == "qid-000" else "qid-001",
                "the_odds_api", r[2], "Home", "Away",
                "2024-09-01T12:00:00Z", "DraftKings", "draftkings",
                "FULL_GAME_SPREAD", "home", "-3.5", "-110", "0.524",
                "2024-09-04T11:55:38Z", "historical_backfill", "true",
            )
            expected_lines.append("|".join(rendered) + "\n")
        expected_lines.sort()
        expected = hashlib.sha256(
            "".join(expected_lines).encode("utf-8")).hexdigest()
        got = fd.fingerprint(_conn(rows), "ncaaf")["dataset_fingerprint"]
        self.assertEqual(got, expected)

    # ---- market scope ----------------------------------------------------

    def test_market_scope_default_all(self):
        rows = [_quote(0, market="FULL_GAME_SPREAD"),
                _quote(1, market="FULL_GAME_MONEYLINE")]
        r = fd.fingerprint(_conn(rows), "nfl")
        self.assertEqual(r["market_scope"], "all")
        self.assertEqual(r["n_quotes"], 2)

    def test_market_scope_spread_total(self):
        rows = [_quote(0, market="FULL_GAME_SPREAD"),
                _quote(1, market="FULL_GAME_TOTAL"),
                _quote(2, market="FULL_GAME_MONEYLINE")]
        r = fd.fingerprint(_conn(rows), "nfl", market_scope="spread_total")
        self.assertEqual(r["market_scope"], "spread_total")
        self.assertEqual(r["n_quotes"], 2)
        # equals the fingerprint over the two in-scope rows alone
        r2 = fd.fingerprint(_conn(rows[:2]), "nfl",
                            market_scope="spread_total")
        self.assertEqual(r["dataset_fingerprint"],
                         r2["dataset_fingerprint"])
        # and differs from the unscoped fingerprint
        r3 = fd.fingerprint(_conn(rows), "nfl")
        self.assertNotEqual(r["dataset_fingerprint"],
                            r3["dataset_fingerprint"])

    def test_market_scope_invalid(self):
        with self.assertRaises(ValueError):
            fd.fingerprint(_conn([]), "nfl", market_scope="bogus")


if __name__ == "__main__":
    unittest.main()
