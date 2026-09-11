"""Tests for the append-only experiment ledger (ledger.py)."""
import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from model.research.ledger import (LEDGER_VERSION, make_entry, query,
                                   read_all, record)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "ledger", "experiments.jsonl")

    def _entry(self, family="elo-v1", version="v1", status="rejected"):
        return make_entry(family, version,
                          metrics={"spread": {"roi": -0.041}},
                          status=status,
                          train_window=[1999, 2009],
                          val_window=[2010, 2024],
                          hyperparameters={"k": 0.15},
                          commit_sha="abc123")

    def test_two_records_append_two_lines(self):
        record(self.path, self._entry())
        record(self.path, self._entry(version="v2"))
        with open(self.path, encoding="utf-8") as f:
            lines = [ln for ln in f if ln.strip()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(len(read_all(self.path)), 2)

    def test_record_never_mutates_existing_lines(self):
        record(self.path, self._entry())
        with open(self.path, encoding="utf-8") as f:
            first_before = f.readline()
        record(self.path, self._entry(version="v2", status="candidate"))
        with open(self.path, encoding="utf-8") as f:
            first_after = f.readline()
        self.assertEqual(first_before, first_after)
        self.assertIn('"version": "v1"', first_after)

    def test_query_filters(self):
        record(self.path, self._entry(family="a", status="rejected"))
        record(self.path, self._entry(family="b", status="candidate"))
        record(self.path, self._entry(family="a", status="candidate"))
        self.assertEqual(len(query(self.path, family="a")), 2)
        self.assertEqual(len(query(self.path, status="candidate")), 2)
        self.assertEqual(len(query(self.path, family="a",
                                   status="candidate")), 1)
        self.assertEqual(len(query(self.path)), 3)

    def test_entry_gets_timestamp_and_version(self):
        e = self._entry()
        stored = record(self.path, e)
        self.assertIn("recorded_at", stored)
        self.assertEqual(stored["ledger_version"], LEDGER_VERSION)
        # Input dict is not mutated.
        self.assertNotIn("recorded_at", e)
        self.assertNotIn("ledger_version", e)

    def test_make_entry_defaults(self):
        e = make_entry("f", "v1", {"roi": 0.0}, "research")
        self.assertEqual(e["hyperparameters"], {})
        self.assertEqual(e["metrics"], {"roi": 0.0})
        self.assertEqual(e["feature_set_hash"], "")
        self.assertEqual(e["commit_sha"], "")

    def test_read_missing_file(self):
        self.assertEqual(read_all(os.path.join(self.tmp, "nope.jsonl")), [])

    def test_round_trip_preserves_metrics(self):
        record(self.path, self._entry())
        e = read_all(self.path)[0]
        self.assertEqual(e["metrics"], {"spread": {"roi": -0.041}})
        self.assertEqual(e["hyperparameters"], {"k": 0.15})
        self.assertEqual(e["train_window"], [1999, 2009])


if __name__ == "__main__":
    unittest.main()
