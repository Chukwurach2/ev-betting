"""Tests for ops/verify_dataset_manifest.py (pure logic via fake DB)."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "ops"))
import verify_dataset_manifest as vm


class FakeConn:
    """Minimal fake: keyed by SQL fragment."""

    def __init__(self, manifest_rows=(), inserted=None):
        self.manifest_rows = list(manifest_rows)
        self.inserted = [] if inserted is None else inserted
        self.executed_ddl = []

    def execute(self, sql, params=()):
        if "019_research_dataset_manifests" in str(
                getattr(self, "_mig", "")):
            pass
        if "research_dataset_manifests" in sql and "SELECT" in sql:
            rows = self.manifest_rows
            is_latest = "ORDER BY id DESC LIMIT 1" in sql

            class Cur:
                def fetchone(inner):
                    if is_latest:
                        scope = params[0] if params else None
                        cand = [r for r in rows if r[1] == scope]
                        return cand[-1] if cand else None
                    version, scope = (tuple(params) + (None, None))[:2]
                    for r in rows:
                        if r[0] == version and r[1] == scope:
                            return r
                    return None

                def fetchall(inner):
                    return rows

            return Cur()
        if "INSERT INTO public.research_dataset_manifests" in sql:
            self.inserted.append(params)

            class Cur2:
                def fetchone(inner):
                    return (len(self.inserted),)

            return Cur2()
        # DDL / anything else
        self.executed_ddl.append(sql)

        class Cur3:
            def fetchone(inner):
                return None

            def fetchall(inner):
                return []

        return Cur3()


def _manifest_row(version="v1", scope="nfl-2022-2024-spread-total",
                  fp="abc123", commit="deadbeef"):
    return (version, scope, fp, commit, 291586, 162,
            "2026-09-16T00:00:00+00:00", "notes")


class VerifyLogicTest(unittest.TestCase):
    def test_compare_clean(self):
        computed = {"n_quotes": 291586, "n_snapshots": 162,
                    "dataset_fingerprint": "abc123"}
        manifest = {"row_count": 291586, "snapshot_count": 162,
                    "fingerprint": "abc123"}
        self.assertEqual(vm.compare(computed, manifest), [])

    def test_compare_reports_every_mismatch(self):
        computed = {"n_quotes": 291587, "n_snapshots": 163,
                    "dataset_fingerprint": "zzz"}
        manifest = {"row_count": 291586, "snapshot_count": 162,
                    "fingerprint": "abc123"}
        mismatches = vm.compare(computed, manifest)
        self.assertEqual(len(mismatches), 3)
        self.assertTrue(any("row_count" in m for m in mismatches))
        self.assertTrue(any("snapshot_count" in m for m in mismatches))
        self.assertTrue(any("fingerprint" in m for m in mismatches))

    def test_get_manifest_row_latest(self):
        conn = FakeConn(manifest_rows=[
            _manifest_row(version="v1", fp="aaa"),
            _manifest_row(version="v2", fp="bbb"),
        ])
        row = vm.get_manifest_row(conn, "latest",
                                  "nfl-2022-2024-spread-total")
        self.assertEqual(row["version"], "v2")
        self.assertEqual(row["fingerprint"], "bbb")

    def test_get_manifest_row_exact(self):
        conn = FakeConn(manifest_rows=[_manifest_row(version="v1")])
        row = vm.get_manifest_row(conn, "v1", "nfl-2022-2024-spread-total")
        self.assertEqual(row["row_count"], 291586)
        self.assertIsNone(
            vm.get_manifest_row(conn, "v9", "nfl-2022-2024-spread-total"))

    def test_record_inserts_new_row(self):
        conn = FakeConn()
        computed = {"dataset_fingerprint": "newfp", "n_quotes": 10,
                    "n_snapshots": 2}
        rec = vm.record_manifest_row(conn, "v1", "scope-a", computed,
                                     "sha123", "note")
        self.assertEqual(rec["action"], "inserted")
        self.assertEqual(len(conn.inserted), 1)
        params = conn.inserted[0]
        self.assertEqual(params[0], "v1")
        self.assertEqual(params[2], "newfp")
        self.assertEqual(params[3], "sha123")

    def test_record_existing_same_fingerprint_is_noop(self):
        conn = FakeConn(manifest_rows=[_manifest_row(fp="same")])
        computed = {"dataset_fingerprint": "same", "n_quotes": 291586,
                    "n_snapshots": 162}
        rec = vm.record_manifest_row(conn, "v1",
                                     "nfl-2022-2024-spread-total",
                                     computed, "sha", "")
        self.assertEqual(rec["action"], "already_present")
        self.assertEqual(conn.inserted, [])

    def test_record_existing_different_fingerprint_fails(self):
        conn = FakeConn(manifest_rows=[_manifest_row(fp="old")])
        computed = {"dataset_fingerprint": "new", "n_quotes": 291586,
                    "n_snapshots": 162}
        with self.assertRaises(RuntimeError):
            vm.record_manifest_row(conn, "v1",
                                   "nfl-2022-2024-spread-total",
                                   computed, "sha", "")

    def test_known_scopes(self):
        self.assertEqual(vm.KNOWN_SCOPES["nfl-2022-2024-spread-total"],
                         ("nfl", "spread_total"))

    def test_unknown_scope_rejected(self):
        with self.assertRaises(ValueError):
            vm.run("postgres://x", "bogus-scope", "latest", False, "")


if __name__ == "__main__":
    unittest.main()
