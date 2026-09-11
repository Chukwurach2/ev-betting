import pathlib
import unittest

MIGRATIONS = pathlib.Path(__file__).parents[1] / "ops" / "migrations"
FORBIDDEN = ("DROP TABLE", "DROP DATABASE", "TRUNCATE", "DELETE FROM")


class MigrationTests(unittest.TestCase):
    def test_migrations_are_additive_only(self):
        files = sorted(MIGRATIONS.glob("*.sql"))
        self.assertTrue(files, "expected at least one migration file")
        for path in files:
            sql = path.read_text().upper()
            for token in FORBIDDEN:
                self.assertNotIn(token, sql, f"{path.name} must stay additive")

    def test_quote_history_migration_shape(self):
        sql = (MIGRATIONS / "002_quote_history.sql").read_text()
        for clause in (
            "CREATE TABLE IF NOT EXISTS public.nfl_edge_odds_quotes",
            "quote_id text PRIMARY KEY",
            "fair_probability",
            "observed_at timestamptz NOT NULL",
            "ENABLE ROW LEVEL SECURITY",
            "nfl_edge_fanout_quotes",
            "AFTER INSERT OR UPDATE",
            "ON CONFLICT (quote_id) DO NOTHING",
        ):
            self.assertIn(clause, sql, f"002 migration missing: {clause}")

    def test_picks_migration_shape(self):
        sql = (MIGRATIONS / "003_picks.sql").read_text()
        for clause in (
            "CREATE TABLE IF NOT EXISTS public.nfl_edge_picks",
            "pick_id text PRIMARY KEY",
            "engine_version text NOT NULL",
            "CHECK (mode IN ('shadow', 'challenger', 'production'))",
            "consensus_fair_prob",
            "kelly_fraction",
            "stake_units",
            "ENABLE ROW LEVEL SECURITY",
        ):
            self.assertIn(clause, sql, f"003 migration missing: {clause}")

    def test_settlement_migration_shape(self):
        sql = (MIGRATIONS / "004_settlement.sql").read_text()
        for clause in (
            "ALTER TABLE public.nfl_edge_picks",
            "final_home_score",
            "clv_prob_points",
            "settled_by",
        ):
            self.assertIn(clause, sql, f"004 migration missing: {clause}")


if __name__ == "__main__":
    unittest.main()
