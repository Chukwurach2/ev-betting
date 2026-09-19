"""Apply additive SQL migrations in order. Safe to run on every collector invocation.

Only executes files in ops/migrations/ ending in .sql, sorted by name.
Each migration must be idempotent (IF NOT EXISTS / OR REPLACE) and additive:
no destructive statements are allowed.
"""
import os
import pathlib
import sys

FORBIDDEN = ("DROP TABLE", "DROP DATABASE", "TRUNCATE", "DELETE FROM")


def main() -> int:
    database = os.environ.get("NFL_EDGE_DATABASE_URL")
    if not database:
        print("NFL_EDGE_DATABASE_URL is not set; skipping migrations")
        return 1
    import psycopg

    migrations_dir = pathlib.Path(__file__).parent / "migrations"
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        print("no migration files found")
        return 1
    applied = []
    with psycopg.connect(database, connect_timeout=10) as connection:
        # 180s: idempotent DDL must out-wait concurrent bulk-write
        # transactions (e.g. the Saturday NCAAF experiment's single-tx
        # persist) rather than abort on lock contention.
        connection.execute("SET statement_timeout='180s'")
        for path in files:
            sql = path.read_text()
            upper = sql.upper()
            for token in FORBIDDEN:
                if token in upper:
                    print(f"refusing destructive migration {path.name}: contains {token}")
                    return 1
            with connection.transaction():
                connection.execute(sql)
            applied.append(path.name)
            print(f"applied {path.name}")
    print(f"migrations ok: {len(applied)} applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
