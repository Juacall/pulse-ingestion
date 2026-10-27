"""Sync local dev.duckdb tables and views to MotherDuck as pulse_prod."""
import os
import re
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_DB_PATH = ROOT_DIR / "dev.duckdb"
MD_DATABASE = "pulse_prod"

load_dotenv(ROOT_DIR / ".env")

if not os.environ.get("MOTHERDUCK_TOKEN"):
    raise SystemExit("MOTHERDUCK_TOKEN is not set in .env")
if not LOCAL_DB_PATH.exists():
    raise SystemExit(f"Local database not found: {LOCAL_DB_PATH}")


def update_tables(conn, md_db: str, source_db: str = "local_db", overwrite: bool = False):
    """Syncs tables from attached local_db to target MotherDuck database."""
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'main'",
            [source_db],
        ).fetchall()
    ]

    for name in tables:
        target_exists = conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? "
            "AND schema_name = 'main' AND table_name = ?",
            [md_db, name],
        ).fetchone()[0] > 0

        if not target_exists:
            print(f"Creating missing table {name}...")
            conn.execute(
                f'CREATE TABLE {md_db}.main."{name}" AS '
                f'SELECT * FROM {source_db}.main."{name}"'
            )
        elif overwrite:
            print(f"Overwriting table {name}...")
            conn.execute(
                f'CREATE OR REPLACE TABLE {md_db}.main."{name}" AS '
                f'SELECT * FROM {source_db}.main."{name}"'
            )
        else:
            print(f"Updating table {name}...")
            try:
                conn.execute(
                    f'INSERT OR REPLACE INTO {md_db}.main."{name}" '
                    f'SELECT * FROM {source_db}.main."{name}"'
                )
            except duckdb.Error:
                print(f"  --> Insert failed for {name}. Fallback to overwrite.")
                conn.execute(
                    f'CREATE OR REPLACE TABLE {md_db}.main."{name}" AS '
                    f'SELECT * FROM {source_db}.main."{name}"'
                )


def update_views(conn, md_db: str, source_db: str = "local_db"):
    """Recreates/updates views in MotherDuck from local_db definitions."""
    views = dict(
        conn.execute(
            "SELECT view_name, sql FROM duckdb_views() "
            "WHERE database_name = ? AND schema_name = 'main' AND NOT internal",
            [source_db],
        ).fetchall()
    )
    pending = dict(views)
    while pending:
        failed = {}
        for name, sql in pending.items():
            try:
                sql = re.sub(r'(?<![\w"])"?dev"?\.', f"{md_db}.", sql)
                sql = sql.replace('main."trim"(', '"trim"(')
                conn.execute(sql.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1))
                print(f"Updated view {name}")
            except duckdb.Error as exc:
                failed[name] = sql
                last_error = exc
        if len(failed) == len(pending):
            raise SystemExit(f"Could not update views {list(failed)}: {last_error}")
        pending = failed


if __name__ == "__main__":

    # Main execution
    conn = duckdb.connect("md:")
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {MD_DATABASE}")
    conn.execute(f"USE {MD_DATABASE}")
    conn.execute("INSTALL vss")
    conn.execute("LOAD vss")
    conn.execute("SET hnsw_enable_experimental_persistence = true")
    conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (READ_ONLY)")

    # Perform table sync (Set overwrite=True if schema structure changes frequently)
    update_tables(conn, md_db=MD_DATABASE, source_db="local_db", overwrite=False)

    # Perform view sync
    update_views(conn, md_db=MD_DATABASE, source_db="local_db")

    conn.execute("DETACH local_db")
    conn.close()
    print(f"Successfully updated MotherDuck database ({MD_DATABASE}).")
