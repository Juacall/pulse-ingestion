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


def get_table_hash(conn, db_name: str, table_name: str) -> str:
    """Computes an MD5 checksum for a table across all columns."""
    cols = [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [db_name, table_name],
        ).fetchall()
    ]
    if not cols:
        return ""

    # Concat all columns to string, get lower 64-bit MD5 integer, XOR across the table
    concat_expr = ", ".join([f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in cols])
    query = (
        f'SELECT BIT_XOR(md5_number_lower(CONCAT({concat_expr}))) '
        f'FROM {db_name}.main."{table_name}"'
    )
    res = conn.execute(query).fetchone()[0]
    return str(res) if res is not None else ""

def update_tables(conn, md_db: str, source_db: str = "local_db", force_overwrite: bool = False):
    """Syncs updated tables from local_db to target MotherDuck database using differential hashing."""
    # Exclude all dbt test artifacts and internal metadata
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'main' "
            "AND table_name NOT LIKE '%dbt_test%' "
            "AND table_name NOT LIKE 'test_%' "
            "AND table_name NOT LIKE '_dbt_%'",
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
            print(f"➕ Creating new table: {name}...")
            conn.execute(
                f'CREATE TABLE {md_db}.main."{name}" AS '
                f'SELECT * FROM {source_db}.main."{name}"'
            )
            continue

        if not force_overwrite:
            local_hash = get_table_hash(conn, source_db, name)
            remote_hash = get_table_hash(conn, md_db, name)

            if local_hash == remote_hash and local_hash != "":
                print(f"⏭️ Skipping {name} (No changes detected)")
                continue

        print(f"🔄 Updating modified table: {name}...")
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
    conn = duckdb.connect("md:")
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {MD_DATABASE}")
    conn.execute(f"USE {MD_DATABASE}")
    conn.execute("INSTALL vss")
    conn.execute("LOAD vss")
    conn.execute("SET hnsw_enable_experimental_persistence = true")
    conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (READ_ONLY)")

    # Perform table sync using hash checks
    update_tables(conn, md_db=MD_DATABASE, source_db="local_db", force_overwrite=False)

    # Perform view sync
    update_views(conn, md_db=MD_DATABASE, source_db="local_db")

    conn.execute("DETACH local_db")
    conn.close()
    print(f"Successfully synced MotherDuck database ({MD_DATABASE}).")