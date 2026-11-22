"""Sync local dev.duckdb tables and views to MotherDuck as pulse_prod."""
import os
import re
from pathlib import Path
import requests
import duckdb
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
LOCAL_DB_PATH = ROOT_DIR / "dev.duckdb"

load_dotenv(ROOT_DIR / ".env")


if os.environ.get("ENV") == "dev":
    MD_DATABASE = "pulse_dev"
    BACKEND_URL  = os.environ.get("DEV_SERVER_URL")
else:
    MD_DATABASE = "pulse_prod"
    BACKEND_URL  = os.environ.get("PROD_SERVER_URL")


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

def update_tables(conn, md_db: str, source_db: str = "local_db", force_overwrite: bool = False,
                  exclude_tables: set[str] | None = None):
    """Syncs updated tables from local_db to target MotherDuck database using differential hashing."""
    exclude_tables = exclude_tables or set()
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
        if name in exclude_tables:
            continue
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


def sync_motherduck(exclude_tables: set[str] | None = None):
    """Sync the local database to MotherDuck, optionally skipping some tables."""
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise SystemExit("MOTHERDUCK_TOKEN is not set in .env")
    if not LOCAL_DB_PATH.exists():
        raise SystemExit(f"Local database not found: {LOCAL_DB_PATH}")

    conn = duckdb.connect("md:")
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {MD_DATABASE}")
    conn.execute(f"USE {MD_DATABASE}")
    conn.execute("INSTALL vss")
    conn.execute("LOAD vss")
    conn.execute("SET hnsw_enable_experimental_persistence = true")
    conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (READ_ONLY)")

    # Perform table sync using hash checks
    update_tables(conn, md_db=MD_DATABASE, source_db="local_db", force_overwrite=False,
                  exclude_tables=exclude_tables)

    # Perform view sync
    update_views(conn, md_db=MD_DATABASE, source_db="local_db")

    conn.execute("DETACH local_db")
    conn.close()
    print(f"Successfully synced MotherDuck database ({MD_DATABASE}).")


def notify_cache_invalidation(prefix: str) -> bool:
    """Invalidate the API cache for a prefix; returns True on success."""
    if not BACKEND_URL:
        print("⚠️ Backend URL (DEV_SERVER_URL / PROD_SERVER_URL) is not set; skipping cache invalidation.")
        return False
    api_url = BACKEND_URL + "/api/internal/cache/invalidate"
    headers = {"x-internal-token": os.getenv("INTERNAL_API_SECRET", "super-secret-internal-key")}

    try:
        res = requests.post(api_url, headers=headers, params={"prefix": f"{prefix}" }, timeout=30)
        if res.status_code == 200:
            body = res.json()  # Parse the JSON dictionary returned by FastAPI
            print(f"Status Code: {res.status_code}")

            print(f"Keys Invalidated: {body.get('keys_invalidated', 0)}")
            print("✅ Successfully invalidated analytical cache in Redis.")
            return True
        print(f"⚠️ Invalidation request failed: {res.status_code} - {res.text}")
    except Exception as err:
        print(f"⚠️ Could not reach API cache invalidation endpoint: {err}")
    return False



if __name__ == "__main__":
    sync_motherduck()
