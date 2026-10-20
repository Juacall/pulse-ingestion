"""One-time upload of the local dev.duckdb tables and views to MotherDuck as pulse_prod."""
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

# MOTHERDUCK_TOKEN is picked up from the environment
conn = duckdb.connect("md:")
conn.execute(f"CREATE DATABASE IF NOT EXISTS {MD_DATABASE}")
conn.execute(f"USE {MD_DATABASE}")
# Load vss before attaching so DuckDB can read the local file's HNSW index metadata
conn.execute("INSTALL vss")
conn.execute("LOAD vss")
conn.execute("SET hnsw_enable_experimental_persistence = true")
conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (READ_ONLY)")

tables = [
    r[0]
    for r in conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = 'local_db' AND schema_name = 'main'"
    ).fetchall()
]
for name in tables:
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? "
        "AND schema_name = 'main' AND table_name = ?",
        [MD_DATABASE, name],
    ).fetchone()[0]
    if exists:
        print(f"Skipping table {name} (already in MotherDuck)")
        continue
    print(f"Copying table {name}...")
    conn.execute(
        f'CREATE TABLE {MD_DATABASE}.main."{name}" AS '
        f'SELECT * FROM local_db.main."{name}"'
    )

# Views can depend on each other, so retry until all are created
views = dict(
    conn.execute(
        "SELECT view_name, sql FROM duckdb_views() "
        "WHERE database_name = 'local_db' AND schema_name = 'main' AND NOT internal"
    ).fetchall()
)
pending = dict(views)
while pending:
    failed = {}
    for name, sql in pending.items():
        try:
            sql = re.sub(r'(?<![\w"])"?dev"?\.', f"{MD_DATABASE}.", sql)
            sql = sql.replace('main."trim"(', '"trim"(')
            conn.execute(sql.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1))
            print(f"Created view {name}")
        except duckdb.Error as exc:
            failed[name] = sql
            last_error = exc
    if len(failed) == len(pending):
        raise SystemExit(f"Could not create views {list(failed)}: {last_error}")
    pending = failed

conn.execute("DETACH local_db")
conn.close()
print(f"Pushed {len(tables)} tables and {len(views)} views to MotherDuck ({MD_DATABASE}).")
