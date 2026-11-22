"""
scripts/database.py

Database schema initialization, connection helpers, validations, and metadata tracking for DuckDB.
"""

import logging
from pathlib import Path
import os
import sys
import duckdb

# Add script folder to path if executed standalone or as module
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from config import DB_PATH, STATE_FIPS_TO_ABBR
except ImportError:
    from scripts.config import DB_PATH, STATE_FIPS_TO_ABBR

logger = logging.getLogger(__name__)


def get_db_connection(db_path: Path = DB_PATH, read_only: bool = False):
    # """Connect to MotherDuck; the local file is used only when ALLOW_LOCAL_DB_FALLBACK is true."""
    # PULSE_FORCE_LOCAL keeps a prod job working on its hydrated local file; it syncs to MotherDuck afterwards.
    if os.getenv("ENV") == "prod" and os.getenv("PULSE_FORCE_LOCAL") != "1":
        token = os.getenv("MOTHERDUCK_TOKEN")
        database_name = "pulse_prod"
        if token:
            logger.info("Using MotherDuck database '%s'.", database_name)
            # The token is read from MOTHERDUCK_TOKEN by DuckDB, so it never appears in the URL
            conn = duckdb.connect(f"md:{database_name}")
            if read_only:
                conn.execute("SET custom_user_agent='legipulse-fastapi-read';")
            return conn

    return duckdb.connect(str(db_path), read_only=read_only)


def check_tables_exist(db, tables: list[str] = None) -> bool:
    """
    Validate whether the specified tables exist in the database.
    Defaults to checking the 3 required raw tables.
    """
    if tables is None:
        tables = ["raw_members", "raw_bills", "raw_amendments"]

    existing = {
        row[0] for row in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    }
    missing = [t for t in tables if t not in existing]
    if missing:
        return False
    return True

def drop_table(db, table_name: str):
    # Ensure table_name contains only alphanumeric characters and underscores

    db.execute(f'DROP TABLE IF EXISTS "{table_name}"')

def addColumn(db,table_name,column_name, type):
    db.execute(f'ALTER TABLE main.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {type}')

def dropColumn(db, table_name: str, column_name: str):
    """
    Safely drop a column from a main schema table in DuckDB if it exists.
    """
    # Double-quote identifiers to preserve casing and protect against invalid syntax
    query = f'ALTER TABLE main."{table_name}" DROP COLUMN IF EXISTS "{column_name}"'
    db.execute(query)


RAW_TABLE_KEYS = {
    "raw_members": ["bioguide_id"],
    "raw_bills": ["congress", "bill_type", "bill_number", "member_id", "relationship"],
    "raw_amendments": ["congress", "bill_type", "bill_number", "amendment_number", "amendment_type"],
    "bill_amendment_ingestion_state": ["congress", "bill_type", "bill_number"],
}


def _set_aside_tables_without_primary_key(db, table_keys: dict[str, list[str]] | None = None) -> list[str]:
    """Rename tables that lost their primary key (e.g. copied with CREATE TABLE AS) so they can be rebuilt."""
    set_aside = []
    for table in table_keys or RAW_TABLE_KEYS:
        exists = db.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database() "
            "AND schema_name = 'main' AND table_name = ?", [table]).fetchone()[0]
        has_key = db.execute(
            "SELECT count(*) FROM duckdb_constraints() WHERE database_name = current_database() "
            "AND schema_name = 'main' AND table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [table]).fetchone()[0]
        if exists and not has_key:
            db.execute(f'ALTER TABLE main."{table}" RENAME TO "{table}__nopk"')
            set_aside.append(table)
    return set_aside


def _restore_set_aside_rows(db, tables: list[str], table_keys: dict[str, list[str]] | None = None):
    table_keys = table_keys or RAW_TABLE_KEYS
    for table in tables:
        keys = ", ".join(table_keys[table])
        db.execute(
            f'INSERT INTO main."{table}" BY NAME '
            f'SELECT * FROM (SELECT DISTINCT ON ({keys}) * FROM main."{table}__nopk") '
            "ON CONFLICT DO NOTHING"
        )
        db.execute(f'DROP TABLE main."{table}__nopk"')
        print(f"Rebuilt {table} with its primary key.")


def ensure_primary_key(db, table: str, keys: list[str]) -> bool:
    """Rebuild ``main.<table>`` with a primary key if it lacks one; returns True when rebuilt.

    Tables copied with CREATE TABLE AS (MotherDuck hydrate/sync) lose their keys, which makes
    ``ON CONFLICT`` upserts fail. DuckDB cannot add a key afterwards, so the table is recreated
    with the same columns and the rows are copied back.
    """
    exists = db.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database() "
        "AND schema_name = 'main' AND table_name = ?", [table]).fetchone()[0]
    has_key = db.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE database_name = current_database() "
        "AND schema_name = 'main' AND table_name = ? AND constraint_type = 'PRIMARY KEY'",
        [table]).fetchone()[0]
    if not exists or has_key:
        return False

    columns = db.execute(
        "SELECT column_name, data_type, column_default FROM duckdb_columns() "
        "WHERE database_name = current_database() AND schema_name = 'main' AND table_name = ? "
        "ORDER BY column_index", [table]).fetchall()
    definitions = [
        f'"{name}" {data_type}' + (f" DEFAULT {default}" if default else "")
        for name, data_type, default in columns
    ]
    key_list = ", ".join(f'"{k}"' for k in keys)
    db.execute(f'ALTER TABLE main."{table}" RENAME TO "{table}__nopk"')
    db.execute(f'CREATE TABLE main."{table}" ({", ".join(definitions)}, PRIMARY KEY ({key_list}))')
    db.execute(
        f'INSERT INTO main."{table}" BY NAME '
        f'SELECT * FROM (SELECT DISTINCT ON ({key_list}) * FROM main."{table}__nopk")'
    )
    db.execute(f'DROP TABLE main."{table}__nopk"')
    print(f"Rebuilt {table} with its primary key.")
    return True


def create_tables(db, replace: bool = False, replace_tables: list[str] | None = None):
    """
    Initialize raw tables in DuckDB with primary keys and checksum/timestamp tracking.

    Three raw tables feed the dbt models downstream:
    - raw_members:    one row per current House or Senate member
    - raw_bills:      one row per member-bill relationship (sponsor or cosponsor)
    - raw_amendments: all amendments to target bills, regardless of sponsor

    When ``replace`` is true, ``replace_tables`` optionally limits which raw
    tables are recreated. If omitted, all raw tables are replaced.
    """
    raw_tables = {"raw_members", "raw_bills", "raw_amendments"}
    tables_to_replace = raw_tables if replace_tables is None else set(replace_tables)
    unknown_tables = tables_to_replace - raw_tables
    if unknown_tables:
        raise ValueError(f"Unknown raw table(s) requested for replacement: {', '.join(sorted(unknown_tables))}")

    def create_stmt(table_name: str) -> str:
        should_replace = replace and table_name in tables_to_replace
        return "CREATE OR REPLACE TABLE" if should_replace else "CREATE TABLE IF NOT EXISTS"

    set_aside = _set_aside_tables_without_primary_key(db)

    db.execute(f"""
               CREATE TABLE IF NOT EXISTS main.etl_watermarks (
                                                                  pipeline_name VARCHAR PRIMARY KEY,
                                                                  last_watermark VARCHAR NOT NULL,      -- ISO timestamp, integer ID, or dataset_hash
                                                                  records_processed BIGINT DEFAULT 0,
                                                                  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               )
    
    """)

    db.execute(f"""
        {create_stmt("raw_members")} main.raw_members (
            bioguide_id   VARCHAR,
            name          VARCHAR,
            state         VARCHAR,
            chamber       VARCHAR,
            district      INTEGER,
            party         VARCHAR,
            geoid_cd      VARCHAR,
            website_url   VARCHAR,
            campaign_url  VARCHAR,
            image_url     VARCHAR,
            committee_id  VARCHAR,
            update_date   TIMESTAMP,
            row_hash      VARCHAR,
            start_year    INTEGER,
            end_year      INTEGER,
            ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (bioguide_id)
            
        )
    """)
    db.execute(f"""
        {create_stmt("raw_bills")} main.raw_bills (
            congress            INTEGER,
            bill_type           VARCHAR,
            bill_number         VARCHAR,
            title               VARCHAR,
            introduced_date     VARCHAR,
            latest_action_date  VARCHAR,
            latest_action_text  VARCHAR,
            member_id           VARCHAR,
            relationship        VARCHAR,
            policy_area         VARCHAR,
            summary             VARCHAR,
            update_date         TIMESTAMP,
            row_hash            VARCHAR,
            ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (congress, bill_type, bill_number, member_id, relationship)
        )
    """)

    db.execute(f"""
        {create_stmt("raw_amendments")} main.raw_amendments (
            congress          INTEGER,
            bill_type         VARCHAR,
            bill_number       VARCHAR,
            amendment_number  VARCHAR,
            amendment_type    VARCHAR,
            description       VARCHAR,
            purpose           VARCHAR,
            sponsor_id        VARCHAR,
            update_date       TIMESTAMP,
            row_hash          VARCHAR,
            ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (congress, bill_type, bill_number, amendment_number, amendment_type)
        )
    """)


    amendment_state_stmt = (
        "CREATE OR REPLACE TABLE"
        if replace and "raw_amendments" in tables_to_replace
        else "CREATE TABLE IF NOT EXISTS"
    )
    db.execute(f"""
        {amendment_state_stmt} main.bill_amendment_ingestion_state (
            congress INTEGER,
            bill_type VARCHAR,
            bill_number VARCHAR,
            latest_action_date VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (congress, bill_type, bill_number)
        )
    """)
    _restore_set_aside_rows(db, set_aside)

    db.execute(f"""
            -- Track execution lifecycle
         CREATE TABLE IF NOT EXISTS main.ingestion_runs (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status VARCHAR -- 'RUNNING', 'COMPLETED', 'FAILED'
        )
""")

    db.execute(f"""
            -- Track completed operational steps to allow seamless resumes
             CREATE TABLE IF NOT EXISTS main.ingestion_step_history (
                run_id VARCHAR,
                step_type VARCHAR,  -- 'MEMBER_BILLS', 'BILL_AMENDMENTS'
                entity_key VARCHAR, -- e.g., 'bioguide_id' or '119-HR-1234'
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, step_type, entity_key)
            )
""")


def get_existing_bill_timestamps(db) -> dict[tuple[int, str, str], str]:
    """
    Return the last action date processed for amendments per bill.

    This is deliberately separate from raw_bills: bill ingestion may run before
    the first amendment ingestion, and should not mark amendments as processed.
    """
    create_tables(db, replace=False)
    rows = db.execute("""
        SELECT congress, bill_type, bill_number, latest_action_date
        FROM main.bill_amendment_ingestion_state
        WHERE latest_action_date IS NOT NULL
    """).fetchall()
    return {(row[0], row[1].upper(), str(row[2])): row[3] for row in rows}


def mark_bill_amendments_processed(db, congress, bill_type, bill_number, latest_action_date):
    """Persist the bill action date after its amendments were fetched and saved."""
    db.execute("""
        INSERT INTO main.bill_amendment_ingestion_state (
            congress, bill_type, bill_number, latest_action_date, processed_at
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (congress, bill_type, bill_number) DO UPDATE SET
            latest_action_date = EXCLUDED.latest_action_date,
            processed_at = EXCLUDED.processed_at
    """, [congress, bill_type.upper(), str(bill_number), latest_action_date])


def get_existing_bills(db, chamber: str | None = None) -> list[tuple[int, str, str, str | None]]:
    """Return distinct stored bills, optionally limited to a chamber."""
    create_tables(db, replace=False)
    query = """
        SELECT bills.congress, bills.bill_type, bills.bill_number,
               MAX(bills.latest_action_date) AS latest_action_date
        FROM main.raw_bills AS bills
    """
    params = []
    if chamber and chamber.lower() in ("house", "senate"):
        query += """
            JOIN main.raw_members AS members
              ON bills.member_id = members.bioguide_id
            WHERE LOWER(members.chamber) = ?
        """
        params.append(chamber.lower())
    query += """
        GROUP BY bills.congress, bills.bill_type, bills.bill_number
        ORDER BY bills.congress, bills.bill_type, bills.bill_number
    """
    rows = db.execute(query, params).fetchall()
    return [(row[0], row[1].upper(), str(row[2]), row[3]) for row in rows]


def get_existing_members(
    db,
    member_limit: int | None = None,
    shuffle: bool = False,
    chamber: str | None = None,
) -> list[dict]:
    """
    Query existing members from raw_members table.
    Returns a list of dicts with 'bioguideId', 'name', 'chamber', etc.
    """
    create_tables(db, replace=False)
    order_clause = "random()" if shuffle else "bioguide_id"
    query = "SELECT bioguide_id, name, state, chamber, district, party, geoid_cd FROM main.raw_members"
    params = []
    if chamber and chamber.lower() in ("house", "senate"):
        query += " WHERE LOWER(chamber) = ?"
        params.append(chamber.lower())
    query += f" ORDER BY {order_clause}"
    if member_limit is not None:
        query += f" LIMIT {int(member_limit)}"
    rows = db.execute(query, params).fetchall() if params else db.execute(query).fetchall()
    return [
        {
            "bioguideId": row[0],
            "name": row[1],
            "state": row[2],
            "chamber": row[3],
            "district": row[4],
            "partyName": row[5],
            "_geoid_cd": row[6],
        }
        for row in rows
    ]


def get_district_geoids(db) -> dict[tuple[str, int], str]:
    """Map (state abbreviation, district number) to census congressional GEOID."""
    if not check_tables_exist(db, ["raw_census__cd11920_county20"]):
        raise RuntimeError(
            "Prerequisite seed table 'raw_census__cd11920_county20' is missing. "
            "Please run 'uv run dbt seed' from the dbt/ directory first."
        )

    rows = db.execute("""
        SELECT DISTINCT
            GEOID_CD119_20,
            LEFT(GEOID_CD119_20, 2) AS state_fips,
            CAST(RIGHT(GEOID_CD119_20, 2) AS INTEGER) AS district_num
        FROM main.raw_census__cd11920_county20
        WHERE GEOID_CD119_20 NOT LIKE '%ZZ'
    """).fetchall()
    return {
        (STATE_FIPS_TO_ABBR[state_fips], district_num): geoid_cd
        for geoid_cd, state_fips, district_num in rows
        if state_fips in STATE_FIPS_TO_ABBR
    }
