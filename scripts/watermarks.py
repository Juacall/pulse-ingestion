"""Read and write per-pipeline ETL watermarks stored in main.etl_watermarks."""
from datetime import datetime, timezone

DEFAULT_WATERMARK = "2025-01-01T00:00:00Z"

# pipeline -> watermark name, source table, and the timestamp column that defines
# the watermark. Pipelines without a timestamp column use the run time instead.
WATERMARK_SOURCES = {
    "members": {"name": "congress_members", "table": "raw_members", "column": "update_date"},
    "bills": {"name": "congress_bills", "table": "raw_bills", "column": "update_date"},
    "amendments": {"name": "congress_amendments", "table": "raw_amendments", "column": "update_date"},
    "candidates": {"name": "fec_candidates", "table": "raw_fec_candidates", "column": None},
    "elections": {"name": "fec_elections", "table": "fct_elections", "column": None},
    "votes": {"name": "member_votes", "table": "fact_member_votes", "column": None},
    "fec": {"name": "fec_donors", "table": "politician_top_donors", "column": None},
}


def _ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.etl_watermarks (
            pipeline_name VARCHAR PRIMARY KEY,
            last_watermark VARCHAR NOT NULL,
            records_processed BIGINT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    from database import ensure_primary_key
    ensure_primary_key(conn, "etl_watermarks", ["pipeline_name"])


def get_watermark(conn, pipeline_name: str) -> str:
    """Return the last stored watermark, or the default if the pipeline never ran."""
    _ensure_table(conn)
    row = conn.execute(
        "SELECT last_watermark FROM main.etl_watermarks WHERE pipeline_name = ?",
        [pipeline_name],
    ).fetchone()
    return row[0] if row else DEFAULT_WATERMARK


def set_watermark(conn, pipeline_name: str, watermark: str, records_processed: int):
    _ensure_table(conn)
    conn.execute("""
        INSERT INTO main.etl_watermarks (pipeline_name, last_watermark, records_processed, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name) DO UPDATE SET
            last_watermark = EXCLUDED.last_watermark,
            records_processed = EXCLUDED.records_processed,
            updated_at = now();
    """, [pipeline_name, watermark, records_processed])


def _parse(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def compute_new_watermark(conn, pipeline: str, previous: str) -> tuple[str, int]:
    """Return (new watermark, records processed since the previous watermark)."""
    source = WATERMARK_SOURCES[pipeline]
    table, column = source["table"], source["column"]

    if column is None:
        total = conn.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), total

    since = _parse(previous) or _parse(DEFAULT_WATERMARK)
    max_ts, count = conn.execute(
        f"SELECT MAX({column}), COUNT(*) FILTER (WHERE {column} > ?) FROM main.{table}",
        [since],
    ).fetchone()
    if max_ts is None or max_ts <= since:
        return previous, 0
    return max_ts.strftime("%Y-%m-%dT%H:%M:%SZ"), count
