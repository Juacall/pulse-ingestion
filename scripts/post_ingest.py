"""
scripts/post_ingest.py

After an ingestion flow: rebuild the dbt models downstream of the tables it
wrote, sync the result to MotherDuck, then invalidate the matching API caches.
"""
import sys
from pathlib import Path

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"

# Per pipeline: dbt selectors (the trailing "+" builds everything downstream),
# optional excludes, and the cache prefixes to invalidate after the sync.
PIPELINE_REFRESH = {
    "members": {
        "select": ["source:congress.raw_members+"],
        "cache_prefixes": ["members", "politicians"],
    },
    "bills": {
        "select": ["source:congress.raw_bills+"],
        "cache_prefixes": ["bills"],
    },
    "amendments": {
        "select": ["source:congress.raw_amendments+"],
        "cache_prefixes": ["amendments"],
    },
    "candidates": {
        "select": ["source:congress.raw_fec_candidates+"],
        "cache_prefixes": ["politicians", "elections"],
    },
    # ingest_elections writes fct_elections/fct_race_candidates directly, so only
    # their downstream models are rebuilt (rebuilding them would overwrite the load).
    "elections": {
        "select": ["fct_elections+", "fct_race_candidates+"],
        "exclude": ["fct_elections", "fct_race_candidates"],
        "cache_prefixes": ["elections"],
    },
    "votes": {
        "select": [],
        "cache_prefixes": ["votes"],
    },
    "fec": {
        "select": [
            "source:congress.politician_fec_summary+",
            "source:congress.politician_top_donors+",
            "source:congress.stg_fec_independent_expenditures+",
        ],
        "cache_prefixes": ["donors", "financials"],
    },
}


def run_dbt_build(select: list[str], exclude: list[str] | None = None) -> bool:
    """Run `dbt build` for the given selectors against the local dev database."""
    import os
    from dbt.cli.main import dbtRunner
    from config import DB_PATH

    # dbt resolves a relative profile path against the cwd, so pass the absolute DB file.
    os.environ["DBT_DUCKDB_PATH"] = str(DB_PATH)

    args = [
        "build",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--select", *select,
        # Concurrent table swaps race on DuckDB's rename-to-backup step, so build serially by default.
        "--threads", os.environ.get("DBT_THREADS", "1"),
    ]
    if exclude:
        args += ["--exclude", *exclude]
    result = dbtRunner().invoke(args)
    _release_dbt_connections()
    return result.success


def _release_dbt_connections():
    """Close dbt's DuckDB connection; an open handle blocks later ATTACHes of the same file in this process."""
    from dbt.adapters.factory import cleanup_connections, reset_adapters
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager

    cleanup_connections()
    reset_adapters()
    if DuckDBConnectionManager._ENV is not None:
        DuckDBConnectionManager._ENV.close()
        DuckDBConnectionManager._ENV = None


def read_watermarks(pipelines: list[str]) -> dict[str, str]:
    """Read each pipeline's last watermark; call this before the ingestion runs."""
    from database import get_db_connection
    from watermarks import WATERMARK_SOURCES, get_watermark

    conn = get_db_connection()
    try:
        previous = {}
        for name in pipelines:
            watermark_name = WATERMARK_SOURCES[name]["name"]
            previous[name] = get_watermark(conn, watermark_name)
            print(f"Last watermark for {watermark_name}: {previous[name]}")
        return previous
    finally:
        conn.close()


def record_watermarks(pipelines: list[str], previous: dict[str, str]):
    """Store the new watermark for each pipeline; call after a successful dbt build."""
    from database import get_db_connection
    from watermarks import WATERMARK_SOURCES, compute_new_watermark, set_watermark

    conn = get_db_connection()
    try:
        for name in pipelines:
            watermark_name = WATERMARK_SOURCES[name]["name"]
            watermark, records = compute_new_watermark(conn, name, previous[name])
            set_watermark(conn, watermark_name, watermark, records)
            print(f"Updated watermark for {watermark_name}: {watermark} ({records} records)")
    finally:
        conn.close()


def refresh_after_ingest(pipelines: list[str], previous_watermarks: dict[str, str] | None = None) -> bool:
    """dbt build downstream models, record watermarks, sync to MotherDuck, then invalidate caches."""
    unknown = [name for name in pipelines if name not in PIPELINE_REFRESH]
    if unknown:
        raise ValueError(f"No refresh config for pipeline(s): {', '.join(unknown)}")

    select, exclude, prefixes = [], [], []
    for name in pipelines:
        config = PIPELINE_REFRESH[name]
        select += [s for s in config["select"] if s not in select]
        exclude += [e for e in config.get("exclude", []) if e not in exclude]
        prefixes += [p for p in config["cache_prefixes"] if p not in prefixes]

    if select:
        print(f"\n-- Rebuilding dbt models: {' '.join(select)} --")
        if not run_dbt_build(select, exclude):
            print("dbt build failed; skipping MotherDuck sync and cache invalidation.")
            return False
    else:
        print("\nNo dbt models depend on this pipeline; skipping dbt build.")

    if previous_watermarks is not None:
        record_watermarks(pipelines, previous_watermarks)

    print("\n-- Syncing to MotherDuck --")
    from sync_motherduck import sync_motherduck, notify_cache_invalidation
    sync_motherduck()

    print("\n-- Invalidating caches --")
    for prefix in prefixes:
        notify_cache_invalidation(prefix)
    return True
