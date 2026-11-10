import argparse
import logging
from pathlib import Path
import sys
import time
import asyncio
import ast
import json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
import duckdb

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import fetch_members_for_congress
from config import (
    ENV,
    BASE_URL,
    API_KEY,
    DB_PATH,
    DEFAULT_CHAMBER,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from database import (
    get_db_connection,
    check_tables_exist,
    create_tables,
    get_district_geoids,
    get_existing_bills,
    get_existing_bill_timestamps,
    get_existing_members,
    drop_table, addColumn, dropColumn,
)
from ingest_amendments import ingest_bill_amendments, _ingest_bill_amendments_async
from ingest_members import ingest_members
from ingest_bills import ingest_member_bills
logger = logging.getLogger(__name__)

# -- Prompt Helpers ------------------------------------------------------------

def prompt_reset_tables() -> bool:
    """Prompt the user to recreate/reset raw tables."""
    while True:
        choice = input("Recreate/reset raw tables? (y/n): ").strip().lower()
        if choice in ("y", "yes"):
            return True
        elif choice in ("n", "no"):
            return False
        print("Invalid input. Please enter 'y' (yes) or 'n' (no).")


def prompt_refresh_mode() -> bool:
    """Prompt the user to specify whether this is a full refresh or incremental refresh."""
    print("\nSelect refresh mode:")
    print("  1) Incremental refresh (preserve existing data, skip unchanged records)")
    print("  2) Full refresh (reset/recreate raw tables)")

    while True:
        choice = input("Is this a full refresh or incremental refresh? (1=incremental, 2=full, or i/f): ").strip().lower()
        if choice in ("1", "i", "incremental", "n", "no"):
            print("Selected: Incremental refresh\n")
            return False
        elif choice in ("2", "f", "full", "y", "yes"):
            print("Selected: Full refresh (tables will be reset)\n")
            return True
        print("Invalid choice. Please enter '1'/'i' for Incremental or '2'/'f' for Full refresh.")


def prompt_ingestion_menu() -> dict | None:
    """Display interactive menu for selecting the ingestion pipeline task."""
    print("\n" + "=" * 55)
    print("           CONGRESS INGESTION PIPELINE")
    print("=" * 55)
    print("Please select which task to run:")
    print("  1) House Members Only")
    print("  2) Senate Members Only")
    print("  3) House Bills Only")
    print("  4) House Amendments Only")
    print("  5) Senate Bills Only")
    print("  6) Senate Amendments Only")
    print("  7) All (Both Chambers - Members, Bills & Amendments)")
    print("  8) Exit")
    print("=" * 55)

    options = {
        "1": {"chamber": "house", "ingestion_mode": "members", "skip_members": False, "label": "House Members Only"},
        "2": {"chamber": "senate", "ingestion_mode": "members", "skip_members": False, "label": "Senate Members Only"},
        "3": {"chamber": "house", "ingestion_mode": "bills", "skip_members": True, "label": "House Bills Only"},
        "4": {"chamber": "house", "ingestion_mode": "amendments", "skip_members": True, "label": "House Amendments Only"},
        "5": {"chamber": "senate", "ingestion_mode": "bills", "skip_members": True, "label": "Senate Bills Only"},
        "6": {"chamber": "senate", "ingestion_mode": "amendments", "skip_members": True, "label": "Senate Amendments Only"},
        "7": {"chamber": "both", "ingestion_mode": "all", "skip_members": False, "label": "All (Both Chambers - Members, Bills & Amendments)"},
    }

    while True:
        choice = input("Enter choice [1-8]: ").strip().lower()
        if choice in options:
            selected = options[choice]
            print(f"Selected: {selected['label']}")
            return selected
        elif choice in ("8", "q", "quit", "exit"):
            print("Exiting pipeline.")
            return None
        print("Invalid choice. Please enter a number from 1 to 8 (or 'q' to exit).")


# -- Setup ---------------------------------------------------------------------

def setup_database(
        db,
        reset: bool | None = None,
        reset_tables: list[str] | None = None,
):
    """Initialize database tables."""
    if reset is None:
        should_reset = prompt_refresh_mode()
    else:
        should_reset = reset

    if should_reset:
        tables = reset_tables if reset_tables is not None else [
            "raw_members", "raw_bills", "raw_amendments"
        ]
        if tables:
            print(f"Recreating raw table(s) (full refresh mode): {', '.join(tables)}...")
        else:
            print("Full refresh selected, but this task does not ingest into a raw table.")
        create_tables(db, replace=True, replace_tables=tables)
    else:
        print("Ensuring raw tables exist (incremental/preserve mode)...")
        create_tables(db, replace=False)
    return should_reset


# -- Ingestion -----------------------------------------------------------------

def run_ingestion(
        db,
        member_limit: int | None = DEFAULT_MEMBER_LIMIT,
        random_sample: bool = False,
        use_async: bool = False,
        members_only: bool = False,
        skip_members: bool = False,
        ingest_bills: bool = True,
        ingest_amendments: bool = True,
        full_refresh: bool = False,
        chamber: str = DEFAULT_CHAMBER,
):
    """Fetch data from Congress API and ingest into database tables incrementally."""
    start_time = time.time()
    chamber = (chamber or DEFAULT_CHAMBER).lower()

    amendments_only = ingest_amendments and not ingest_bills
    if not skip_members and not amendments_only:
        if not check_tables_exist(db, ["raw_census__cd11920_county20"]):
            raise RuntimeError(
                "Prerequisite seed table 'raw_census__cd11920_county20' is missing. "
                "Please run 'uv run dbt seed' from the dbt/ directory first."
            )

    if not check_tables_exist(db, ["raw_members", "raw_bills", "raw_amendments"]):
        print("Raw destination tables missing. Creating raw tables...")
        create_tables(db, replace=False)

    limit_desc = "ALL" if member_limit is None else str(member_limit)

    member_stats = None
    existing_bills = None
    if members_only:
        ingest_bills = False
        ingest_amendments = False

    if amendments_only:
        members = []
        print(f"\nLoading stored {chamber} bills from raw_bills for amendment ingestion...")
        existing_bills = get_existing_bills(
            db, chamber=chamber if chamber != "both" else None
        )
        print(f"Loaded {len(existing_bills)} distinct bill(s) from database.")
    elif skip_members:
        print(f"\nSkipping member API fetch. Loading existing members from raw_members table (chamber={chamber}, limit={limit_desc}, random={random_sample})...")
        members = get_existing_members(db, member_limit=member_limit, shuffle=random_sample, chamber=chamber if chamber != "both" else None)
        print(f"Loaded {len(members)} member(s) from database.")
    else:
        members, member_stats = ingest_members(
            db,
            member_limit=member_limit,
            random_sample=random_sample,
            chamber=chamber,
        )
        existing_bills = None

    if not ingest_bills and not ingest_amendments:
        print("\nMembers-only mode selected. Skipping bills and amendments ingestion.")
        unique_bills_cnt = 0
        fetched_amdts = 0
        skipped_amdts = 0
    else:
        existing_bill_timestamps = get_existing_bill_timestamps(db)

        mode = "Async Streaming" if use_async else "Synchronous"
        print(f"\n Starting Ingestion Pipeline [{mode} Mode] (chamber={chamber})...")

        if use_async:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_async_pipeline_wrapper(
                db, members, existing_bill_timestamps, chamber=chamber,
                ingest_bills=ingest_bills, ingest_amendments=ingest_amendments,
                existing_bills=existing_bills,
                force_amendment_refresh=full_refresh and ingest_amendments,
            )
        else:
            unique_bills_cnt, fetched_amdts, skipped_amdts = run_sync_pipeline(
                db, members, existing_bill_timestamps, chamber=chamber,
                ingest_bills=ingest_bills, ingest_amendments=ingest_amendments,
                existing_bills=existing_bills,
                force_amendment_refresh=full_refresh and ingest_amendments,
            )

    elapsed = time.time() - start_time
    print(f"\n-- Ingestion Summary --")
    print(f"  Total time elapsed: {elapsed:.2f}s")
    print(f"  Chamber: {chamber.upper()}")
    print(f"  Members fetched: {len(members)}")
    if member_stats is not None:
        print(f"    - New (inserted):          {member_stats['inserted']}")
        print(f"    - Changed (updated):       {member_stats['updated']}")
        print(f"    - Unchanged (write skipped): {member_stats['unchanged']}")
    else:
        print(f"    (members loaded from DB; no upsert performed)")
    print(f"  Unique bills tracked: {unique_bills_cnt}")
    print(f"  Amendment calls made: {fetched_amdts}")
    print(f"  Amendment calls skipped (unchanged): {skipped_amdts}")

    print("\n-- Tables in dev.duckdb --")
    for (table,) in db.execute("SHOW TABLES").fetchall():
        count = db.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")


def run_sync_pipeline(
        db,
        members,
        existing_timestamps,
        chamber: str = "house",
        ingest_bills: bool = True,
        ingest_amendments: bool = True,
        existing_bills: list[tuple[int, str, str, str | None]] | None = None,
        force_amendment_refresh: bool = False,
):
    """Synchronous pipeline using in-memory unique_bills dict."""
    unique_bills = {}

    if ingest_bills:
        unique_bills = ingest_member_bills(db, members, chamber=chamber, fetch_summaries_sync=True)

    if not ingest_amendments:
        return len(unique_bills), 0, 0

    bills_to_process = existing_bills if existing_bills is not None else unique_bills
    return ingest_bill_amendments(
        db,
        bills_to_process,
        existing_timestamps=existing_timestamps,
        full_refresh=force_amendment_refresh,
    )


# ---------------- Streaming Pipeline Flow ----------------------

def run_async_pipeline_wrapper(
        db,
        members,
        existing_timestamps,
        chamber: str = "house",
        ingest_bills: bool = True,
        ingest_amendments: bool = True,
        existing_bills: list[tuple[int, str, str, str | None]] | None = None,
        force_amendment_refresh: bool = False,
):
    """Wrapper to trigger asyncio event loop for async streaming engine."""
    return asyncio.run(run_streaming_pipeline(
        db, members, existing_timestamps, chamber=chamber,
        ingest_bills=ingest_bills, ingest_amendments=ingest_amendments,
        existing_bills=existing_bills,
        force_amendment_refresh=force_amendment_refresh,
    ))


async def run_streaming_pipeline(
        db,
        members,
        existing_timestamps,
        chamber: str = "house",
        ingest_bills: bool = True,
        ingest_amendments: bool = True,
        existing_bills: list[tuple[int, str, str, str | None]] | None = None,
        force_amendment_refresh: bool = False,
):
    """Ingest bills, then fetch their amendments and CRS summaries concurrently."""
    if ingest_bills:
        bills_to_process = ingest_member_bills(db, members, chamber=chamber, fetch_summaries_sync=False)
    elif existing_bills is not None:
        bills_to_process = existing_bills
    else:
        bills_to_process = {}

    return await _ingest_bill_amendments_async(
        db,
        bills_to_process,
        existing_timestamps,
        full_refresh=force_amendment_refresh,
    )


def start_pipeline(args):
    member_limit = None if ENV != 'Dev' else 5
    random_sample = getattr(args, "random", False)

    has_explicit_cli_flags = any([
        getattr(args, "house", False),
        getattr(args, "senate", False),
        getattr(args, "chamber", None) is not None,
        getattr(args, "members_only", False),
        getattr(args, "bills_only", False),
        getattr(args, "amendments_only", False),
        getattr(args, "skip_members", False),
        ])

    reset = getattr(args, "reset", None)

    if not has_explicit_cli_flags:
        selection = prompt_ingestion_menu()
        if selection is None:
            return
        chamber = selection["chamber"]
        ingestion_mode = selection["ingestion_mode"]
        skip_members = selection["skip_members"]

        if reset is None:
            reset = prompt_refresh_mode()

    else:
        chamber = DEFAULT_CHAMBER
        if getattr(args, "senate", False):
            chamber = "senate"
        elif getattr(args, "house", False):
            chamber = "house"
        elif getattr(args, "chamber", None):
            chamber = args.chamber
        members_only = getattr(args, "members_only", False)
        if getattr(args, "bills_only", False):
            ingestion_mode = "bills"
        elif getattr(args, "amendments_only", False):
            ingestion_mode = "amendments"
        elif members_only:
            ingestion_mode = "members"
        else:
            ingestion_mode = "all"
        skip_members = getattr(args, "skip_members", False)

    members_only = ingestion_mode == "members"
    ingest_bills = ingestion_mode in ("bills", "all")
    ingest_amendments = ingestion_mode in ("amendments", "all")

    db = get_db_connection()

    reset_tables = []
    if not skip_members and ingestion_mode in ("members", "all"):
        reset_tables.append("raw_members")
    if ingest_bills:
        reset_tables.append("raw_bills")
    if ingest_amendments:
        reset_tables.append("raw_amendments")
    full_refresh = setup_database(db, reset=reset, reset_tables=reset_tables)

    run_ingestion(
        db,
        member_limit=member_limit,
        random_sample=random_sample,
        use_async=getattr(args, "use_async", True),
        members_only=members_only,
        skip_members=skip_members,
        ingest_bills=ingest_bills,
        ingest_amendments=ingest_amendments,
        full_refresh=full_refresh,
        chamber=chamber,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")
