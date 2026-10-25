import argparse
import logging
from pathlib import Path
from helpers import parse_api_date
import sys
import time
import asyncio
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
import duckdb

# Bounded queue and concurrency limits to prevent memory bloat and rate-limiting
QUEUE_MAX_SIZE = 500
NUM_AMENDMENT_WORKERS = 5
NUM_BILL_WORKERS = 5
SEMAPHORE = asyncio.Semaphore(NUM_AMENDMENT_WORKERS)



# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import (
    fetch_amendments_for_bill,
    fetch_legislation_for_member,
    fetch_members_for_congress,
)
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
    mark_bill_amendments_processed, drop_table,
)
from ingestion import load_amendments, load_bills, load_members
logger = logging.getLogger(__name__)

# -- Prompt Helpers ------------------------------------------------------------

def prompt_reset_tables() -> bool:
    """
    Prompt the user to recreate/reset raw tables.
    Only accepts 'y', 'yes', 'n', 'no' (case-insensitive).
    Re-prompts until valid input is given.
    """
    while True:
        choice = input("Recreate/reset raw tables? (y/n): ").strip().lower()
        if choice in ("y", "yes"):
            return True
        elif choice in ("n", "no"):
            return False
        print("Invalid input. Please enter 'y' (yes) or 'n' (no).")


def prompt_refresh_mode() -> bool:
    """
    Prompt the user to specify whether this is a full refresh or incremental refresh.
    Returns True for full refresh (recreates/resets tables), False for incremental refresh (preserves existing data).
    """
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
    """
    Display interactive menu for selecting the ingestion pipeline task.
    Returns a dict with chamber, ingestion mode, and member-source configuration,
    or None if the user chooses to exit.
    """
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
    """
    Initialize database tables.
    If reset is explicitly provided (True/False via CLI or interactive prompt), use it.
    Otherwise, interactively prompt the user.
    On a full refresh, reset only the requested raw tables.
    """
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
def fetch_and_upsert_latest_votes(congress=119, session=2):
    conn = get_db_connection()

    # 1. Fetch recent House roll calls from Congress.gov API
    url = f"https://api.congress.gov/v3/house-vote/{congress}/{session}?api_key={API_KEY}&format=json&limit=20"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Congress.gov API returned status {response.status_code}")

    recent_votes = response.json().get("houseVotes", [])
    print(f"Fetched {len(recent_votes)} recent roll calls from Congress.gov API.")

    records_to_insert = []

    for vote in recent_votes:
        roll_number = vote.get("rollCallNumber")
        vote_id = f"{congress}_{session}_h{roll_number}"

        # Check if vote_id already exists in MotherDuck to save API calls
        exists = conn.execute(
            "SELECT 1 FROM main.fact_member_votes WHERE vote_id = ? LIMIT 1;",
            [vote_id]
        ).fetchone()

        if exists:
            continue

        # Detail fetch for individual member voting positions
        detail_url = f"https://api.congress.gov/v3/house-vote/{congress}/{session}/{roll_number}?api_key={API_KEY}&format=json"
        detail_res = requests.get(detail_url).json()

        positions = detail_res.get("houseVote", {}).get("positions", [])
        for pos in positions:
            records_to_insert.append((
                vote_id,
                pos.get("bioguideId"),
                pos.get("votePosition"),  # 'Yea', 'Nay', 'Present', 'Not Voting'
                'congress_gov_api'
            ))

    # 2. Batch UPSERT into MotherDuck
    if records_to_insert:
        print(f"Upserting {len(records_to_insert)} new voting records...")
        conn.executemany("""
                         INSERT INTO main.fact_member_votes (vote_id, member_id, vote_position, data_source)
                         VALUES (?, ?, ?, ?)
                             ON CONFLICT (vote_id, member_id) DO UPDATE SET
                             vote_position = EXCLUDED.vote_position,
                                                                     data_source = EXCLUDED.data_source;
                         """, records_to_insert)
        print("Upsert successful.")
    else:
        print("MotherDuck is already up-to-date with latest votes.")

    conn.close()

def run_initial_bulk_load_votes():
    conn = get_db_connection()

    print("1. Loading raw Voteview bulk vote records into MotherDuck...")

    conn.execute("""
                 CREATE OR REPLACE TABLE main.fact_member_votes AS
                 SELECT
                     CONCAT(v.congress, '_', LOWER(v.chamber), '_', v.rollnumber) AS vote_id,
                     m.bioguide_id AS member_id, -- Corrected column name
                     CASE
                         WHEN v.cast_code IN (1, 2, 3) THEN 'Yea'
                         WHEN v.cast_code IN (4, 5, 6) THEN 'Nay'
                         WHEN v.cast_code IN (7, 8) THEN 'Present'
                         WHEN v.cast_code = 9 THEN 'Not Voting'
                         ELSE 'Other'
                         END AS vote_position,
                     'bulk_voteview' AS data_source,
                     CURRENT_TIMESTAMP AS ingested_at
                 FROM read_csv_auto('https://voteview.com/static/data/out/votes/HS118_votes.csv') v
                          JOIN read_csv_auto('https://voteview.com/static/data/out/members/HS118_members.csv') m
                               ON v.icpsr = m.icpsr AND v.chamber = m.chamber;
                 """)

    # Primary key constraints for safe ON CONFLICT upserts during incremental updates
    conn.execute("""
                 ALTER TABLE main.fact_member_votes
                     ADD PRIMARY KEY (vote_id, member_id);
                 """)

    print("2. Creating analytical attendance view...")
    conn.execute("""
                 CREATE OR REPLACE VIEW main.mart_member_attendance AS
                 SELECT
                     member_id,
                     COUNT(vote_id) AS total_eligible_votes,
                     COUNT(CASE WHEN vote_position = 'Not Voting' THEN 1 END) AS missed_votes,
                     COUNT(CASE WHEN vote_position = 'Present' THEN 1 END) AS present_abstain_votes,
                     ROUND(
                             (COUNT(CASE WHEN vote_position = 'Not Voting' THEN 1 END)::FLOAT / COUNT(vote_id)) * 100,
                             2
                     ) AS missed_vote_pct
                 FROM main.fact_member_votes
                 GROUP BY member_id;
                 """)

    conn.close()
    print("Initial bulk hydration complete!")



STATE_NAME_TO_CODE = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
    "PUERTO RICO": "PR", "GUAM": "GU", "VIRGIN ISLANDS": "VI",
    "AMERICAN SAMOA": "AS", "NORTHERN MARIANA ISLANDS": "MP"
}

def resolve_state_code(raw_state: str | None) -> str | None:
    if not raw_state:
        return None
    clean_state = raw_state.strip().upper()
    # Return directly if already 2-letter postal code, else map from full name
    if len(clean_state) == 2:
        return clean_state
    return STATE_NAME_TO_CODE.get(clean_state)

def ingest_members(
        db,
        member_limit: int | None = DEFAULT_MEMBER_LIMIT,
        random_sample: bool = False,
        chamber: str = DEFAULT_CHAMBER,
):
    """Fetch current Congress members and upsert the selected chamber to raw_members."""
    chamber = (chamber or DEFAULT_CHAMBER).lower()

    limit_desc = "ALL" if member_limit is None else str(member_limit)
    print(f"\nFetching {chamber} members from the Congress API (limit={limit_desc}, random={random_sample})...")
    members = fetch_members_for_congress(
        chamber=chamber,
        member_limit=member_limit,
        shuffle=random_sample,
    )

    district_geoids = get_district_geoids(db)
    for member in members:

        state = (member.get("state") or member.get("stateCode") or "").upper()
        district = member.get("district")
        state_code = resolve_state_code(state)
        member['stateCode'] = state_code
        try:
            district = int(district) if district is not None else None
        except (TypeError, ValueError):
            district = None
        member["district"] = district
        member["_geoid_cd"] = district_geoids.get((state_code, district)) if district is not None else None

        official_url = fetch_member_official_url(member.get("bioguideId"))
        member["website_url"] = official_url
    member_stats = load_members(db, members)
    return members, member_stats


def ingest_member_bills(db, members, chamber: str = "house"):
    """Fetch bills concurrently and serialize their DuckDB upserts on this thread."""
    unique_bills = {}
    bill_stats = {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0}
    if not members:
        print("\n-- Bill Ingestion Complete --")
        print("  Bills ingested (new or changed): 0")
        print("  Bills skipped (unchanged): 0")
        return unique_bills

    def fetch_for_member(member):
        bid = member["bioguideId"]
        member_chamber = member.get("chamber") or chamber
        return fetch_legislation_for_member(bid, chamber=member_chamber)

    with ThreadPoolExecutor(max_workers=NUM_BILL_WORKERS) as executor:
        futures = {
            executor.submit(fetch_for_member, member): member
            for member in members
        }
        for idx, future in enumerate(as_completed(futures), start=1):
            member = futures[future]
            bid = member["bioguideId"]
            try:
                sponsored, cosponsored = future.result()
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to fetch legislation for member {bid} "
                    f"({member.get('name', bid)})"
                ) from exc

            member_chamber = member.get("chamber") or chamber
            print(
                f"[{idx}/{len(members)}] Loaded legislation for "
                f"{member.get('name', bid)} ({member_chamber})..."
            )
            for bills, relationship in (
                (sponsored, "sponsor"),
                (cosponsored, "cosponsor"),
            ):
                stats = load_bills(db, bills, bid, relationship)
                for stat_name in bill_stats:
                    bill_stats[stat_name] += stats[stat_name]

            for bill in sponsored + cosponsored:
                key = (bill.get("congress"), bill.get("type", "").upper(), str(bill.get("number")))
                if key not in unique_bills:
                    unique_bills[key] = bill

    print("\n-- Bill Ingestion Complete --")
    print(f"  Member-bill relationships: {bill_stats['total']}")
    print(
        "  Bills ingested (new or changed): "
        f"{bill_stats['inserted'] + bill_stats['updated']} "
        f"(new: {bill_stats['inserted']}, updated: {bill_stats['updated']})"
    )
    print(f"  Bills skipped (unchanged): {bill_stats['unchanged']}")
    return unique_bills


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

    # Member ingestion uses the census crosswalk only to populate House GEOIDs.
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
        # Load existing bill timestamps for incremental change detection
        existing_bill_timestamps = get_existing_bill_timestamps(db)

        # Execution Branch: Sync vs Async Engine
        mode = "Async Streaming" if use_async and ingest_amendments else "Synchronous"
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
        unique_bills = ingest_member_bills(db, members, chamber=chamber)

    if not ingest_amendments:
        return len(unique_bills), 0, 0

    bills_to_process = existing_bills if existing_bills is not None else unique_bills
    return ingest_bill_amendments(
        db,
        bills_to_process,
        existing_timestamps=existing_timestamps,
        full_refresh=force_amendment_refresh,
    )


def _amendment_bill_items(bills):
    """Normalize bill mappings or stored bill rows to (key, action date) pairs."""
    if isinstance(bills, dict):
        for (congress, bill_type, bill_number), bill in bills.items():
            action_date = (bill.get("latestAction") or {}).get("actionDate")
            yield (congress, bill_type, bill_number), action_date
        return

    for congress, bill_type, bill_number, action_date in bills:
        yield (congress, bill_type, bill_number), action_date


def ingest_bill_amendments(
        db,
        bills,
        existing_timestamps: dict | None = None,
        full_refresh: bool = False,
        use_async: bool = False,
):
    """Fetch and upsert amendments for bills, returning (total, fetched, skipped)."""
    existing_timestamps = existing_timestamps or {}
    if use_async:
        return asyncio.run(_ingest_bill_amendments_async(
            db, bills, existing_timestamps, full_refresh=full_refresh
        ))

    items = list(_amendment_bill_items(bills))
    timestamps = {} if full_refresh else existing_timestamps
    skipped_amendments = 0
    fetched_amendments = 0
    processed_bills = 0
    bills_without_amendments = 0
    for idx, ((congress, bill_type, bill_number), action_date) in enumerate(items, start=1):
        key = (congress, bill_type, bill_number)
        bill_ref = f"{congress}-{bill_type}-{bill_number}"
        current_update = parse_api_date(action_date)
        previous_update = parse_api_date(timestamps.get(key))

        if previous_update and current_update and current_update <= previous_update:
            skipped_amendments += 1
            print(
                f"[{idx}/{len(items)}] Skipping bill {bill_ref}: no changes "
                f"(latest action {current_update}, stored {previous_update})"
            )
            continue

        print(f"[{idx}/{len(items)}] Processing bill {bill_ref}: fetching amendments...")
        amendments = fetch_amendments_for_bill(congress, bill_type, bill_number)
        load_amendments(db, amendments, congress, bill_type, bill_number)
        mark_bill_amendments_processed(
            db, congress, bill_type, bill_number, action_date
        )
        fetched_amendments += 1
        processed_bills += 1
        if amendments:
            print(f"[{idx}/{len(items)}] Processed bill {bill_ref}: loaded {len(amendments)} amendment(s)")
        else:
            bills_without_amendments += 1
            print(f"[{idx}/{len(items)}] Processed bill {bill_ref}: no amendments found")

    print("\n-- Amendment Ingestion Complete --")
    print(f"  Bills considered: {len(items)}")
    print(f"  Bills processed: {processed_bills}")
    print(f"  Bills skipped (no changes): {skipped_amendments}")
    print(f"  Bills with no amendments: {bills_without_amendments}")
    return len(items), fetched_amendments, skipped_amendments


def fetch_member_official_url(bioguide_id: str) -> str | None:
    """Fetches full member detail payload to retrieve officialUrl."""
    url = f"https://api.congress.gov/v3/member/{bioguide_id}?api_key={API_KEY}"
    res = requests.get(url)
    if res.status_code == 200:
        return res.json().get("member", {}).get("officialWebsiteUrl")
    return None

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
    if not ingest_amendments:
        return run_sync_pipeline(
            db, members, existing_timestamps, chamber=chamber,
            ingest_bills=ingest_bills, ingest_amendments=False,
        )
    return asyncio.run(run_streaming_pipeline(
        db, members, existing_timestamps, chamber=chamber,
        ingest_bills=ingest_bills, existing_bills=existing_bills,
        force_amendment_refresh=force_amendment_refresh,
    ))

async def amendment_worker(
        worker_id: int,
        queue: asyncio.Queue,
        write_queue: asyncio.Queue,
        existing_timestamps: dict,
        stats: dict,
):
    """
    Consumer Worker: Continuously pulls unique bills from the queue, fetches
    their amendments, and hands the DB write off to the single db_writer_worker
    (which owns the DuckDB connection) via write_queue.
    """
    while True:
        item = await queue.get()
        if item is None:  # Sentinel value signaling pipeline completion
            queue.task_done()
            break

        congress, bill_type, bill_number, update_date_raw = item
        key = (congress, bill_type, bill_number)
        bill_ref = f"{congress}-{bill_type}-{bill_number}"

        # 1. Fast In-Memory Timestamp Delta Check
        # update_date_raw is the bill's latestAction.actionDate; the baseline holds
        # latest_action_date as a VARCHAR, so parse both sides to datetime.
        current_update = parse_api_date(update_date_raw)
        previous_update = parse_api_date(existing_timestamps.get(key))

        if previous_update and current_update and current_update <= previous_update:
            stats["skipped_amendments"] += 1
            print(
                f"[Worker {worker_id}] Skipping bill {bill_ref}: no changes "
                f"(latest action {current_update}, stored {previous_update})"
            )
            queue.task_done()
            continue

        # 2. Fetch Amendments with Bounded Concurrency
        # Reuse the synchronous api.py client (pagination + 429/5xx retry + 404
        # handling) off the event loop via a thread so the loop stays responsive.
        async with SEMAPHORE:
            try:
                amendments = await asyncio.to_thread(
                    fetch_amendments_for_bill, congress, bill_type, bill_number
                )

                stats["fetched_amendments"] += 1
                stats["bills_processed"] += 1
                # 3. Hand off the write to the single-owner db_writer_worker
                await write_queue.put((
                    amendments, congress, bill_type, bill_number, update_date_raw
                ))
                if amendments:
                    print(
                        f"[Worker {worker_id}] Processed bill {bill_ref}: "
                        f"fetched {len(amendments)} amendment(s) -> queued for write"
                    )
                else:
                    stats["bills_without_amendments"] += 1
                    print(f"[Worker {worker_id}] Processed bill {bill_ref}: no amendments found")

            except Exception as e:
                print(f"[Worker {worker_id}] Error fetching {bill_ref}: {e}")
            finally:
                queue.task_done()


async def _ingest_bill_amendments_async(
        db,
        bills,
        existing_timestamps: dict,
        full_refresh: bool = False,
):
    """Fetch amendments concurrently and serialize DuckDB writes."""
    queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    write_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    stats = {
        "bills_processed": 0,
        "bills_without_amendments": 0,
        "fetched_amendments": 0,
        "skipped_amendments": 0,
    }
    timestamps = {} if full_refresh else existing_timestamps

    writer = asyncio.create_task(db_writer_worker(db, write_queue))
    workers = [
        asyncio.create_task(amendment_worker(i, queue, write_queue, timestamps, stats))
        for i in range(NUM_AMENDMENT_WORKERS)
    ]

    items = list(_amendment_bill_items(bills))
    for (congress, bill_type, bill_number), action_date in items:
        await queue.put((congress, bill_type, bill_number, action_date))

    await queue.join()
    for _ in range(NUM_AMENDMENT_WORKERS):
        await queue.put(None)
    await asyncio.gather(*workers)

    await write_queue.join()
    await write_queue.put(None)
    await writer

    print("\n-- Amendment Ingestion Complete --")
    print(f"  Bills considered: {len(items)}")
    print(f"  Bills processed: {stats['bills_processed']}")
    print(f"  Bills skipped (no changes): {stats['skipped_amendments']}")
    print(f"  Bills with no amendments: {stats['bills_without_amendments']}")
    print(f"  Amendment Packages Fetched: {stats['fetched_amendments']}")
    return len(items), stats["fetched_amendments"], stats["skipped_amendments"]


async def run_streaming_pipeline(
        db,
        members,
        existing_timestamps,
        chamber: str = "house",
        ingest_bills: bool = True,
        existing_bills: list[tuple[int, str, str, str | None]] | None = None,
        force_amendment_refresh: bool = False,
):
    """
    Ingest bills, then fetch their amendments concurrently.
    """
    if ingest_bills:
        bills_to_process = ingest_member_bills(db, members, chamber=chamber)
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

async def db_writer_worker(db, write_queue: asyncio.Queue):
    """
    Single-consumer loop that owns the DuckDB write connection.

    Amendment workers hand off (amendments, congress, bill_type, bill_number)
    tuples here so all writes are serialized through one owner. Items are
    batched and flushed in one transaction to minimize transaction overhead.
    """
    batch = []

    while True:
        item = await write_queue.get()
        if item is None:  # Sentinel to stop worker
            _flush_batch_to_duckdb(db, batch)  # flush any remainder before exit
            batch.clear()
            write_queue.task_done()
            break

        batch.append(item)

        # Flush batch when it reaches size threshold or queue is temporarily empty
        if len(batch) >= 50 or write_queue.empty():
            _flush_batch_to_duckdb(db, batch)
            batch.clear()

        write_queue.task_done()


def _flush_batch_to_duckdb(db, batch: list[tuple]):
    """
    Persist a batch of per-bill amendment payloads in a single transaction.

    Each item is (amendments, congress, bill_type, bill_number, latest_action_date). Reuses
    load_amendments so the upsert/hash logic lives in exactly one place and
    stays in sync with the raw_amendments schema.
    """
    if not batch:
        return

    db.execute("BEGIN TRANSACTION;")
    try:
        for amendments, congress, bill_type, bill_number, latest_action_date in batch:
            load_amendments(db, amendments, congress, bill_type, bill_number)
            mark_bill_amendments_processed(
                db, congress, bill_type, bill_number, latest_action_date
            )
        db.execute("COMMIT;")
    except Exception as e:
        db.execute("ROLLBACK;")
        print(f"[DB Writer] Error flushing batch of {len(batch)}: {e}")

def start_pipeline(args):
    member_limit = None if ENV != 'Dev' else 5
    random_sample = getattr(args, "random", False)

    # Check if explicit mode/chamber CLI flags were passed
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
        # Prompt user with the interactive menu
        selection = prompt_ingestion_menu()
        if selection is None:
            return  # User selected Exit
        chamber = selection["chamber"]
        ingestion_mode = selection["ingestion_mode"]
        skip_members = selection["skip_members"]

        # Prompt right after the menu for full refresh vs incremental refresh if not specified via CLI
        if reset is None:
            reset = prompt_refresh_mode()

    else:
        # Resolve chamber configuration from CLI arguments
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

    # Setup phase (table initialization/reset)
    reset_tables = []
    if not skip_members and ingestion_mode in ("members", "all"):
        reset_tables.append("raw_members")
    if ingest_bills:
        reset_tables.append("raw_bills")
    if ingest_amendments:
        reset_tables.append("raw_amendments")
    full_refresh = setup_database(db, reset=reset, reset_tables=reset_tables)
    # Ingestion phase
    run_ingestion(
        db,
        member_limit=member_limit,
        random_sample=random_sample,
        use_async=getattr(args, "use_async", False),
        members_only=members_only,
        skip_members=skip_members,
        ingest_bills=ingest_bills,
        ingest_amendments=ingest_amendments,
        full_refresh=full_refresh,
        chamber=chamber,
    )

    db.close()
    print("\nDone. Run `uv run dbt build` from the dbt/ directory.")
