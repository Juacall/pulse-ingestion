import asyncio
import sys
from pathlib import Path

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import fetch_amendments_for_bill, fetch_summary_for_bill
from database import mark_bill_amendments_processed
from helpers import parse_api_date
from ingestion import load_amendments

# Bounded queue and concurrency limits to prevent memory bloat and rate-limiting
QUEUE_MAX_SIZE = 500
NUM_AMENDMENT_WORKERS = 5
NUM_SUMMARY_WORKERS = 5
SEMAPHORE = asyncio.Semaphore(NUM_AMENDMENT_WORKERS)


async def summary_worker(worker_id: int, queue: asyncio.Queue, db_writer_queue: asyncio.Queue):
    """
    Consumer Worker: Continuously pulls unique bills from the queue, fetches
    their CRS summary text using fetch_summary_for_bill, and queues the write.
    """
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        congress, bill_type, bill_number = item
        bill_ref = f"{congress}-{bill_type}-{bill_number}"

        async with SEMAPHORE:
            try:
                summary_text = await asyncio.to_thread(
                    fetch_summary_for_bill, congress, bill_type, bill_number
                )

                if summary_text:
                    await db_writer_queue.put((
                        "UPDATE_SUMMARY", congress, bill_type, bill_number, summary_text
                    ))
                    print(f"[Summary Worker {worker_id}] Fetched CRS summary for {bill_ref}")
                else:
                    print(f"[Summary Worker {worker_id}] No summary available yet for {bill_ref}")

            except Exception as e:
                print(f"[Summary Worker {worker_id}] Error fetching summary for {bill_ref}: {e}")
            finally:
                queue.task_done()


async def amendment_worker(
        worker_id: int,
        queue: asyncio.Queue,
        write_queue: asyncio.Queue,
        existing_timestamps: dict,
        stats: dict,
):
    """
    Consumer Worker: Continuously pulls unique bills from the queue, fetches
    their amendments, and hands the DB write off to db_writer_worker via write_queue.
    """
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        congress, bill_type, bill_number, update_date_raw = item
        key = (congress, bill_type, bill_number)
        bill_ref = f"{congress}-{bill_type}-{bill_number}"

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

        async with SEMAPHORE:
            try:
                amendments = await asyncio.to_thread(
                    fetch_amendments_for_bill, congress, bill_type, bill_number
                )

                stats["fetched_amendments"] += 1
                stats["bills_processed"] += 1
                await write_queue.put((
                    "AMENDMENTS", amendments, congress, bill_type, bill_number, update_date_raw
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


async def _ingest_bill_amendments_async(
        db,
        bills,
        existing_timestamps: dict,
        full_refresh: bool = False,
):
    """Fetch amendments and CRS summaries concurrently and serialize DuckDB writes."""
    amdt_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    summary_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    write_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    stats = {
        "bills_processed": 0,
        "bills_without_amendments": 0,
        "fetched_amendments": 0,
        "skipped_amendments": 0,
    }
    timestamps = {} if full_refresh else existing_timestamps

    writer = asyncio.create_task(db_writer_worker(db, write_queue))

    amdt_workers = [
        asyncio.create_task(amendment_worker(i, amdt_queue, write_queue, timestamps, stats))
        for i in range(NUM_AMENDMENT_WORKERS)
    ]
    summary_workers = [
        asyncio.create_task(summary_worker(i, summary_queue, write_queue))
        for i in range(NUM_SUMMARY_WORKERS)
    ]

    items = list(_amendment_bill_items(bills))
    for (congress, bill_type, bill_number), action_date in items:
        await amdt_queue.put((congress, bill_type, bill_number, action_date))
        await summary_queue.put((congress, bill_type, bill_number))

    # Wait for amendment tasks to complete
    await amdt_queue.join()
    for _ in range(NUM_AMENDMENT_WORKERS):
        await amdt_queue.put(None)
    await asyncio.gather(*amdt_workers)

    # Wait for summary tasks to complete
    await summary_queue.join()
    for _ in range(NUM_SUMMARY_WORKERS):
        await summary_queue.put(None)
    await asyncio.gather(*summary_workers)

    # Flush database writer
    await write_queue.join()
    await write_queue.put(None)
    await writer

    print("\n-- Streaming Pipeline Ingestion Complete --")
    print(f"  Bills considered: {len(items)}")
    print(f"  Bills processed: {stats['bills_processed']}")
    print(f"  Bills skipped (no changes): {stats['skipped_amendments']}")
    print(f"  Bills with no amendments: {stats['bills_without_amendments']}")
    print(f"  Amendment Packages Fetched: {stats['fetched_amendments']}")
    return len(items), stats["fetched_amendments"], stats["skipped_amendments"]


async def db_writer_worker(db, write_queue: asyncio.Queue):
    """
    Single-consumer loop that owns the DuckDB write connection.
    Handles both amendment loading and CRS summary updates.
    """
    batch = []

    while True:
        item = await write_queue.get()
        if item is None:
            _flush_batch_to_duckdb(db, batch)
            batch.clear()
            write_queue.task_done()
            break

        batch.append(item)

        if len(batch) >= 50 or write_queue.empty():
            _flush_batch_to_duckdb(db, batch)
            batch.clear()

        write_queue.task_done()


def _flush_batch_to_duckdb(db, batch: list[tuple]):
    """Persist a batch of amendment and summary payloads in a single transaction."""
    if not batch:
        return

    db.execute("BEGIN TRANSACTION;")
    try:
        for item in batch:
            op_type = item[0]
            if op_type == "AMENDMENTS":
                _, amendments, congress, bill_type, bill_number, latest_action_date = item
                load_amendments(db, amendments, congress, bill_type, bill_number)
                mark_bill_amendments_processed(
                    db, congress, bill_type, bill_number, latest_action_date
                )
            elif op_type == "UPDATE_SUMMARY":
                _, congress, bill_type, bill_number, summary_text = item
                db.execute(
                    """
                    UPDATE main.raw_bills
                    SET summary = ?
                    WHERE congress = ? AND bill_type = ? AND bill_number = ?
                    """,
                    [summary_text, congress, bill_type, bill_number]
                )
        db.execute("COMMIT;")
    except Exception as e:
        db.execute("ROLLBACK;")
        print(f"[DB Writer] Error flushing batch of {len(batch)}: {e}")
