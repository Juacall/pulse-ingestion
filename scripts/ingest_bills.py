import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api import fetch_legislation_for_member, fetch_summary_for_bill
from database import addColumn
from ingestion import load_bills

NUM_BILL_WORKERS = 5


def ingest_member_bills(db, members, chamber: str = "house", fetch_summaries_sync: bool = False):
    """Fetch bills concurrently and serialize their DuckDB upserts on this thread."""
    unique_bills = {}
    addColumn(db, "raw_bills", "summary", "VARCHAR")
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

            # In synchronous mode, fetch summary inline if missing
            if fetch_summaries_sync:
                for b in sponsored + cosponsored:
                    if not b.get("summary"):
                        summary_text = fetch_summary_for_bill(b.get("congress"), b.get("type"), b.get("number"))
                        if summary_text:
                            b["summary"] = summary_text

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
