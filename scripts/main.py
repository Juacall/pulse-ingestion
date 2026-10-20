"""
scripts/main.py

Main entrypoint for Congress data ingestion.
Supports interactive database setup and CLI options for flexible data scoping (--limit, --full, --random, --members-only, --skip-members, --house, --senate).
"""

import argparse
from pathlib import Path
import sys

# Add script folder to path if executed standalone
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    DB_PATH,
    DEFAULT_CHAMBER,
    MEMBER_LIMIT as DEFAULT_MEMBER_LIMIT,
)
from pipeline import (
    prompt_ingestion_menu,
    prompt_refresh_mode,
    prompt_reset_tables,
    start_pipeline,
)

# ── CLI & Main ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest Congress data into DuckDB for downstream dbt transformations."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run ingestion for all current Congress members (no limit).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_MEMBER_LIMIT,
        help=f"Cap the number of members to ingest (default: {DEFAULT_MEMBER_LIMIT}). Ignored if --full is set.",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Randomly sample current Congress members up to --limit.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        default=None,
        help="Force table recreation/reset without prompting.",
    )
    parser.add_argument(
        "--no-reset",
        action="store_false",
        dest="reset",
        help="Skip table reset without prompting (preserve existing data).",
    )
    parser.add_argument(
        "--async",
        action="store_true",
        dest="use_async",  # Avoids conflict with Python's 'async' keyword
        help="Run ingestion using async HTTP workers and streaming queue.",
    )

    ingestion_group = parser.add_mutually_exclusive_group()
    ingestion_group.add_argument(
        "--members-only",
        action="store_true",
        help="Only fetch and update raw_members metadata; skip bills and amendments.",
    )
    ingestion_group.add_argument(
        "--bills-only",
        action="store_true",
        help="Fetch and update raw_bills only; do not fetch amendments.",
    )
    ingestion_group.add_argument(
        "--amendments-only",
        action="store_true",
        help="Fetch amendments for bills already stored in raw_bills.",
    )
    parser.add_argument(
        "--skip-members",
        action="store_true",
        help="Skip fetching member metadata and read existing members directly from DuckDB to fetch bills.",
    )

    # Chamber selection options
    parser.add_argument(
        "--house",
        "-house",
        action="store_true",
        help="Fetch House members only (default).",
    )
    parser.add_argument(
        "--senate",
        "-senate",
        action="store_true",
        help="Fetch Senate members only.",
    )
    parser.add_argument(
        "--chamber",
        choices=["house", "senate", "both"],
        default=None,
        help=f"Specify chamber: 'house', 'senate', or 'both' (default from config: '{DEFAULT_CHAMBER}').",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    start_pipeline(args)




if __name__ == "__main__":
    main()
