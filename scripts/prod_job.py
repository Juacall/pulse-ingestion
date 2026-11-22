"""
scripts/prod_job.py

Non-interactive production job for ephemeral runners (e.g. Cloud Run Jobs):

  1. hydrate a fresh local dev.duckdb with the needed tables from MotherDuck
  2. read the pipeline watermark
  3. run the ingestion pipeline
  4. stop if nothing changed
  5. dbt build downstream models, sync to MotherDuck
  6. record the new watermark in MotherDuck (only after a successful sync)
  7. invalidate API caches

Usage: python scripts/prod_job.py --pipeline fec
Exits non-zero on any failure so the runner can retry.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

# Work on the hydrated local file; database.get_db_connection must not write straight to MotherDuck.
from dotenv import load_dotenv

# Load .env first so ENV=dev there is respected; prod is the default.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")
os.environ.setdefault("ENV", "prod")
os.environ["PULSE_FORCE_LOCAL"] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb

from config import DB_PATH, DEFAULT_CHAMBER
from post_ingest import DBT_DIR, PIPELINE_REFRESH, run_dbt_build
from sync_motherduck import MD_DATABASE, get_table_hash, notify_cache_invalidation, sync_motherduck
from watermarks import WATERMARK_SOURCES, compute_new_watermark, get_watermark, set_watermark

REQUIRED_ENV = ["MOTHERDUCK_TOKEN", "INTERNAL_API_SECRET"]
CONGRESS_PIPELINES = {"members", "bills", "amendments"}
REBUILT_TYPES = {"model", "seed", "snapshot"}
INVALIDATION_ATTEMPTS = 3

# Tables an ingest script reads or writes directly, beyond what dbt lineage pulls in.
EXTRA_TABLES = {
    "members": ["raw_members", "raw_bills", "raw_amendments", "bill_amendment_ingestion_state",
                "raw_census__cd11920_county20"],
    "bills": ["raw_members", "raw_bills", "raw_amendments", "bill_amendment_ingestion_state"],
    "amendments": ["raw_members", "raw_bills", "raw_amendments", "bill_amendment_ingestion_state"],
    "votes": ["fact_member_votes", "raw_members"],
    "elections": ["fct_elections", "fct_race_candidates", "dim_politicians", "dim_seats"],
    "candidates": ["raw_fec_candidates", "stg_fec_candidates", "dim_politicians"],
    "fec": ["dim_politicians", "politician_fec_summary", "politician_top_donors",
            "stg_fec_independent_expenditures"],
}

# Tables whose content hash decides "did anything change" for pipelines with no timestamp column.
CHANGE_TABLES = {
    "fec": ["politician_fec_summary", "politician_top_donors", "stg_fec_independent_expenditures"],
}


def is_dev() -> bool:
    return os.environ.get("ENV") == "dev"


def validate_env():
    required = REQUIRED_ENV + ["DEV_SERVER_URL" if is_dev() else "PROD_SERVER_URL"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")


def _dbt_ls(select: list[str], exclude: list[str] | None = None) -> list[tuple[str, str]]:
    """Return (resource_type, relation name) for the nodes dbt selects."""
    from dbt.cli.main import dbtRunner

    args = ["ls", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
            "--output", "json", "--output-keys", "name", "resource_type", "relation_name",
            "--select", *select]
    if exclude:
        args += ["--exclude", *exclude]
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt ls failed for {select}: {result.exception}")

    nodes = []
    for line in result.result or []:
        try:
            node = json.loads(line)
        except (TypeError, ValueError):
            continue
        relation = node.get("relation_name") or node["name"]
        nodes.append((node["resource_type"], relation.split(".")[-1].strip('"')))
    return nodes


def tables_to_hydrate(pipeline: str) -> set[str]:
    """Tables the run needs locally: ingest inputs/outputs plus every dbt parent it will not rebuild."""
    needed = {"etl_watermarks", *EXTRA_TABLES[pipeline]}
    config = PIPELINE_REFRESH[pipeline]
    select = config["select"]
    if select:
        rebuilt = {name for kind, name in _dbt_ls(select, config.get("exclude")) if kind in REBUILT_TYPES}
        lineage = _dbt_ls([f"@{s.rstrip('+')}" for s in select])
        needed |= {name for kind, name in lineage if kind in REBUILT_TYPES | {"source"}} - rebuilt
    return needed


def missing_parent_models(pipeline: str) -> list[str]:
    """Parent models the build needs but the local database lacks (e.g. never built in dev)."""
    config = PIPELINE_REFRESH[pipeline]
    select = config["select"]
    if not select:
        return []
    rebuilt = {name for kind, name in _dbt_ls(select, config.get("exclude")) if kind in REBUILT_TYPES}
    lineage = _dbt_ls([f"@{s.rstrip('+')}" for s in select])
    parents = {name for kind, name in lineage if kind in REBUILT_TYPES} - rebuilt

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        existing = {row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()}
    finally:
        conn.close()
    return sorted(parents - existing)


def hydrate_local(pipeline: str) -> None:
    """Replace the local database with a copy of the needed MotherDuck tables."""
    tables = tables_to_hydrate(pipeline)
    for suffix in ("", ".wal"):
        Path(f"{DB_PATH}{suffix}").unlink(missing_ok=True)

    conn = duckdb.connect(str(DB_PATH))
    try:
        # Alias must equal the database name: MotherDuck views reference it as <db>.main.<table>.
        conn.execute(f"ATTACH 'md:{MD_DATABASE}' AS {MD_DATABASE} (READ_ONLY)")
        remote = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{MD_DATABASE}' AND table_schema = 'main'"
            ).fetchall()
        }
        ddl = dict(conn.execute(
            "SELECT table_name, sql FROM duckdb_tables() "
            f"WHERE database_name = '{MD_DATABASE}' AND schema_name = 'main'"
        ).fetchall())
        for name in sorted(tables):
            if name not in remote:
                print(f"  (not in MotherDuck yet, skipping) {name}")
                continue
            source = f'{MD_DATABASE}.main."{name}"'
            conn.execute(f'DROP TABLE IF EXISTS main."{name}"')
            if ddl.get(name):
                # Recreate from the remote DDL so primary keys survive; CTAS would drop them and break ON CONFLICT upserts.
                conn.execute(ddl[name])
                conn.execute(f'INSERT INTO main."{name}" BY NAME SELECT * FROM {source}')
            else:
                conn.execute(f'CREATE TABLE main."{name}" AS SELECT * FROM {source}')
            print(f"  hydrated {name}")
        conn.execute(f"DETACH {MD_DATABASE}")
    finally:
        conn.close()


def run_ingestion_for(pipeline: str, chamber: str = DEFAULT_CHAMBER) -> None:
    if pipeline in CONGRESS_PIPELINES:
        from database import get_db_connection
        from pipeline import run_ingestion, setup_database

        db = get_db_connection()
        try:
            setup_database(db, reset=False, reset_tables=[])
            run_ingestion(
                db,
                member_limit=None,
                use_async=True,
                members_only=pipeline == "members",
                skip_members=pipeline != "members",
                ingest_bills=pipeline == "bills",
                ingest_amendments=pipeline == "amendments",
                chamber=chamber,
            )
        finally:
            db.close()
    else:
        from pipeline import run_standalone_pipeline
        run_standalone_pipeline(pipeline)


def _content_hash(conn, tables: list[str]) -> str:
    catalog = conn.execute("SELECT current_database()").fetchone()[0]
    return "|".join(get_table_hash(conn, catalog, table) for table in tables)


def _change_tables(pipeline: str) -> list[str]:
    return CHANGE_TABLES.get(pipeline, [WATERMARK_SOURCES[pipeline]["table"]])


def invalidate_caches(prefixes: list[str]) -> bool:
    ok = True
    for prefix in prefixes:
        for attempt in range(1, INVALIDATION_ATTEMPTS + 1):
            if notify_cache_invalidation(prefix):
                break
            time.sleep(2 ** attempt)
        else:
            ok = False
    return ok


def run_job(pipeline: str, chamber: str = DEFAULT_CHAMBER) -> int:
    validate_env()
    watermark_name = WATERMARK_SOURCES[pipeline]["name"]

    if is_dev():
        print(f"\n-- ENV=dev: using the existing local database ({DB_PATH}); skipping MotherDuck pull --")
    else:
        print(f"\n-- Hydrating local database from MotherDuck ({MD_DATABASE}) --")
        hydrate_local(pipeline)

    conn = duckdb.connect(str(DB_PATH))
    try:
        previous = get_watermark(conn, watermark_name)
        hash_before = _content_hash(conn, _change_tables(pipeline))
    finally:
        conn.close()
    print(f"Last watermark for {watermark_name}: {previous}")

    print(f"\n-- Running {pipeline} pipeline --")
    run_ingestion_for(pipeline, chamber)

    conn = duckdb.connect(str(DB_PATH))
    try:
        new_watermark, records = compute_new_watermark(conn, pipeline, previous)
        if WATERMARK_SOURCES[pipeline]["column"] is None:
            changed = _content_hash(conn, _change_tables(pipeline)) != hash_before
        else:
            changed = records > 0
    finally:
        conn.close()

    if not changed:
        print("\nNo updates detected; MotherDuck and caches left untouched.")
        return 0

    config = PIPELINE_REFRESH[pipeline]
    if config["select"]:
        print(f"\n-- Rebuilding dbt models: {' '.join(config['select'])} --")
        missing = missing_parent_models(pipeline)
        if missing:
            print(f"Also building parent models missing locally: {' '.join(missing)}")
        if not run_dbt_build(config["select"] + missing, config.get("exclude")):
            print("dbt build failed; not syncing.")
            return 1

    print("\n-- Syncing to MotherDuck --")
    # In dev the local file is the source of truth for watermarks, so they sync with everything else.
    if is_dev():
        local = duckdb.connect(str(DB_PATH))
        try:
            set_watermark(local, watermark_name, new_watermark, records)
        finally:
            local.close()
        sync_motherduck()
        print(f"Updated watermark for {watermark_name}: {new_watermark} ({records} records)")
    else:
        sync_motherduck(exclude_tables={"etl_watermarks"})
        # Watermarks are written only once the data is live, so a failed run is retried from the old one.
        md = duckdb.connect(f"md:{MD_DATABASE}")
        try:
            set_watermark(md, watermark_name, new_watermark, records)
        finally:
            md.close()
        print(f"Updated watermark for {watermark_name}: {new_watermark} ({records} records)")

    print("\n-- Invalidating caches --")
    if not invalidate_caches(config["cache_prefixes"]):
        print("Cache invalidation failed after retries.")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one production ingestion pipeline end to end.")
    parser.add_argument("--pipeline", choices=sorted(PIPELINE_REFRESH), default=os.environ.get("PIPELINE"),
                        help="Pipeline to run (or set the PIPELINE env var).")
    parser.add_argument("--chamber", choices=["house", "senate", "both"], default=DEFAULT_CHAMBER,
                        help="Chamber for members/bills/amendments (default: CHAMBER env var, else house).")
    args = parser.parse_args()
    if not args.pipeline:
        parser.error("--pipeline (or PIPELINE env var) is required")
    return run_job(args.pipeline, args.chamber)


if __name__ == "__main__":
    sys.exit(main())
