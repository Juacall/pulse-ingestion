from pathlib import Path
import hashlib
import json
import os
import time

import duckdb
from langchain_text_splitters import RecursiveCharacterTextSplitter
import voyageai
from voyageai.error import (
    APIConnectionError,
    RateLimitError,
    ServerError,
    ServiceUnavailableError,
)

DB_PATH = Path(__file__).resolve().parents[2] / "dev.duckdb"
EMBEDDING_MODEL = "voyage-3-lite"
EMBEDDING_DIMENSIONS = 512
EMBEDDING_BATCH_SIZE = 64
EMBEDDING_REQUEST_TIMEOUT = 30
EMBEDDING_MAX_ATTEMPTS = 3
SOURCE_BATCH_SIZE = 500
STAGING_TABLE = "doc_embeddings_build"
SOURCE_TEMP_TABLE = "rag_documents_source"

INDEX_COLUMNS = {
    "doc_id",
    "source_doc_id",
    "chunk_index",
    "content_hash",
    "embedding_model",
    "embedding_dimensions",
    "member_id",
    "chamber",
    "state_code",
    "party_name",
    "activity_type",
    "page_content",
    "embedding",
}
CHUNK_COLUMNS = (
    "doc_id",
    "source_doc_id",
    "chunk_index",
    "content_hash",
    "embedding_model",
    "embedding_dimensions",
    "member_id",
    "chamber",
    "state_code",
    "party_name",
    "activity_type",
    "page_content",
)
INDEX_INSERT_COLUMNS = (*CHUNK_COLUMNS, "embedding")


def _content_hash(values: tuple) -> str:
    payload = json.dumps(
        [EMBEDDING_MODEL, EMBEDDING_DIMENSIONS, *values],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _bulk_insert_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: tuple[str, ...],
    rows: list[tuple],
) -> None:
    if not rows:
        return

    placeholders = ", ".join(
        "(" + ", ".join("?" for _ in columns) + ")" for _ in rows
    )
    parameters = [value for row in rows for value in row]
    conn.execute(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {placeholders}",
        parameters,
    )


def _embed_documents(
    voyage_client: voyageai.Client,
    texts: list[str],
    completed_count: int,
) -> list[list[float]]:
    embeddings = []
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        batch = texts[start : start + EMBEDDING_BATCH_SIZE]
        batch_start = completed_count + start + 1
        batch_end = completed_count + start + len(batch)
        print(
            f"Requesting Voyage embeddings for documents "
            f"{batch_start}-{batch_end} (batch of {len(batch)}; "
            f"timeout {EMBEDDING_REQUEST_TIMEOUT}s)...",
            flush=True,
        )
        for attempt in range(1, EMBEDDING_MAX_ATTEMPTS + 1):
            try:
                request_started = time.monotonic()
                response = voyage_client.embed(
                    batch,
                    model=EMBEDDING_MODEL,
                    input_type="document",
                    output_dimension=EMBEDDING_DIMENSIONS,
                )
                print(
                    f"Received Voyage embeddings for documents "
                    f"{batch_start}-{batch_end} in "
                    f"{time.monotonic() - request_started:.1f}s.",
                    flush=True,
                )
                break
            except (
                APIConnectionError,
                RateLimitError,
                ServerError,
                ServiceUnavailableError,
            ) as error:
                if attempt == EMBEDDING_MAX_ATTEMPTS:
                    raise RuntimeError(
                        f"Voyage embedding request for documents "
                        f"{batch_start}-{batch_end} failed after "
                        f"{EMBEDDING_MAX_ATTEMPTS} attempts: {error}"
                    ) from error
                delay = 2**attempt
                print(
                    f"Voyage request for documents {batch_start}-{batch_end} "
                    f"failed on attempt {attempt}/{EMBEDDING_MAX_ATTEMPTS}: "
                    f"{error}. Retrying in {delay}s.",
                    flush=True,
                )
                time.sleep(delay)
        embeddings.extend(response.embeddings)

    for embedding in embeddings:
        if len(embedding) != EMBEDDING_DIMENSIONS:
            raise ValueError(
                f"{EMBEDDING_MODEL} returned a {len(embedding)}-dimension vector; "
                f"expected {EMBEDDING_DIMENSIONS}."
            )
    return embeddings


def _embedding_dimension(conn: duckdb.DuckDBPyConnection) -> int | None:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if "doc_embeddings" not in tables:
        return None

    columns = conn.execute("DESCRIBE main.doc_embeddings").fetchall()
    embedding_type = next(
        (row[1] for row in columns if row[0] == "embedding"), None
    )
    if embedding_type is None:
        raise RuntimeError("main.doc_embeddings is missing its embedding column.")

    if embedding_type.startswith("FLOAT[") and embedding_type.endswith("]"):
        return int(embedding_type[6:-1])
    raise RuntimeError(
        "main.doc_embeddings.embedding must be a fixed-size FLOAT array; "
        f"found {embedding_type}."
    )


def _index_is_compatible(conn: duckdb.DuckDBPyConnection) -> bool:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if "doc_embeddings" not in tables:
        return False

    columns = {
        row[0]: row[1]
        for row in conn.execute("DESCRIBE main.doc_embeddings").fetchall()
    }
    return (
        INDEX_COLUMNS.issubset(columns)
        and columns["embedding"] == f"FLOAT[{EMBEDDING_DIMENSIONS}]"
    )


def _create_embeddings_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> None:
    if table_name not in {"doc_embeddings", STAGING_TABLE}:
        raise ValueError(f"Unsupported embeddings table: {table_name}")
    conn.execute(
        f"""
        CREATE TABLE main.{table_name} (
            doc_id VARCHAR PRIMARY KEY,
            source_doc_id VARCHAR,
            chunk_index INTEGER,
            content_hash VARCHAR,
            embedding_model VARCHAR,
            embedding_dimensions INTEGER,
            member_id VARCHAR,
            chamber VARCHAR,
            state_code VARCHAR,
            party_name VARCHAR,
            activity_type VARCHAR,
            page_content VARCHAR,
            embedding FLOAT[512]
        )
        """
    )


def _insert_document_batch(
    conn: duckdb.DuckDBPyConnection,
    voyage_client: voyageai.Client,
    documents: list[tuple],
    completed_count: int,
) -> None:
    if not documents:
        return

    embeddings = _embed_documents(
        voyage_client,
        [document[11] for document in documents],
        completed_count,
    )
    if len(embeddings) != len(documents):
        raise RuntimeError(
            f"Voyage returned {len(embeddings)} embeddings for "
            f"{len(documents)} documents."
        )
    print(
        f"Writing embeddings for documents "
        f"{completed_count + 1}-{completed_count + len(documents)} to DuckDB...",
        flush=True,
    )
    write_started = time.monotonic()
    _bulk_insert_rows(
        conn,
        f"main.{STAGING_TABLE}",
        INDEX_INSERT_COLUMNS,
        [(*document, embedding) for document, embedding in zip(documents, embeddings)],
    )
    print(
        f"Saved embeddings for documents "
        f"{completed_count + 1}-{completed_count + len(documents)} "
        f"to DuckDB in {time.monotonic() - write_started:.1f}s.",
        flush=True,
    )


def _prepare_source_chunks(
    conn: duckdb.DuckDBPyConnection,
    text_splitter: RecursiveCharacterTextSplitter,
) -> int:
    conn.execute(
        f"""
        CREATE TEMP TABLE {SOURCE_TEMP_TABLE} (
            doc_id VARCHAR PRIMARY KEY,
            source_doc_id VARCHAR,
            chunk_index INTEGER,
            content_hash VARCHAR,
            embedding_model VARCHAR,
            embedding_dimensions INTEGER,
            member_id VARCHAR,
            chamber VARCHAR,
            state_code VARCHAR,
            party_name VARCHAR,
            activity_type VARCHAR,
            page_content VARCHAR
        )
        """
    )

    print(
        "Materializing main.view_rag_documents once before comparing chunks...",
        flush=True,
    )
    started = time.monotonic()
    source_rows = conn.execute(
        """
        SELECT doc_id, member_id, chamber, state_code, party_name,
               activity_type, page_content
        FROM main.view_rag_documents
        """
    ).fetchall()
    source_count = len(source_rows)
    chunk_count = 0
    for start in range(0, source_count, SOURCE_BATCH_SIZE):
        rows = source_rows[start : start + SOURCE_BATCH_SIZE]
        chunk_rows = []
        for source_doc_id, member_id, chamber, state, party, activity_type, content in rows:
            if source_doc_id is None:
                raise ValueError("view_rag_documents contains a NULL doc_id.")
            chunks = text_splitter.split_text(content or "")
            for chunk_index, chunk in enumerate(chunks):
                doc_id = f"{source_doc_id}::{chunk_index}"
                metadata = (
                    chunk,
                    member_id,
                    chamber,
                    state,
                    party,
                    activity_type,
                )
                chunk_rows.append(
                    (
                        doc_id,
                        source_doc_id,
                        chunk_index,
                        _content_hash(metadata),
                        EMBEDDING_MODEL,
                        EMBEDDING_DIMENSIONS,
                        member_id,
                        chamber,
                        state,
                        party,
                        activity_type,
                        chunk,
                    )
                )
        _bulk_insert_rows(conn, f"{SOURCE_TEMP_TABLE}", CHUNK_COLUMNS, chunk_rows)
        chunk_count += len(chunk_rows)
        print(
            f"Prepared {chunk_count} chunks from {source_count} source records.",
            flush=True,
        )

    print(
        f"Source comparison input ready: {source_count} source records, "
        f"{chunk_count} chunks in {time.monotonic() - started:.1f}s.",
        flush=True,
    )
    return chunk_count


def build_vector_index(db_path: str | Path = DB_PATH) -> None:
    print(f"Opening DuckDB database at {db_path}...", flush=True)
    try:
        conn = duckdb.connect(str(db_path))
    except duckdb.IOException as error:
        if "being used by another process" in str(error).lower():
            raise RuntimeError(
                f"Cannot open {db_path}: another process already has this "
                "DuckDB file open. Close other scripts or database tools "
                "connected to this file, then retry."
            ) from error
        raise

    staging_created = False
    transaction_active = False
    try:
        print("Loading DuckDB vector similarity extension...", flush=True)
        conn.execute("INSTALL vss;")
        conn.execute("LOAD vss;")
        conn.execute("SET hnsw_enable_experimental_persistence = true")

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=150,
        )
        conn.execute(f"DROP TABLE IF EXISTS main.{STAGING_TABLE}")
        _create_embeddings_table(conn, STAGING_TABLE)
        staging_created = True
        chunk_count = _prepare_source_chunks(conn, text_splitter)
        compatible_index = _index_is_compatible(conn)

        if compatible_index:
            changed_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {SOURCE_TEMP_TABLE} AS source
                LEFT JOIN main.doc_embeddings AS stored USING (doc_id)
                WHERE stored.doc_id IS NULL
                   OR stored.content_hash IS DISTINCT FROM source.content_hash
                   OR stored.embedding_model IS DISTINCT FROM source.embedding_model
                   OR stored.embedding_dimensions IS DISTINCT FROM source.embedding_dimensions
                """
            ).fetchone()[0]
            stale_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM main.doc_embeddings AS stored
                WHERE NOT EXISTS (
                    SELECT 1 FROM {SOURCE_TEMP_TABLE} AS source
                    WHERE source.doc_id = stored.doc_id
                )
                """
            ).fetchone()[0]
            unchanged_count = chunk_count - changed_count
        else:
            changed_count = chunk_count
            unchanged_count = 0
            old_dimension = _embedding_dimension(conn)
            stale_count = (
                conn.execute("SELECT COUNT(*) FROM main.doc_embeddings").fetchone()[0]
                if old_dimension is not None
                else 0
            )

        print(
            f"RAG comparison: {unchanged_count} unchanged, "
            f"{changed_count} new/changed, {stale_count} stale chunks.",
            flush=True,
        )

        if changed_count:
            api_key = os.environ.get("VOYAGE_API_KEY")
            if not api_key:
                raise ValueError(
                    "VOYAGE_API_KEY is required to embed new or changed RAG documents."
                )
            voyage_client = voyageai.Client(
                api_key=api_key,
                timeout=EMBEDDING_REQUEST_TIMEOUT,
                max_retries=0,
            )
            changed_rows = conn.execute(
                f"""
                SELECT source.doc_id, source.source_doc_id, source.chunk_index,
                       source.content_hash, source.embedding_model,
                       source.embedding_dimensions, source.member_id,
                       source.chamber, source.state_code, source.party_name,
                       source.activity_type, source.page_content
                FROM {SOURCE_TEMP_TABLE} AS source
                {"LEFT JOIN main.doc_embeddings AS stored USING (doc_id)" if compatible_index else ""}
                {"WHERE stored.doc_id IS NULL OR stored.content_hash IS DISTINCT FROM source.content_hash "
                 "OR stored.embedding_model IS DISTINCT FROM source.embedding_model "
                 "OR stored.embedding_dimensions IS DISTINCT FROM source.embedding_dimensions"
                 if compatible_index else ""}
                ORDER BY source.doc_id
                """
            ).fetchall()
            embedded_count = 0
            for start in range(0, len(changed_rows), EMBEDDING_BATCH_SIZE):
                rows = changed_rows[start : start + EMBEDDING_BATCH_SIZE]
                _insert_document_batch(
                    conn,
                    voyage_client,
                    rows,
                    embedded_count,
                )
                embedded_count += len(rows)

        if compatible_index and changed_count == 0 and stale_count == 0:
            conn.execute(f"DROP TABLE main.{STAGING_TABLE}")
            staging_created = False
            print(
                f"RAG index is current ({chunk_count} chunks); no embeddings "
                "or vector-index changes required.",
                flush=True,
            )
            return

        print(
            f"Applying {changed_count} embeddings and removing {stale_count} "
            "stale chunks...",
            flush=True,
        )
        conn.execute("BEGIN TRANSACTION")
        transaction_active = True
        try:
            if compatible_index:
                conn.execute("DROP INDEX IF EXISTS main.idx_doc_embeddings")
                conn.execute(
                    f"""
                    DELETE FROM main.doc_embeddings AS stored
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {SOURCE_TEMP_TABLE} AS source
                        WHERE source.doc_id = stored.doc_id
                    )
                    OR EXISTS (
                        SELECT 1 FROM {SOURCE_TEMP_TABLE} AS source
                        WHERE source.doc_id = stored.doc_id
                          AND (
                              source.content_hash IS DISTINCT FROM stored.content_hash
                              OR source.embedding_model IS DISTINCT FROM stored.embedding_model
                              OR source.embedding_dimensions IS DISTINCT FROM stored.embedding_dimensions
                          )
                    )
                    """
                )
                conn.execute(
                    f"""
                    INSERT INTO main.doc_embeddings ({', '.join(INDEX_INSERT_COLUMNS)})
                    SELECT {', '.join(INDEX_INSERT_COLUMNS)}
                    FROM main.{STAGING_TABLE}
                    """
                )
            else:
                old_dimension = _embedding_dimension(conn)
                if old_dimension is not None:
                    print(
                        f"Replacing incompatible {old_dimension}-dimension or "
                        "legacy index with the current Voyage index.",
                        flush=True,
                    )
                conn.execute("DROP INDEX IF EXISTS main.idx_doc_embeddings")
                conn.execute("DROP TABLE IF EXISTS main.doc_embeddings")
                conn.execute(f"ALTER TABLE main.{STAGING_TABLE} RENAME TO doc_embeddings")

            remaining_count = conn.execute(
                "SELECT COUNT(*) FROM main.doc_embeddings"
            ).fetchone()[0]
            if remaining_count:
                conn.execute(
                    """
                    CREATE INDEX idx_doc_embeddings
                    ON main.doc_embeddings USING HNSW (embedding)
                    """
                )
            conn.execute("COMMIT")
            transaction_active = False
            staging_created = False
        except Exception:
            conn.execute("ROLLBACK")
            transaction_active = False
            raise

        print(
            f"RAG sync complete: {changed_count} embedded, "
            f"{unchanged_count} unchanged, {stale_count} stale removed; "
            f"{remaining_count} chunks indexed.",
            flush=True,
        )
    finally:
        if transaction_active:
            conn.execute("ROLLBACK")
        if staging_created:
            conn.execute(f"DROP TABLE IF EXISTS main.{STAGING_TABLE}")
        conn.close()


if __name__ == "__main__":
    build_vector_index()
