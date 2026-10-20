from pathlib import Path
import sys

import duckdb
import pytest
from voyageai.error import APIConnectionError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from rag import indexer


class FakeEmbeddingResponse:
    def __init__(self, embeddings):
        self.embeddings = embeddings


class FakeVoyageClient:
    def __init__(self, **kwargs):
        self.fail = False
        self.embedded_texts = []

    def embed(self, texts, **kwargs):
        if self.fail:
            raise APIConnectionError("Voyage unavailable")
        self.embedded_texts.extend(texts)
        vectors = [
            [float(index)] * indexer.EMBEDDING_DIMENSIONS
            for index, _ in enumerate(texts)
        ]
        return FakeEmbeddingResponse(vectors)


def _create_source_view(db_path, documents):
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE OR REPLACE TABLE main.rag_test_source (
            doc_id VARCHAR,
            member_id VARCHAR,
            chamber VARCHAR,
            state_code VARCHAR,
            party_name VARCHAR,
            activity_type VARCHAR,
            page_content VARCHAR
        )
        """
    )
    conn.execute("DROP VIEW IF EXISTS main.view_rag_documents")
    if documents:
        conn.executemany(
            f"""
            INSERT INTO main.rag_test_source VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            documents,
        )
    conn.execute(
        "CREATE VIEW main.view_rag_documents AS "
        "SELECT * FROM main.rag_test_source"
    )
    conn.close()


def _indexed_rows(db_path):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(
            """
            SELECT doc_id, page_content, content_hash, embedding_model,
                   embedding_dimensions, array_length(embedding)
            FROM main.doc_embeddings
            ORDER BY doc_id
            """
        ).fetchall()
    finally:
        conn.close()


def test_incremental_index_updates_changed_and_removes_stale_vectors(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "rag.duckdb"
    voyage_client = FakeVoyageClient()
    monkeypatch.setattr(indexer.voyageai, "Client", lambda **kwargs: voyage_client)
    monkeypatch.setattr(indexer.time, "sleep", lambda _delay: None)
    monkeypatch.setenv("VOYAGE_API_KEY", "test-key")

    _create_source_view(
        db_path,
        [
            ("doc-a", "member-a", "House", "WA", "D", "sponsored", "Alpha content"),
            ("doc-b", "member-b", "Senate", "OR", "R", "cosponsored", "Beta content"),
        ],
    )
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE main.doc_embeddings "
        "(doc_id VARCHAR, embedding FLOAT[1536])"
    )
    conn.execute(
        "INSERT INTO main.doc_embeddings VALUES "
        "('legacy', range(1536)::FLOAT[1536])"
    )
    conn.close()

    indexer.build_vector_index(db_path)
    first_rows = _indexed_rows(db_path)
    assert len(first_rows) == 2
    assert len(voyage_client.embedded_texts) == 2
    assert all(row[4:] == (512, 512) for row in first_rows)

    indexer.build_vector_index(db_path)
    assert len(voyage_client.embedded_texts) == 2
    assert _indexed_rows(db_path) == first_rows

    _create_source_view(
        db_path,
        [
            ("doc-a", "member-a", "House", "WA", "D", "sponsored", "Updated alpha"),
            ("doc-c", "member-c", "House", "CA", "D", "sponsored", "New gamma"),
        ],
    )
    indexer.build_vector_index(db_path)
    updated_rows = _indexed_rows(db_path)
    assert [row[0] for row in updated_rows] == ["doc-a::0", "doc-c::0"]
    assert updated_rows[0][1] == "Updated alpha"
    assert len(voyage_client.embedded_texts) == 4

    previous_rows = updated_rows
    voyage_client.fail = True
    _create_source_view(
        db_path,
        [
            ("doc-a", "member-a", "House", "WA", "D", "sponsored", "Failed update"),
            ("doc-c", "member-c", "House", "CA", "D", "sponsored", "New gamma"),
        ],
    )
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        indexer.build_vector_index(db_path)

    assert _indexed_rows(db_path) == previous_rows
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'doc_embeddings_build'"
        ).fetchone()[0] == 0
    finally:
        conn.close()

    voyage_client.fail = False
    monkeypatch.delenv("VOYAGE_API_KEY")
    _create_source_view(db_path, [])
    indexer.build_vector_index(db_path)
    assert _indexed_rows(db_path) == []
