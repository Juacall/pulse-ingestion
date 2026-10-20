import pytest


@pytest.fixture(autouse=True)
def local_database_only(monkeypatch):
    """Keep tests on temporary local DuckDB files, never the MotherDuck database from .env."""
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.setenv("ALLOW_LOCAL_DB_FALLBACK", "true")
