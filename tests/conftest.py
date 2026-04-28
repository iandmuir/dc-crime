"""pytest fixtures shared across the suite."""
import pytest
from wswdy.db import connect, init_schema


@pytest.fixture
def db(tmp_path):
    """Per-test SQLite connection in a temp file (avoids shared-memory pitfalls)."""
    conn = connect(str(tmp_path / "test.db"))
    init_schema(conn)
    yield conn
    conn.close()
