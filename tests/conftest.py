"""pytest fixtures shared across the suite."""
import pytest

import wswdy.config as _cfg
from wswdy.db import connect, init_schema


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    _cfg.get_settings.cache_clear()
    yield
    _cfg.get_settings.cache_clear()


@pytest.fixture
def db(tmp_path):
    """Per-test SQLite connection in a temp file (avoids shared-memory pitfalls)."""
    conn = connect(str(tmp_path / "test.db"))
    init_schema(conn)
    yield conn
    conn.close()
