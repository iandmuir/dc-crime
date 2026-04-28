import sqlite3
from wswdy.db import connect, init_schema


def test_init_schema_creates_all_tables(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in rows}
    assert {
        "subscribers", "crimes", "send_log", "fetch_log", "admin_alerts"
    } <= names


def test_init_schema_is_idempotent(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    init_schema(conn)  # should not raise
    conn.execute("INSERT INTO subscribers (id, display_name, preferred_channel, "
                 "address_text, lat, lon, radius_m) "
                 "VALUES ('a', 'A', 'email', '1 St', 38.9, -77.0, 1000)")
    conn.commit()


def test_connect_uses_wal_mode(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_connect_rows_are_dicts(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    conn.execute("INSERT INTO subscribers (id, display_name, preferred_channel, "
                 "address_text, lat, lon, radius_m) VALUES "
                 "('a', 'A', 'email', '1', 1.0, 2.0, 500)")
    conn.commit()
    row = conn.execute("SELECT * FROM subscribers WHERE id='a'").fetchone()
    assert row["display_name"] == "A"
