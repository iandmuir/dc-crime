"""Tiny key/value store for app-level state (e.g. scanner cursors)."""
import sqlite3


def get(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute("SELECT value FROM app_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_value(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        """INSERT INTO app_state(key, value) VALUES(?, ?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                          updated_at=CURRENT_TIMESTAMP""",
        (key, value),
    )
    db.commit()
