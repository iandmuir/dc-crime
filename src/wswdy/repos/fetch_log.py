"""Fetch log — one row per MPD fetch attempt."""
import sqlite3


def record_success(db: sqlite3.Connection, *, added: int, updated: int) -> None:
    db.execute(
        "INSERT INTO fetch_log (status, crimes_added, crimes_updated) VALUES ('ok', ?, ?)",
        (added, updated),
    )
    db.commit()


def record_failure(db: sqlite3.Connection, *, error: str) -> None:
    db.execute(
        "INSERT INTO fetch_log (status, error) VALUES ('failed', ?)",
        (error,),
    )
    db.commit()


def last_successful(db: sqlite3.Connection) -> dict | None:
    row = db.execute(
        "SELECT * FROM fetch_log WHERE status='ok' ORDER BY fetched_at DESC, id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def last_attempt(db: sqlite3.Connection) -> dict | None:
    row = db.execute(
        "SELECT * FROM fetch_log ORDER BY fetched_at DESC, id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def last_with_data(db: sqlite3.Connection) -> dict | None:
    """Most recent fetch that actually moved data — i.e. added at least one
    new crime. Updates alone are routine churn (MPD re-publishes the rolling
    window every fetch), so we key on crimes_added > 0 to answer "when did
    we last see something new?"."""
    row = db.execute(
        "SELECT * FROM fetch_log WHERE status='ok' AND crimes_added > 0 "
        "ORDER BY fetched_at DESC, id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None
