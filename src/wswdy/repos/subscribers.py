"""Subscriber CRUD."""
import sqlite3
from datetime import UTC, datetime

VALID_STATUSES = {"PENDING", "APPROVED", "REJECTED", "UNSUBSCRIBED"}


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def insert_pending(
    db: sqlite3.Connection, *,
    sid: str, display_name: str, email: str | None, phone: str | None,
    preferred_channel: str, address_text: str, lat: float, lon: float, radius_m: int,
) -> str:
    db.execute(
        """INSERT INTO subscribers
           (id, display_name, email, phone, preferred_channel,
            address_text, lat, lon, radius_m, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')""",
        (sid, display_name, email, phone, preferred_channel,
         address_text, lat, lon, radius_m),
    )
    db.commit()
    return sid


def get(db: sqlite3.Connection, sid: str) -> dict | None:
    row = db.execute("SELECT * FROM subscribers WHERE id = ?", (sid,)).fetchone()
    return dict(row) if row else None


def set_status(db: sqlite3.Connection, sid: str, status: str) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    now = _utcnow()
    if status == "APPROVED":
        db.execute("UPDATE subscribers SET status=?, approved_at=? WHERE id=?",
                   (status, now, sid))
    elif status == "UNSUBSCRIBED":
        db.execute("UPDATE subscribers SET status=?, unsubscribed_at=? WHERE id=?",
                   (status, now, sid))
    else:
        db.execute("UPDATE subscribers SET status=? WHERE id=?", (status, sid))
    db.commit()


def set_last_sent(db: sqlite3.Connection, sid: str, when_iso: str) -> None:
    db.execute("UPDATE subscribers SET last_sent_at=? WHERE id=?", (when_iso, sid))
    db.commit()


def list_active(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute("SELECT * FROM subscribers WHERE status='APPROVED' "
                      "ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def list_by_status(db: sqlite3.Connection, status: str) -> list[dict]:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    rows = db.execute("SELECT * FROM subscribers WHERE status=? ORDER BY created_at DESC",
                      (status,)).fetchall()
    return [dict(r) for r in rows]


def delete(db: sqlite3.Connection, sid: str) -> bool:
    """Hard-delete a subscriber and its send_log rows. Returns True if a row was removed."""
    cur = db.execute("DELETE FROM subscribers WHERE id=?", (sid,))
    db.execute("DELETE FROM send_log WHERE subscriber_id=?", (sid,))
    db.commit()
    return cur.rowcount > 0
