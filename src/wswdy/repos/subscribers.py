"""Subscriber CRUD."""
import sqlite3
from datetime import UTC, datetime

VALID_STATUSES = {"PENDING", "APPROVED", "REJECTED", "UNSUBSCRIBED"}


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def insert_pending(
    db: sqlite3.Connection, *,
    sid: str, display_name: str, email: str | None, phone: str | None,
    preferred_channel: str, address_text: str, lat: float, lon: float,
    radius_m: int, district: str | None = None,
) -> str:
    db.execute(
        """INSERT INTO subscribers
           (id, display_name, email, phone, preferred_channel,
            address_text, lat, lon, radius_m, status, district)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
        (sid, display_name, email, phone, preferred_channel,
         address_text, lat, lon, radius_m, district),
    )
    db.commit()
    return sid


def set_district(
    db: sqlite3.Connection, sid: str, district: str | None,
) -> None:
    """Set or clear a subscriber's primary district. Used by the
    backfill script and any admin-initiated overrides."""
    db.execute("UPDATE subscribers SET district=? WHERE id=?",
               (district, sid))
    db.commit()


def set_extra_districts(
    db: sqlite3.Connection, sid: str, extras: list[str] | None,
) -> None:
    """Set the comma-separated extra-districts list. Pass ``None`` or
    an empty list to clear."""
    val = ",".join(d.strip().upper() for d in extras if d.strip()) \
        if extras else None
    db.execute("UPDATE subscribers SET extra_districts=? WHERE id=?",
               (val, sid))
    db.commit()


def tracked_districts(sub: dict) -> list[str]:
    """Return the full list of districts a subscriber depends on
    (primary + extras), uppercased and deduplicated, preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in [sub.get("district")] + (
        (sub.get("extra_districts") or "").split(",")
    ):
        if not raw:
            continue
        d = str(raw).strip().upper()
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


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


def set_radius(db: sqlite3.Connection, sid: str, radius_m: int) -> None:
    """Update a subscriber's notification radius (in metres). Used by the
    admin "Edit radius" inline form. Caller is expected to validate the
    value range — this just writes."""
    db.execute("UPDATE subscribers SET radius_m=? WHERE id=?", (radius_m, sid))
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
