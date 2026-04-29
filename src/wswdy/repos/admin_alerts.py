"""Admin alerts — log of alerts sent + 6h suppression markers."""
import sqlite3
from datetime import UTC, datetime


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def record(db: sqlite3.Connection, *, alert_type: str, message: str) -> int:
    cur = db.execute(
        "INSERT INTO admin_alerts (alert_type, message) VALUES (?, ?)",
        (alert_type, message),
    )
    db.commit()
    return cur.lastrowid


def set_suppressed_until(db: sqlite3.Connection, alert_type: str, until_iso: str) -> None:
    """Marks all subsequent alerts of `alert_type` as suppressed until `until_iso`."""
    db.execute(
        "INSERT INTO admin_alerts (alert_type, message, suppressed_until) VALUES (?, ?, ?)",
        (alert_type, "(suppression marker)", until_iso),
    )
    db.commit()


def is_suppressed(db: sqlite3.Connection, alert_type: str) -> bool:
    row = db.execute(
        """SELECT suppressed_until FROM admin_alerts
            WHERE alert_type=? AND suppressed_until IS NOT NULL
         ORDER BY created_at DESC, id DESC LIMIT 1""",
        (alert_type,),
    ).fetchone()
    if not row or not row["suppressed_until"]:
        return False
    return row["suppressed_until"] > _utcnow_iso()


def list_recent(db: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = db.execute(
        """SELECT * FROM admin_alerts
            WHERE suppressed_until IS NULL
         ORDER BY created_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
