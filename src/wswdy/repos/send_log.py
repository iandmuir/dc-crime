"""Send log — one row per (subscriber, send_date, channel)."""
import sqlite3


def record(db: sqlite3.Connection, subscriber_id: str, send_date: str,
           channel: str, status: str, error: str | None = None) -> None:
    db.execute(
        """INSERT OR IGNORE INTO send_log
           (subscriber_id, send_date, channel, status, error)
           VALUES (?, ?, ?, ?, ?)""",
        (subscriber_id, send_date, channel, status, error),
    )
    db.commit()


def exists_for_today(db: sqlite3.Connection, subscriber_id: str,
                     send_date: str, channel: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM send_log WHERE subscriber_id=? AND send_date=? AND channel=?",
        (subscriber_id, send_date, channel),
    ).fetchone()
    return row is not None


def any_sent_today(db: sqlite3.Connection, send_date: str) -> bool:
    """True if at least one row exists for the given send_date (any subscriber/channel/status).

    Used by the adaptive send job to make sure we only attempt one digest run
    per calendar day even if the hourly trigger fires multiple times.
    """
    row = db.execute(
        "SELECT 1 FROM send_log WHERE send_date=? LIMIT 1", (send_date,),
    ).fetchone()
    return row is not None


def recent_failures(db: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = db.execute(
        "SELECT * FROM send_log WHERE status='failed' ORDER BY sent_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def send_volume_last_n_days(db: sqlite3.Connection, *, n: int, today: str) -> list[dict]:
    """Returns one row per send_date with sent/failed counts. Empty days omitted."""
    rows = db.execute(
        """SELECT send_date,
                  SUM(CASE WHEN status='sent'   THEN 1 ELSE 0 END) AS sent,
                  SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                  SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped
             FROM send_log
            WHERE send_date >= date(?, ?)
         GROUP BY send_date
         ORDER BY send_date""",
        (today, f"-{n} days"),
    ).fetchall()
    return [dict(r) for r in rows]
