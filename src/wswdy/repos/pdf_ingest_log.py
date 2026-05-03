"""PDF ingest log — one row per (district, kind) PDF we successfully ingested.

Drives the admin coverage tracker: most-recent row per (district, kind) tells
us whether and when each district's daily report has landed. Combined with
"now" this gives us a traffic light (green <24h, yellow 24-48h, red older
or never seen) for each cell of the 7-districts × 2-kinds grid.
"""
import sqlite3


def record(db: sqlite3.Connection, *, district: str, kind: str,
           source_file: str | None, records: int) -> None:
    """Append one ingest event. Multiple rows per (district, kind) are fine —
    the coverage view just picks the most recent."""
    db.execute(
        """INSERT INTO pdf_ingest_log (district, kind, source_file, records)
           VALUES (?, ?, ?, ?)""",
        (district, kind, source_file, records),
    )
    db.commit()


def latest_per_district_kind(db: sqlite3.Connection) -> list[dict]:
    """Most-recent ingest row per (district, kind). Used by the admin
    coverage tracker. Returns rows sorted by (district, kind) for stable
    table rendering."""
    rows = db.execute(
        """SELECT l.district, l.kind, l.source_file, l.records, l.ingested_at
           FROM pdf_ingest_log l
           JOIN (
             SELECT district, kind, MAX(ingested_at) AS max_at
             FROM pdf_ingest_log
             GROUP BY district, kind
           ) m
           ON l.district = m.district AND l.kind = m.kind
              AND l.ingested_at = m.max_at
           ORDER BY l.district, l.kind"""
    ).fetchall()
    return [dict(r) for r in rows]


def recent(db: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Most recent N ingest events, newest first. For an audit trail view."""
    rows = db.execute(
        "SELECT * FROM pdf_ingest_log ORDER BY ingested_at DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
