"""Crime extras table — per-CCN extras parsed from MPD's daily LISTSERV crime PDFs.

The canonical crime row still comes from the public ArcGIS feed (repos/crimes.py).
This table only holds the new fields that the API doesn't expose — most importantly
the `location` enum (Restaurant, Residence/Home, Church Synagogue Temple Mosque,
Highway/Road/Alley/Street/Sidewalk, ...). Joined to crimes by ccn at read time.
"""
import sqlite3

_COLUMNS = ("ccn", "district", "psa", "location", "source")


def upsert_many(db: sqlite3.Connection, extras: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated). Match key is `ccn`."""
    added = updated = 0
    for e in extras:
        cur = db.execute("SELECT 1 FROM crime_extras WHERE ccn=?", (e["ccn"],)).fetchone()
        if cur:
            db.execute(
                """UPDATE crime_extras
                   SET district=?, psa=?, location=?, source=?,
                       ingested_at=CURRENT_TIMESTAMP
                   WHERE ccn=?""",
                (e.get("district"), e.get("psa"), e.get("location"),
                 e.get("source", "pdf_listserv"), e["ccn"]),
            )
            updated += 1
        else:
            db.execute(
                """INSERT INTO crime_extras (ccn, district, psa, location, source)
                   VALUES (?,?,?,?,?)""",
                (e["ccn"], e.get("district"), e.get("psa"), e.get("location"),
                 e.get("source", "pdf_listserv")),
            )
            added += 1
    db.commit()
    return added, updated


def get_by_ccn(db: sqlite3.Connection, ccn: str) -> dict | None:
    row = db.execute("SELECT * FROM crime_extras WHERE ccn=?", (ccn,)).fetchone()
    return dict(row) if row else None


def get_many_by_ccn(db: sqlite3.Connection, ccns: list[str]) -> dict[str, dict]:
    """Bulk lookup for joining onto a list of crimes. Returns {ccn: extras_row}.

    Uses a single IN-list query so the API endpoint can decorate a page of
    crimes without N+1 reads.
    """
    if not ccns:
        return {}
    placeholders = ",".join("?" * len(ccns))
    rows = db.execute(
        f"SELECT * FROM crime_extras WHERE ccn IN ({placeholders})", tuple(ccns)
    ).fetchall()
    return {r["ccn"]: dict(r) for r in rows}
