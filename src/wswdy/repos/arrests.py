"""Arrests table — upsert + radius-filtered queries.

Standalone dataset (no join to crimes — DC doesn't publish the link). Sourced
from MPD's daily arrest PDFs. Address is geocoded once at upsert time via
MapTiler; lat/lon may be NULL if geocoding failed and the row should be
omitted from spatial queries when so.

Mirrors the bbox + haversine pattern used in repos/crashes.py and repos/crimes.py
so the map layer / API endpoint can treat it the same way.
"""
import math
import sqlite3

from wswdy.geo import haversine_m

_M_PER_DEG_LAT = 111_320.0


def _bbox(lat: float, lon: float, radius_m: float) -> tuple[float, float, float, float]:
    dlat = radius_m / _M_PER_DEG_LAT
    dlon = radius_m / (_M_PER_DEG_LAT * math.cos(math.radians(lat)))
    return lat - dlat, lat + dlat, lon - dlon, lon + dlon


_COLUMNS = (
    "arrest_number", "district", "psa", "arrest_dt", "arrest_location",
    "lat", "lon",
    "offender_first", "offender_last", "gender", "age",
    "offense", "felony_misd", "officer",
)


def upsert_many(db: sqlite3.Connection, arrests: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated). Match key is `arrest_number`.

    Inputs that already have lat/lon set bypass geocoding; the ingest job is
    expected to do the MapTiler call before handing rows to this function.
    """
    added = updated = 0
    cols = ", ".join(_COLUMNS)
    placeholders = ", ".join(["?"] * len(_COLUMNS))
    update_set = ", ".join(f"{c}=?" for c in _COLUMNS if c != "arrest_number")

    for a in arrests:
        cur = db.execute(
            "SELECT 1 FROM arrests WHERE arrest_number=?", (a["arrest_number"],)
        ).fetchone()
        if cur:
            db.execute(
                f"UPDATE arrests SET {update_set} WHERE arrest_number=?",
                (*[a.get(col) for col in _COLUMNS if col != "arrest_number"],
                 a["arrest_number"]),
            )
            updated += 1
        else:
            db.execute(
                f"INSERT INTO arrests ({cols}) VALUES ({placeholders})",
                tuple(a.get(col) for col in _COLUMNS),
            )
            added += 1
    db.commit()
    return added, updated


def update_geocode(db: sqlite3.Connection, arrest_number: str,
                   lat: float | None, lon: float | None) -> None:
    """Backfill geocode for an existing arrest row."""
    db.execute(
        "UPDATE arrests SET lat=?, lon=? WHERE arrest_number=?",
        (lat, lon, arrest_number),
    )
    db.commit()


def list_missing_geocode(db: sqlite3.Connection, limit: int = 100) -> list[dict]:
    """Arrests that haven't been geocoded yet (lat IS NULL)."""
    rows = db.execute(
        "SELECT * FROM arrests WHERE lat IS NULL ORDER BY arrest_dt DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _candidates(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                extra_where: str = "", params: tuple = ()) -> list[dict]:
    s_lat, n_lat, w_lon, e_lon = _bbox(lat, lon, radius_m)
    sql = ("SELECT * FROM arrests WHERE lat IS NOT NULL "
           "AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
           + (" AND " + extra_where if extra_where else ""))
    rows = db.execute(sql, (s_lat, n_lat, w_lon, e_lon, *params)).fetchall()
    return [dict(r) for r in rows
            if haversine_m(lat, lon, r["lat"], r["lon"]) <= radius_m]


def list_in_radius_window(
    db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
    *, start: str, end: str,
) -> list[dict]:
    return _candidates(
        db, lat, lon, radius_m,
        extra_where="arrest_dt >= ? AND arrest_dt < ?",
        params=(start, end),
    )


def prune_older_than(db: sqlite3.Connection, cutoff_iso: str) -> int:
    cur = db.execute("DELETE FROM arrests WHERE arrest_dt < ?", (cutoff_iso,))
    db.commit()
    return cur.rowcount
