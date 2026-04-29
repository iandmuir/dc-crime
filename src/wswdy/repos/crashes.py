"""Crashes table — upsert + radius-filtered queries.

Mirrors repos/crimes.py: same equirectangular bbox pre-filter + haversine
refinement pattern. Different schema (per-role injury counts, severity flags)
but the spatial query interface is intentionally identical so the send job
and digest builder can treat both feeds the same way.
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
    "id", "ccn", "report_dt", "last_updated", "address", "lat", "lon",
    "fatal", "major_injury", "minor_injury",
    "ped_fatal", "ped_major", "bike_fatal", "bike_major",
    "total_vehicles", "total_pedestrians", "total_bicycles",
    "speeding", "impaired", "ward", "raw_json",
)


def upsert_many(db: sqlite3.Connection, crashes: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated). Match key is `id` (DC's CRIMEID)."""
    added = updated = 0
    cols = ", ".join(_COLUMNS)
    placeholders = ", ".join(["?"] * len(_COLUMNS))
    update_set = ", ".join(f"{c}=?" for c in _COLUMNS if c != "id")

    for c in crashes:
        cur = db.execute("SELECT 1 FROM crashes WHERE id=?", (c["id"],)).fetchone()
        if cur:
            db.execute(
                f"UPDATE crashes SET {update_set} WHERE id=?",
                (*[c.get(col) for col in _COLUMNS if col != "id"], c["id"]),
            )
            updated += 1
        else:
            db.execute(
                f"INSERT INTO crashes ({cols}) VALUES ({placeholders})",
                tuple(c.get(col) for col in _COLUMNS),
            )
            added += 1
    db.commit()
    return added, updated


def _candidates(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                extra_where: str = "", params: tuple = ()) -> list[dict]:
    s_lat, n_lat, w_lon, e_lon = _bbox(lat, lon, radius_m)
    sql = ("SELECT * FROM crashes WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
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
        extra_where="report_dt >= ? AND report_dt < ?",
        params=(start, end),
    )


def prune_older_than(db: sqlite3.Connection, cutoff_iso: str) -> int:
    cur = db.execute("DELETE FROM crashes WHERE report_dt < ?", (cutoff_iso,))
    db.commit()
    return cur.rowcount
