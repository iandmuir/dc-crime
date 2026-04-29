"""Crimes table — upsert + radius-filtered queries.

Distance filter uses an equirectangular approximation pre-filter (cheap, in SQL),
followed by an exact haversine refinement in Python on the small candidate set.
At <100k crimes this is well under a millisecond.
"""
import math
import sqlite3

from wswdy.geo import haversine_m

# 1 degree latitude  ≈ 111_320 m
# 1 degree longitude ≈ 111_320 * cos(lat) m  (varies with latitude — DC ≈ 86_700 m)
_M_PER_DEG_LAT = 111_320.0


def _bbox(lat: float, lon: float, radius_m: float) -> tuple[float, float, float, float]:
    dlat = radius_m / _M_PER_DEG_LAT
    dlon = radius_m / (_M_PER_DEG_LAT * math.cos(math.radians(lat)))
    return lat - dlat, lat + dlat, lon - dlon, lon + dlon


def upsert_many(db: sqlite3.Connection, crimes: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated)."""
    added = updated = 0
    for c in crimes:
        cur = db.execute("SELECT 1 FROM crimes WHERE ccn=?", (c["ccn"],)).fetchone()
        if cur:
            db.execute(
                """UPDATE crimes SET
                   offense=?, method=?, shift=?, block_address=?, lat=?, lon=?,
                   report_dt=?, start_dt=?, end_dt=?, ward=?, district=?, raw_json=?
                   WHERE ccn=?""",
                (c["offense"], c["method"], c["shift"], c["block_address"], c["lat"], c["lon"],
                 c["report_dt"], c["start_dt"], c["end_dt"], c["ward"], c["district"],
                 c["raw_json"], c["ccn"]),
            )
            updated += 1
        else:
            db.execute(
                """INSERT INTO crimes
                   (ccn, offense, method, shift, block_address, lat, lon,
                    report_dt, start_dt, end_dt, ward, district, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (c["ccn"], c["offense"], c["method"], c["shift"], c["block_address"],
                 c["lat"], c["lon"], c["report_dt"], c["start_dt"], c["end_dt"],
                 c["ward"], c["district"], c["raw_json"]),
            )
            added += 1
    db.commit()
    return added, updated


def _candidates(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                extra_where: str = "", params: tuple = ()) -> list[dict]:
    s_lat, n_lat, w_lon, e_lon = _bbox(lat, lon, radius_m)
    sql = ("SELECT * FROM crimes WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
           + (" AND " + extra_where if extra_where else ""))
    rows = db.execute(sql, (s_lat, n_lat, w_lon, e_lon, *params)).fetchall()
    return [dict(r) for r in rows
            if haversine_m(lat, lon, r["lat"], r["lon"]) <= radius_m]


def count_in_radius(db: sqlite3.Connection, lat: float, lon: float, radius_m: float) -> int:
    return len(_candidates(db, lat, lon, radius_m))


def list_in_radius(db: sqlite3.Connection, lat: float, lon: float, radius_m: float) -> list[dict]:
    return _candidates(db, lat, lon, radius_m)


def list_in_radius_window(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                          *, start: str, end: str) -> list[dict]:
    return _candidates(
        db, lat, lon, radius_m,
        extra_where="report_dt >= ? AND report_dt < ?",
        params=(start, end),
    )


def prune_older_than(db: sqlite3.Connection, cutoff_iso: str) -> int:
    cur = db.execute("DELETE FROM crimes WHERE report_dt < ?", (cutoff_iso,))
    db.commit()
    return cur.rowcount
