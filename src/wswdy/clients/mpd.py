"""MPD GeoJSON fetcher.

The MPD feed publishes one Feature per reported incident. Coordinates are
WGS84 (`Point`, [lon, lat]). Timestamp fields are Unix epoch *milliseconds*
from the ArcGIS server.
"""
import json
from datetime import UTC, datetime
from typing import Any

import httpx


async def fetch_recent_geojson(feed_url: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(feed_url)
        r.raise_for_status()
        return r.json()


def _epoch_ms_to_iso(v: Any) -> str | None:
    if v is None:
        return None
    try:
        ms = int(v)
        return datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        return None


def parse_features(geojson: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for feat in geojson.get("features") or []:
        geom = feat.get("geometry")
        if not geom or geom.get("type") != "Point":
            continue
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        if lat is None or lon is None:
            continue
        try:
            lat_f, lon_f = float(lat), float(lon)
        except (TypeError, ValueError):
            continue

        p = feat.get("properties") or {}
        ccn = p.get("CCN")
        if not ccn:
            continue

        out.append({
            "ccn": str(ccn),
            "offense": p.get("OFFENSE") or "UNKNOWN",
            "method": p.get("METHOD"),
            "shift": p.get("SHIFT"),
            "block_address": p.get("BLOCK"),
            "lat": lat_f,
            "lon": lon_f,
            "report_dt": _epoch_ms_to_iso(p.get("REPORT_DAT")),
            "start_dt": _epoch_ms_to_iso(p.get("START_DATE")),
            "end_dt": _epoch_ms_to_iso(p.get("END_DATE")),
            "ward": str(p.get("WARD")) if p.get("WARD") is not None else None,
            "district": str(p.get("DISTRICT")) if p.get("DISTRICT") is not None else None,
            "raw_json": json.dumps(p, separators=(",", ":")),
        })
    # Drop any with bad timestamps — MPD occasionally publishes nulls
    return [c for c in out if c["report_dt"]]
