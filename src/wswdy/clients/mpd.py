"""MPD GeoJSON fetcher.

The MPD feed publishes one Feature per reported incident. Coordinates are
WGS84 (`Point`, [lon, lat]). Timestamp fields are Unix epoch *milliseconds*
from the ArcGIS server.

The ArcGIS server caps single responses at ~1000 features. With a rolling
30-day window of ~1500 incidents, an unpaginated fetch loses the most
recent 500 records to that cap (since the default OBJECTID ASC ordering
serves the oldest first). We sort REPORT_DAT DESC so cap-truncation drops
the oldest (acceptable — they're aging out anyway) and paginate via
resultOffset until the server stops setting exceededTransferLimit.
"""
import json
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx


async def fetch_recent_geojson(feed_url: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
    # Strip any existing query string so we control all params explicitly.
    split = urlsplit(feed_url)
    base = urlunsplit((split.scheme, split.netloc, split.path, "", ""))

    base_params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "orderByFields": "REPORT_DAT DESC",
        "resultRecordCount": 2000,
    }
    all_features: list[dict[str, Any]] = []
    seen_ccns: set[str] = set()
    offset = 0
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        while True:
            params = {**base_params, "resultOffset": offset}
            r = await client.get(base, params=params)
            r.raise_for_status()
            data = r.json()
            features = data.get("features") or []
            if not features:
                break
            for f in features:
                ccn = (f.get("properties") or {}).get("CCN")
                if ccn:
                    if ccn in seen_ccns:
                        continue
                    seen_ccns.add(ccn)
                all_features.append(f)
            # The GeoJSON envelope also exposes exceededTransferLimit when
            # there are more pages. If absent or False, we're done.
            if not data.get("exceededTransferLimit"):
                break
            offset += len(features)
            # Defensive cap so a misbehaving server can't loop forever.
            if offset > 50_000:
                break
    return {"type": "FeatureCollection", "features": all_features}


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
