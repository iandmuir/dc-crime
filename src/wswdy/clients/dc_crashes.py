"""DC Crashes client — fetches the public crashes feed.

Endpoint: ArcGIS REST FeatureServer at maps2.dcgis.dc.gov, layer 24 of the
Public_Safety_WebMercator MapServer. Returns GeoJSON when ?f=geojson is
appended.

Data freshness: the feed is updated daily, but FROMDATE (the crash
timestamp) typically lags 3-5 days behind real time — DC's reporting
pipeline takes that long to publish. So digests show a rolling 7-day
window; "yesterday" claims would be unreliable.

Schema: each feature has properties matching DC's crash fields. We map
to the wswdy crashes table:

  CRIMEID                    -> id
  CCN                        -> ccn
  FROMDATE (epoch ms)        -> report_dt (ISO UTC)
  LASTUPDATEDATE (epoch ms)  -> last_updated (ISO UTC)
  ADDRESS                    -> address
  geometry.coordinates       -> lon, lat
  FATAL_*                    -> summed into `fatal`, plus role splits
  MAJORINJURIES_*            -> summed into `major_injury`, plus role splits
  MINORINJURIES_*            -> summed into `minor_injury`
  PEDESTRIANSIMPAIRED        -> impaired (any-flag OR'd)
  BICYCLISTSIMPAIRED         -> impaired
  DRIVERSIMPAIRED            -> impaired
  SPEEDING_INVOLVED          -> speeding
  TOTAL_VEHICLES             -> total_vehicles
  TOTAL_PEDESTRIANS          -> total_pedestrians
  TOTAL_BICYCLES             -> total_bicycles
  WARD                       -> ward
"""
import json
from datetime import UTC, datetime, timedelta

import httpx

DEFAULT_URL = (
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Public_Safety_WebMercator/MapServer/24/query"
)
DEFAULT_LOOKBACK_DAYS = 30  # we only ingest the last N days; older crashes are pruned


def _ms_to_iso(ms: int | None) -> str | None:
    """Convert epoch milliseconds (DC's date format) to an ISO UTC string."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="seconds")


def _coalesce_int(v) -> int:
    """Return 0 for None / missing values so we can sum without TypeError."""
    return int(v) if v is not None else 0


def _feature_to_record(f: dict) -> dict | None:
    """Map a GeoJSON feature into a crashes-table row dict. Returns None if the
    feature is missing required fields (id, geometry, fromdate)."""
    props = f.get("properties") or {}
    geom = f.get("geometry") or {}

    crash_id = str(props.get("CRIMEID") or props.get("OBJECTID") or "").strip()
    coords = geom.get("coordinates") or []
    fromdate = props.get("FROMDATE")
    if not crash_id or len(coords) != 2 or fromdate is None:
        return None

    lon, lat = coords

    ped_fatal = _coalesce_int(props.get("FATAL_PEDESTRIAN"))
    ped_major = _coalesce_int(props.get("MAJORINJURIES_PEDESTRIAN"))
    bike_fatal = _coalesce_int(props.get("FATAL_BICYCLIST"))
    bike_major = _coalesce_int(props.get("MAJORINJURIES_BICYCLIST"))

    fatal = (
        ped_fatal + bike_fatal
        + _coalesce_int(props.get("FATAL_DRIVER"))
        + _coalesce_int(props.get("FATALPASSENGER"))
        + _coalesce_int(props.get("FATALOTHER"))
    )
    major = (
        ped_major + bike_major
        + _coalesce_int(props.get("MAJORINJURIES_DRIVER"))
        + _coalesce_int(props.get("MAJORINJURIESPASSENGER"))
        + _coalesce_int(props.get("MAJORINJURIESOTHER"))
    )
    minor = (
        _coalesce_int(props.get("MINORINJURIES_PEDESTRIAN"))
        + _coalesce_int(props.get("MINORINJURIES_BICYCLIST"))
        + _coalesce_int(props.get("MINORINJURIES_DRIVER"))
        + _coalesce_int(props.get("MINORINJURIESPASSENGER"))
        + _coalesce_int(props.get("MINORINJURIESOTHER"))
    )

    impaired = (
        _coalesce_int(props.get("PEDESTRIANSIMPAIRED"))
        | _coalesce_int(props.get("BICYCLISTSIMPAIRED"))
        | _coalesce_int(props.get("DRIVERSIMPAIRED"))
    )

    return {
        "id": crash_id,
        "ccn": props.get("CCN"),
        "report_dt": _ms_to_iso(fromdate),
        "last_updated": _ms_to_iso(props.get("LASTUPDATEDATE")),
        "address": props.get("ADDRESS"),
        "lat": float(lat),
        "lon": float(lon),
        "fatal": fatal,
        "major_injury": major,
        "minor_injury": minor,
        "ped_fatal": ped_fatal,
        "ped_major": ped_major,
        "bike_fatal": bike_fatal,
        "bike_major": bike_major,
        "total_vehicles": _coalesce_int(props.get("TOTAL_VEHICLES")),
        "total_pedestrians": _coalesce_int(props.get("TOTAL_PEDESTRIANS")),
        "total_bicycles": _coalesce_int(props.get("TOTAL_BICYCLES")),
        "speeding": _coalesce_int(props.get("SPEEDING_INVOLVED")),
        "impaired": 1 if impaired else 0,
        "ward": props.get("WARD"),
        "raw_json": json.dumps(props, default=str),
    }


async def fetch_recent_crashes(
    *,
    feed_url: str = DEFAULT_URL,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    timeout_s: float = 30.0,
) -> list[dict]:
    """Fetch crashes with FROMDATE in the last `lookback_days` and return them as
    crashes-table rows. Order is feed order (typically newest first)."""
    since_dt = datetime.now(UTC) - timedelta(days=lookback_days)
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    where = f"FROMDATE >= timestamp '{since_str}'"
    params = {
        "where": where,
        "outFields": "*",
        "f": "geojson",
        # DC's server caps responses at ~2000 features; lookback windows beyond
        # ~60 days may need pagination. 30 days is well under the cap (~1300
        # crashes/month observed empirically).
        "resultRecordCount": 2000,
    }
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(feed_url, params=params)
        r.raise_for_status()
        data = r.json()

    out: list[dict] = []
    for feature in data.get("features") or []:
        rec = _feature_to_record(feature)
        if rec is not None:
            out.append(rec)
    return out
