"""MapTiler API client — Geocoding only.

Static maps used to live here too, but MapTiler's free tier doesn't include
the static-maps product (returns a 403 "invalid key" PNG). Static rendering
moved to wswdy.clients.geoapify. Tile maps for the interactive /map view
still come from MapTiler — raster tiles ARE included in the free tier.
"""
import httpx

from wswdy.geo import in_dc_bbox

GEOCODE_URL = "https://api.maptiler.com/geocoding/{q}.json"


class GeocodeError(Exception):
    """Raised when an address can't be resolved or is outside DC."""


async def geocode_address(query: str, *, api_key: str, timeout_s: float = 10.0) -> dict:
    """Geocode a DC address, returning {lat, lon, display}. Raises GeocodeError if outside DC."""
    params = {"key": api_key, "limit": 1, "country": "us",
              "bbox": "-77.120,38.791,-76.909,38.996"}  # DC bbox prefilter
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(GEOCODE_URL.format(q=query), params=params)
        r.raise_for_status()
        data = r.json()

    features = data.get("features") or []
    if not features:
        raise GeocodeError("no results for that address")
    f = features[0]
    lon, lat = f["center"]
    if not in_dc_bbox(lat, lon):
        raise GeocodeError("address is outside DC")
    display = f.get("place_name", query)
    # MapTiler labels DC addresses as "Columbia <ZIP>" — normalize to "Washington, DC".
    display = display.replace(", Columbia ", ", Washington, DC ")
    return {"lat": float(lat), "lon": float(lon), "display": display}
