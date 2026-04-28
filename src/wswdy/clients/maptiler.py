"""MapTiler API client — Geocoding + Static Maps."""
from pathlib import Path

import httpx

from wswdy.geo import in_dc_bbox

GEOCODE_URL = "https://api.maptiler.com/geocoding/{q}.json"
STATIC_URL = "https://api.maptiler.com/maps/streets-v2/static/{lon},{lat},{zoom}/{w}x{h}.png"


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
    return {"lat": float(lat), "lon": float(lon), "display": f.get("place_name", query)}


def _zoom_for_radius_m(radius_m: int) -> int:
    """Return a heuristic zoom level for the given radius on a ~600x400 canvas."""
    if radius_m <= 300:
        return 16
    if radius_m <= 700:
        return 15
    if radius_m <= 1300:
        return 14
    if radius_m <= 2200:
        return 13
    return 12


_TIER_HEX = {1: "DC2626", 2: "EA580C", 3: "D97706", 4: "65A30D"}


async def render_static_map(
    *,
    api_key: str,
    center_lat: float,
    center_lon: float,
    radius_m: int,
    markers: list[tuple[float, float, int]],
    out_path: Path,
    width: int = 600,
    height: int = 400,
    timeout_s: float = 20.0,
) -> Path:
    """Render a static PNG map with tier-coloured markers and write it to `out_path`.

    `markers` is a list of (lat, lon, tier).
    """
    zoom = _zoom_for_radius_m(radius_m)
    url = STATIC_URL.format(lon=center_lon, lat=center_lat, zoom=zoom, w=width, h=height)
    params: list[tuple[str, str]] = [("key", api_key)]
    # Home pin first (near-black)
    params.append(("marker", f"{center_lon},{center_lat},#0A0A0A"))
    for lat, lon, tier in markers[:60]:  # cap to keep URL length sane
        color = _TIER_HEX.get(tier, "888888")  # grey fallback for unknown tiers
        params.append(("marker", f"{lon},{lat},#{color}"))
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(r.content)
    return out_path
