"""Geoapify Static Maps API client.

We use Geoapify (not MapTiler) for static maps because the MapTiler free tier
doesn't include the static-maps product — it returns a 403 "invalid key" PNG.
Geoapify's free tier gives 3,000 static-map renders per day, which is plenty
for this app at any plausible scale.

Docs: https://apidocs.geoapify.com/docs/maps/map-tiles/static-maps/

Note on URL encoding: Geoapify's marker syntax uses `:`, `;`, `,`, and `#` as
structural separators inside parameter VALUES (e.g. `lonlat:-77.0,38.9` and
`color:#0A0A0A;size:medium`). httpx's default param encoding percent-encodes
those reserved characters, which Geoapify rejects with 400 Bad Request. We
build the query string with urlencode(safe=":;,#") so they pass through
literally, the way the API expects.
"""
from pathlib import Path
from urllib.parse import urlencode

import httpx

STATIC_URL = "https://maps.geoapify.com/v1/staticmap"

# Tier hex codes — kept in sync with the CSS variables in static/shared.css
# (--t1 violent, --t2 serious property, --t3 vehicle, --t4 petty).
_TIER_HEX = {1: "DC2626", 2: "EA580C", 3: "D97706", 4: "65A30D"}


def _zoom_for_radius_m(radius_m: int) -> int:
    """Heuristic zoom level for a ~600x400 canvas at the given radius."""
    if radius_m <= 300:
        return 16
    if radius_m <= 700:
        return 15
    if radius_m <= 1300:
        return 14
    if radius_m <= 2200:
        return 13
    return 12


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
    """Render a static PNG map with a home pin + tier-coloured crime markers.

    `markers` is a list of (lat, lon, tier).
    """
    zoom = _zoom_for_radius_m(radius_m)
    params: list[tuple[str, str]] = [
        ("style", "osm-bright"),
        ("width", str(width)),
        ("height", str(height)),
        ("center", f"lonlat:{center_lon},{center_lat}"),
        ("zoom", str(zoom)),
        ("apiKey", api_key),
    ]
    # Home pin first (near-black, slightly larger so it stands out).
    params.append((
        "marker",
        f"lonlat:{center_lon},{center_lat};color:#0A0A0A;size:medium",
    ))
    # Crime markers — cap to 50 so the URL stays under any sane length limit.
    for lat, lon, tier in markers[:50]:
        color = _TIER_HEX.get(tier, "888888")
        params.append((
            "marker",
            f"lonlat:{lon},{lat};color:#{color};size:small",
        ))

    # Build the query string ourselves so `:`, `;`, `,`, `#` survive un-encoded
    # in marker values — Geoapify parses them as structural separators and
    # returns 400 Bad Request when they're percent-encoded.
    url = f"{STATIC_URL}?{urlencode(params, safe=':;,#')}"

    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(url)
        r.raise_for_status()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(r.content)
    return out_path
