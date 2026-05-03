"""MPD police-district lookup from a (lat, lon) coordinate.

Loads a bundled GeoJSON FeatureCollection of DC's seven police districts
(``1D``..``7D``) and exposes a single ``district_for(lat, lon)`` helper
that returns the district code, or ``None`` if the point is outside every
district polygon.

Used to tag each subscriber with the district whose LISTSERV reports
their digest depends on. Subscribers can also have additional
"extra districts" (for cases where their search radius spills across a
boundary), but the auto-tag at signup uses just the home address.

Implementation notes
--------------------
- Pure-Python ray casting against polygon coordinates — no shapely or
  GIS libraries required, no native deps.
- Districts can be Polygon or MultiPolygon; both are supported.
- We assume coordinates are in WGS84 ``[lon, lat]`` order, which is the
  GeoJSON spec default.
- The boundary file is loaded once and cached for the process lifetime.
- If the file is missing or fails to parse, lookups return ``None``
  (rather than raising) so the rest of the app stays functional —
  subscribers without a district fall back to the legacy global
  readiness check.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path

log = logging.getLogger(__name__)


# Bundled boundary file. Expected to be a GeoJSON FeatureCollection
# where each feature has a property identifying the district (e.g.
# "DISTRICT": "1D"). The fetch script writes it here.
DEFAULT_BOUNDARIES_PATH = (
    Path(__file__).resolve().parent / "static" / "dc_police_districts.geojson"
)

# Likely candidate property names that hold the district label, in
# priority order. DC's GIS exports vary slightly between datasets.
_DISTRICT_PROP_NAMES = ("DISTRICT", "NAME", "DIST", "Districts", "District")


def _normalize_district(raw: str) -> str | None:
    """Coerce a raw label like ``'First District'``, ``'1'``, or ``'1D'``
    into the canonical ``'1D'`` format used everywhere else in the app."""
    if not raw:
        return None
    s = str(raw).strip().upper()
    # "1D" / "2D" / ... — already canonical
    if len(s) == 2 and s[0].isdigit() and s[1] == "D":
        return s
    # "1" / "2" / ...
    if s.isdigit() and len(s) == 1 and s in "1234567":
        return f"{s}D"
    # "FIRST DISTRICT" / "SECOND DISTRICT" / ...
    word_to_digit = {
        "FIRST": "1", "SECOND": "2", "THIRD": "3", "FOURTH": "4",
        "FIFTH": "5", "SIXTH": "6", "SEVENTH": "7",
    }
    for word, digit in word_to_digit.items():
        if s.startswith(word):
            return f"{digit}D"
    return None


def _extract_label(properties: dict) -> str | None:
    """Pull the district label out of a feature's properties dict."""
    for key in _DISTRICT_PROP_NAMES:
        if key in properties and properties[key]:
            label = _normalize_district(properties[key])
            if label:
                return label
    # Last-ditch: any property whose name CONTAINS "district" and value
    # parses as a district code. Helps with quirky exports.
    for key, val in properties.items():
        if "district" in key.lower():
            label = _normalize_district(val)
            if label:
                return label
    return None


def _point_in_ring(lon: float, lat: float, ring: list[list[float]]) -> bool:
    """Ray-cast point-in-ring test. ``ring`` is a list of ``[lon, lat]``
    pairs forming a closed polygon ring."""
    inside = False
    n = len(ring)
    if n < 3:
        return False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        # Edge straddles the horizontal line at y=lat?
        if (yi > lat) != (yj > lat):
            x_cross = (xj - xi) * (lat - yi) / (yj - yi + 1e-30) + xi
            if lon < x_cross:
                inside = not inside
        j = i
    return inside


def _point_in_polygon(lon: float, lat: float, polygon: list[list[list[float]]]) -> bool:
    """A GeoJSON Polygon is ``[outer_ring, hole_1, hole_2, ...]``.
    A point is "in" iff it's in the outer ring AND not in any hole."""
    if not polygon:
        return False
    if not _point_in_ring(lon, lat, polygon[0]):
        return False
    for hole in polygon[1:]:
        if _point_in_ring(lon, lat, hole):
            return False
    return True


@lru_cache(maxsize=1)
def _load_districts(path_str: str | None = None) -> list[tuple[str, list]]:
    """Load and cache the boundary file. Returns ``[(district, polygons)]``
    where ``polygons`` is a list of Polygon coordinate-arrays (one per
    feature for Polygon, several per feature for MultiPolygon)."""
    path = Path(path_str) if path_str else DEFAULT_BOUNDARIES_PATH
    if not path.exists():
        log.warning(
            "district boundaries file missing at %s — district lookups "
            "will return None until you run scripts/fetch_district_boundaries.py",
            path,
        )
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        log.warning("failed to load district boundaries: %s", e)
        return []

    out: list[tuple[str, list]] = []
    for feature in data.get("features") or []:
        label = _extract_label(feature.get("properties") or {})
        if not label:
            continue
        geom = feature.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates") or []
        polys: list[list[list[list[float]]]] = []
        if gtype == "Polygon":
            polys.append(coords)
        elif gtype == "MultiPolygon":
            polys.extend(coords)
        else:
            continue
        out.append((label, polys))
    log.info("loaded %d district polygon(s) from %s",
             sum(len(p) for _, p in out), path)
    return out


def district_for(
    lat: float, lon: float, *, boundaries_path: str | None = None,
) -> str | None:
    """Return the MPD district label (``'1D'``..``'7D'``) containing the
    point, or ``None`` if outside every district (including non-DC
    coordinates).
    """
    for label, polys in _load_districts(boundaries_path):
        for poly in polys:
            if _point_in_polygon(lon, lat, poly):
                return label
    return None


def reset_cache() -> None:
    """Drop the cached boundaries — call this in tests after writing a
    new boundaries file. (lru_cache otherwise keys on the path string
    only, so two tests using the default path collide.)"""
    _load_districts.cache_clear()
