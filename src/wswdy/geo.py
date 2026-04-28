"""Geographic helpers — haversine distance and DC bounding-box check."""
import math
from typing import Final

# (south_lat, west_lon, north_lat, east_lon) — DC's official boundary in WGS84.
DC_BBOX: Final[tuple[float, float, float, float]] = (38.791, -77.120, 38.996, -76.909)

_EARTH_RADIUS_M: Final = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def in_dc_bbox(lat: float, lon: float) -> bool:
    s, w, n, e = DC_BBOX
    return s <= lat <= n and w <= lon <= e
