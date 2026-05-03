#!/usr/bin/env python3
"""Download MPD's police-district boundary GeoJSON from DC's open GIS.

Run once after deploy::

    /root/wswdy/.venv/bin/python scripts/fetch_district_boundaries.py

Writes to ``src/wswdy/static/dc_police_districts.geojson``. The runtime
``wswdy.districts`` module reads this file for point-in-polygon
subscriber-to-district tagging.

DC GIS publishes the boundaries through its public ArcGIS MapServer.
The script tries a small list of well-known endpoints in order — if one
returns a healthy GeoJSON FeatureCollection it's saved; otherwise we
try the next.
"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

# Layer URLs to try, in priority order. The MPD MapServer occasionally
# renumbers layers, so we try several known IDs before giving up.
_CANDIDATES = [
    # Public_Safety_WebMercator MapServer — typical "Police Districts" layer
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Public_Safety_WebMercator/MapServer/16/query"
    "?where=1%3D1&outFields=*&f=geojson&outSR=4326",
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Public_Safety_WebMercator/MapServer/19/query"
    "?where=1%3D1&outFields=*&f=geojson&outSR=4326",
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/"
    "Public_Safety_WebMercator/MapServer/27/query"
    "?where=1%3D1&outFields=*&f=geojson&outSR=4326",
    # opendata.arcgis.com export of the same dataset
    "https://opendata.arcgis.com/datasets/"
    "9e5c2f2b3e3b4b4f9b5c2f2b3e3b4b4f_0.geojson",
]


OUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "src" / "wswdy" / "static" / "dc_police_districts.geojson"
)


def _try_fetch(url: str) -> dict | None:
    print(f"trying {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "wswdy/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except Exception as e:  # noqa: BLE001
        print(f"  ! {e}", file=sys.stderr)
        return None
    if data.get("type") != "FeatureCollection":
        print("  ! not a FeatureCollection", file=sys.stderr)
        return None
    feats = data.get("features") or []
    # Sanity: DC has exactly seven police districts.
    if not (5 <= len(feats) <= 12):
        print(f"  ! suspicious feature count ({len(feats)}), skipping",
              file=sys.stderr)
        return None
    return data


def main() -> int:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    for url in _CANDIDATES:
        data = _try_fetch(url)
        if data is None:
            continue
        OUT_PATH.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
        size_kb = OUT_PATH.stat().st_size / 1024
        print(f"\nwrote {OUT_PATH} ({size_kb:.0f} KB, {len(data['features'])} features)")
        # Show what district labels we picked up — sanity for the user.
        from wswdy.districts import _extract_label, reset_cache  # noqa: E402
        reset_cache()
        labels = []
        for f in data["features"]:
            label = _extract_label(f.get("properties") or {})
            if label:
                labels.append(label)
        print(f"districts found: {sorted(set(labels))}")
        return 0
    print("\nERROR: no candidate URL returned a usable boundary file. "
          "You can manually drop a GeoJSON FeatureCollection at "
          f"{OUT_PATH} as long as each feature has a 'DISTRICT' (or "
          "similar) property like '1D', '2D', etc.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    # Make the wswdy package importable for the labels-check at the end.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    raise SystemExit(main())
