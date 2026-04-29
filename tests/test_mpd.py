import json
from pathlib import Path

import httpx
import pytest
import respx

from wswdy.clients.mpd import fetch_recent_geojson, parse_features

FIXTURE = Path(__file__).parent / "fixtures" / "mpd_sample.geojson"


@respx.mock
async def test_fetch_recent_geojson_returns_dict():
    respx.get("https://example.test/feed").mock(
        return_value=httpx.Response(200, content=FIXTURE.read_bytes(),
                                    headers={"content-type": "application/json"})
    )
    out = await fetch_recent_geojson("https://example.test/feed")
    assert out["type"] == "FeatureCollection"
    assert "features" in out


@respx.mock
async def test_fetch_recent_geojson_raises_on_500():
    respx.get("https://example.test/feed").mock(return_value=httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_recent_geojson("https://example.test/feed")


def test_parse_features_extracts_required_fields():
    data = json.loads(FIXTURE.read_text())
    crimes = parse_features(data)
    assert len(crimes) > 0
    c = crimes[0]
    # required keys for upsert_many
    for k in ("ccn", "offense", "method", "shift", "block_address",
              "lat", "lon", "report_dt", "start_dt", "end_dt",
              "ward", "district", "raw_json"):
        assert k in c, f"missing key: {k}"
    assert isinstance(c["lat"], float)
    assert isinstance(c["lon"], float)
    assert c["raw_json"].startswith("{")


def test_parse_features_skips_features_with_no_geometry():
    crimes = parse_features({
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": None, "properties": {"CCN": "X"}},
            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-77.0, 38.9]},
             "properties": {"CCN": "Y", "OFFENSE": "ROBBERY", "METHOD": "GUN",
                            "SHIFT": "DAY", "BLOCK": "x", "REPORT_DAT": 1714150000000,
                            "START_DATE": 1714150000000, "END_DATE": None,
                            "WARD": "2", "DISTRICT": "3"}},
        ],
    })
    assert [c["ccn"] for c in crimes] == ["Y"]


def test_parse_features_handles_epoch_ms_timestamps():
    crimes = parse_features({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-77.03, 38.91]},
            "properties": {
                "CCN": "T1", "OFFENSE": "BURGLARY", "METHOD": None, "SHIFT": "DAY",
                "BLOCK": "1500 BLOCK", "REPORT_DAT": 1714150000000,
                "START_DATE": 1714150000000, "END_DATE": None,
                "WARD": "2", "DISTRICT": "3",
            },
        }],
    })
    assert crimes[0]["report_dt"].startswith("2024-")  # 1714150000000 = 2024-04-26
