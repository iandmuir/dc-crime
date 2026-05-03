"""Tests for the districts module — point-in-polygon lookup."""
import json

from wswdy import districts


def _square_feature(label: str, x0: float, y0: float, x1: float, y1: float) -> dict:
    """Helper: build a GeoJSON feature with a square polygon."""
    return {
        "type": "Feature",
        "properties": {"DISTRICT": label},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0],
            ]],
        },
    }


def _write_geojson(path, features):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "type": "FeatureCollection",
        "features": features,
    }), encoding="utf-8")
    districts.reset_cache()


def test_district_for_point_inside_polygon(tmp_path):
    f = tmp_path / "p.geojson"
    _write_geojson(f, [_square_feature("2D", 0, 0, 10, 10)])
    assert districts.district_for(5, 5, boundaries_path=str(f)) == "2D"


def test_district_for_point_outside_returns_none(tmp_path):
    f = tmp_path / "p.geojson"
    _write_geojson(f, [_square_feature("2D", 0, 0, 10, 10)])
    assert districts.district_for(50, 50, boundaries_path=str(f)) is None


def test_district_for_picks_correct_polygon_among_many(tmp_path):
    f = tmp_path / "p.geojson"
    _write_geojson(f, [
        _square_feature("1D", 0, 0, 10, 10),
        _square_feature("2D", 10, 0, 20, 10),
        _square_feature("3D", 0, 10, 10, 20),
    ])
    # Note: GeoJSON is [lon, lat]; district_for takes (lat, lon).
    # Inside the second box (10<=lon<=20, 0<=lat<=10) → "2D".
    assert districts.district_for(5, 15, boundaries_path=str(f)) == "2D"


def test_district_for_handles_multipolygon(tmp_path):
    f = tmp_path / "p.geojson"
    _write_geojson(f, [{
        "type": "Feature",
        "properties": {"DISTRICT": "7D"},
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]],
                [[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]],
            ],
        },
    }])
    assert districts.district_for(2, 2, boundaries_path=str(f)) == "7D"
    assert districts.district_for(12, 12, boundaries_path=str(f)) == "7D"
    assert districts.district_for(7, 7, boundaries_path=str(f)) is None


def test_district_for_normalizes_label_variants(tmp_path):
    """The boundary file might use 'First District' or '1' instead of '1D'."""
    f = tmp_path / "p.geojson"
    _write_geojson(f, [
        {
            "type": "Feature",
            "properties": {"DISTRICT": "First District"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"DISTRICT": "2"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[10, 0], [20, 0], [20, 10], [10, 10], [10, 0]]],
            },
        },
    ])
    assert districts.district_for(5, 5, boundaries_path=str(f)) == "1D"
    assert districts.district_for(5, 15, boundaries_path=str(f)) == "2D"


def test_district_for_returns_none_when_file_missing(tmp_path):
    """Graceful degradation — no file means no error, just None."""
    districts.reset_cache()
    assert districts.district_for(
        38.9097, -77.0319,
        boundaries_path=str(tmp_path / "doesnt_exist.geojson"),
    ) is None
