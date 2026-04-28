import pytest
from wswdy.geo import haversine_m, in_dc_bbox, DC_BBOX


def test_haversine_zero_distance():
    assert haversine_m(38.9, -77.0, 38.9, -77.0) == pytest.approx(0.0, abs=0.5)


def test_haversine_known_distance():
    # Logan Circle (38.9097,-77.0319) to Lincoln Memorial (38.8893,-77.0502) ~ 2.65 km
    d = haversine_m(38.9097, -77.0319, 38.8893, -77.0502)
    assert 2400 <= d <= 2900


def test_in_dc_bbox_logan_circle():
    assert in_dc_bbox(38.9097, -77.0319) is True


def test_in_dc_bbox_baltimore_no():
    assert in_dc_bbox(39.29, -76.62) is False


def test_in_dc_bbox_alexandria_no():
    # Alexandria is just south of DC (south boundary is 38.791, so use 38.78)
    assert in_dc_bbox(38.78, -77.05) is False


def test_dc_bbox_constant_shape():
    assert DC_BBOX == (38.791, -77.120, 38.996, -76.909)
