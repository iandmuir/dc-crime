"""Tests for /api/arrests — token auth, window filtering, GeoJSON shape."""
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.arrests import upsert_many
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _arrest(arrest_number: str, **overrides):
    base = {
        "arrest_number": arrest_number,
        "district": "2D",
        "psa": "202",
        "arrest_dt": datetime.now(UTC).isoformat(timespec="seconds"),
        "arrest_location": "5028 Belt Rd NW Washington DC",
        "lat": 38.9100, "lon": -77.0319,
        "offender_first": "Jane",
        "offender_last": "Doe",
        "gender": "Female",
        "age": 30,
        "offense": "Simple Assault",
        "felony_misd": "MISDEMEANOR",
        "officer": "Smith 12345",
    }
    base.update(overrides)
    return base


def _seed(app):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="Jane",
        email="j@x", phone=None, preferred_channel="email",
        address_text="x", lat=38.9097, lon=-77.0319, radius_m=1000,
    )
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    now = datetime.now(UTC)
    upsert_many(app.state.db, [
        _arrest("near-recent",
                arrest_dt=(now - timedelta(hours=12)).isoformat(timespec="seconds"),
                felony_misd="FELONY"),
        _arrest("near-old",
                arrest_dt=(now - timedelta(days=5)).isoformat(timespec="seconds"),
                felony_misd="MISDEMEANOR"),
        _arrest("far", lat=38.9500, lon=-77.0500,
                arrest_dt=(now - timedelta(hours=2)).isoformat(timespec="seconds")),
        _arrest("ungeocoded", lat=None, lon=None,
                arrest_dt=(now - timedelta(hours=2)).isoformat(timespec="seconds")),
    ])
    return sign("secret", purpose="map", subscriber_id="abc")


def test_api_arrests_24h_returns_recent_geocoded_in_radius(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=24h")
    assert r.status_code == 200
    fc = r.json()
    nums = {f["properties"]["arrest_number"] for f in fc["features"]}
    # ungeocoded excluded by repo's lat-IS-NOT-NULL filter; far excluded by
    # radius; old excluded by 24h window.
    assert nums == {"near-recent"}


def test_api_arrests_7d_includes_older(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=7d")
    nums = {f["properties"]["arrest_number"] for f in r.json()["features"]}
    assert nums == {"near-recent", "near-old"}


def test_api_arrests_tier_classification(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=7d")
    by_num = {f["properties"]["arrest_number"]: f["properties"]
              for f in r.json()["features"]}
    assert by_num["near-recent"]["tier"] == 1   # FELONY -> tier 1
    assert by_num["near-old"]["tier"] == 2      # MISDEMEANOR -> tier 2


def test_api_arrests_unspecified_classification_is_tier_3(app, monkeypatch):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="Jane",
        email="j@x", phone=None, preferred_channel="email",
        address_text="x", lat=38.9097, lon=-77.0319, radius_m=1000,
    )
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    upsert_many(app.state.db, [_arrest("u1", felony_misd=None)])
    token = sign("secret", purpose="map", subscriber_id="abc")
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=24h")
    p = r.json()["features"][0]["properties"]
    assert p["tier"] == 3
    assert p["felony_misd"] is None


def test_api_arrests_combines_first_and_last_name(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=24h")
    p = r.json()["features"][0]["properties"]
    assert p["name"] == "Jane Doe"
    assert p["age"] == 30
    assert p["gender"] == "Female"


def test_api_arrests_invalid_token_401(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/api/arrests?subscriber=abc&token=bad&window=7d")
    assert r.status_code == 401


def test_api_arrests_invalid_window_400(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/arrests?subscriber=abc&token={token}&window=year")
    assert r.status_code == 400
