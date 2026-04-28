from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import upsert_many
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app):
    subs_repo.insert_pending(app.state.db, sid="abc", display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9097, lon=-77.0319, radius_m=1000)
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    now = datetime.now(UTC)
    upsert_many(app.state.db, [
        {"ccn": "near-recent", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
         "block_address": "1400 P", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(hours=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "near-old", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=5)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "far", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9500, "lon": -77.0500,
         "report_dt": (now - timedelta(hours=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
    ])
    return sign("secret", purpose="map", subscriber_id="abc")


def test_api_crimes_24h_returns_recent_only(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=24h")
    assert r.status_code == 200
    fc = r.json()
    assert fc["type"] == "FeatureCollection"
    ccns = {f["properties"]["ccn"] for f in fc["features"]}
    assert ccns == {"near-recent"}


def test_api_crimes_7d_includes_older(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=7d")
    fc = r.json()
    ccns = {f["properties"]["ccn"] for f in fc["features"]}
    assert ccns == {"near-recent", "near-old"}


def test_api_crimes_features_include_tier(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=24h")
    f = r.json()["features"][0]
    assert "tier" in f["properties"]
    assert f["properties"]["tier"] == 1  # ROBBERY + GUN


def test_api_crimes_invalid_token_401(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/api/crimes?subscriber=abc&token=bad&window=24h")
    assert r.status_code == 401


def test_api_crimes_invalid_window_400(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=year")
    assert r.status_code == 400
