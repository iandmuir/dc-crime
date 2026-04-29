from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos.crimes import upsert_many


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app):
    now = datetime.now(UTC)
    upsert_many(app.state.db, [
        {"ccn": "1", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
        {"ccn": "2", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=4)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
        {"ccn": "3", "offense": "BURGLARY", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9500, "lon": -77.0500,  # far
         "report_dt": (now - timedelta(days=1)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
    ])


def test_preview_returns_aggregate_counts(app):
    _seed(app)
    client = TestClient(app)
    r = client.post("/api/preview",
                    json={"lat": 38.9097, "lon": -77.0319, "radius_m": 500})
    assert r.status_code == 200
    d = r.json()
    assert d["window_days"] == 7
    assert d["total"] == 2
    assert d["by_tier"]["1"] == 1
    assert d["by_tier"]["4"] == 1
    assert d["avg_per_day"] == pytest.approx(2/7, abs=0.01)


def test_preview_validates_radius(app):
    client = TestClient(app)
    r = client.post("/api/preview", json={"lat": 38.9, "lon": -77.0, "radius_m": 50})
    assert r.status_code in (400, 422)


def test_preview_outside_dc_rejected(app):
    client = TestClient(app)
    r = client.post("/api/preview", json={"lat": 39.29, "lon": -76.62, "radius_m": 500})
    assert r.status_code == 400


def test_preview_rate_limited(app, monkeypatch):
    import wswdy.routes.api_preview as _mod
    from wswdy.ratelimit import RateLimiter
    monkeypatch.setattr(_mod, "_rl", RateLimiter(max_requests=30, window_s=60))
    client = TestClient(app)
    body = {"lat": 38.9097, "lon": -77.0319, "radius_m": 500}
    for _ in range(30):
        client.post("/api/preview", json=body)
    r = client.post("/api/preview", json=body)
    assert r.status_code == 429
