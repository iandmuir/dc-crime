from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crashes import upsert_many
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _crash(id, **overrides):
    base = {
        "id": id, "ccn": "x",
        "report_dt": datetime.now(UTC).isoformat(timespec="seconds"),
        "last_updated": datetime.now(UTC).isoformat(timespec="seconds"),
        "address": "1500 14TH ST NW",
        "lat": 38.9100, "lon": -77.0319,
        "fatal": 0, "major_injury": 0, "minor_injury": 0,
        "ped_fatal": 0, "ped_major": 0, "bike_fatal": 0, "bike_major": 0,
        "total_vehicles": 1, "total_pedestrians": 0, "total_bicycles": 0,
        "speeding": 0, "impaired": 0, "ward": "Ward 1", "raw_json": "{}",
    }
    base.update(overrides)
    return base


def _seed(app):
    subs_repo.insert_pending(app.state.db, sid="abc", display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9097, lon=-77.0319, radius_m=1000)
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    now = datetime.now(UTC)
    upsert_many(app.state.db, [
        _crash("near-recent", report_dt=(now - timedelta(hours=12)).isoformat(timespec="seconds"),
               fatal=1),
        _crash("near-old", report_dt=(now - timedelta(days=5)).isoformat(timespec="seconds"),
               major_injury=1, ped_major=1),
        _crash("far", lat=38.9500, lon=-77.0500,
               report_dt=(now - timedelta(hours=2)).isoformat(timespec="seconds")),
    ])
    return sign("secret", purpose="map", subscriber_id="abc")


def test_api_crashes_24h_returns_recent_only(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crashes?subscriber=abc&token={token}&window=24h")
    assert r.status_code == 200
    fc = r.json()
    ids = {f["properties"]["id"] for f in fc["features"]}
    assert ids == {"near-recent"}


def test_api_crashes_7d_includes_older(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crashes?subscriber=abc&token={token}&window=7d")
    ids = {f["properties"]["id"] for f in r.json()["features"]}
    assert ids == {"near-recent", "near-old"}


def test_api_crashes_features_include_tier_and_flags(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crashes?subscriber=abc&token={token}&window=7d")
    by_id = {f["properties"]["id"]: f["properties"] for f in r.json()["features"]}
    assert by_id["near-recent"]["tier"] == 1            # fatal -> tier 1
    assert by_id["near-old"]["tier"] == 2               # major injury -> tier 2
    assert by_id["near-old"]["ped_struck"] is True


def test_api_crashes_invalid_token_401(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/api/crashes?subscriber=abc&token=bad&window=7d")
    assert r.status_code == 401


def test_api_crashes_invalid_window_400(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crashes?subscriber=abc&token={token}&window=year")
    assert r.status_code == 400
