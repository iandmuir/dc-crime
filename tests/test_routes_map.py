import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
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
                              address_text="1500 14th St NW", lat=38.9097, lon=-77.0319,
                              radius_m=1000)
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    return sign("secret", purpose="map", subscriber_id="abc")


def test_map_renders_with_valid_token(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/map/abc?token={token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"leaflet" in r.content.lower()
    assert b"MAPTILER_API_KEY" not in r.content  # injected as JS, not as literal name
    assert b"abc" in r.content


def test_map_invalid_token_400(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/map/abc?token=bad.token")
    assert r.status_code == 400
