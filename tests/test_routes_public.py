from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos.subscribers import list_by_status


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@x")
    return create_app()


def test_get_signup_form_renders(app):
    client = TestClient(app)
    r = client.get("/")
    assert r.status_code == 200
    assert b"What" in r.content and b"shit" in r.content
    assert b"display_name" in r.content


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
def test_post_signup_creates_pending_and_emails_admin(mock_geo, app):
    mock_geo.return_value = {"lat": 38.9097, "lon": -77.0319,
                              "display": "1500 14th St NW, Washington, DC"}
    # Replace the email notifier with a FakeNotifier we can inspect
    from wswdy.notifiers.fake import FakeNotifier
    fake = FakeNotifier()
    app.state.email_notifier = fake

    client = TestClient(app)
    r = client.post("/signup", data={
        "display_name": "Jane",
        "address_text": "1500 14th St NW",
        "lat": "38.9097", "lon": "-77.0319",
        "preferred_channel": "email",
        "email": "jane@example.com",
        "radius_m": "1000",
    }, follow_redirects=False)
    assert r.status_code in (303, 302)

    pending = list_by_status(app.state.db, "PENDING")
    assert len(pending) == 1
    assert pending[0]["display_name"] == "Jane"
    assert fake.sent and fake.sent[0]["recipient"] == "admin@x"
    assert "Approve" in fake.sent[0]["text"]


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
def test_post_signup_outside_dc_returns_form_with_error(mock_geo, app):
    from wswdy.clients.maptiler import GeocodeError
    mock_geo.side_effect = GeocodeError("address is outside DC")
    client = TestClient(app)
    r = client.post("/signup", data={
        "display_name": "Bob",
        "address_text": "1 Inner Harbor, Baltimore",
        "preferred_channel": "email",
        "email": "bob@x",
        "radius_m": "1000",
    })
    assert r.status_code == 400
    assert b"outside DC" in r.content


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
def test_post_signup_rate_limited(mock_geo, app, monkeypatch):
    import wswdy.routes.public as _pub
    from wswdy.notifiers.fake import FakeNotifier
    from wswdy.ratelimit import RateLimiter
    monkeypatch.setattr(_pub, "_signup_rl", RateLimiter(max_requests=10, window_s=3600))
    mock_geo.return_value = {"lat": 38.9097, "lon": -77.0319, "display": "test address"}
    app.state.email_notifier = FakeNotifier()
    client = TestClient(app)
    for _ in range(10):
        client.post("/signup", data={"display_name": "x", "address_text": "y",
                                      "preferred_channel": "email", "email": "x@x",
                                      "radius_m": "1000"})
    r = client.post("/signup", data={"display_name": "x", "address_text": "y",
                                      "preferred_channel": "email", "email": "x@x",
                                      "radius_m": "1000"})
    assert r.status_code == 429
