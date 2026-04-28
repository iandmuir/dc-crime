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


def _seed(app, sid="abc"):
    subs_repo.insert_pending(app.state.db, sid=sid, display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9, lon=-77.0, radius_m=1000)
    subs_repo.set_status(app.state.db, sid, "APPROVED")
    return sign("secret", purpose="unsubscribe", subscriber_id=sid)


def test_get_unsubscribe_renders_confirmation(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/u/abc?token={token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"unsubscribe" in r.content.lower()


def test_post_unsubscribe_marks_unsubscribed(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/u/abc?token={token}")
    assert r.status_code == 200
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "UNSUBSCRIBED"
    assert b"out" in r.content.lower() or b"unsubscribed" in r.content.lower()


def test_unsubscribe_token_for_other_subscriber_rejected(app):
    _seed(app, sid="abc")
    _seed(app, sid="other")
    bad_token = sign("secret", purpose="unsubscribe", subscriber_id="other")
    client = TestClient(app)
    r = client.get(f"/u/abc?token={bad_token}")
    assert r.status_code == 400


def test_unsubscribe_no_expiry(app):
    """Unsubscribe links must work indefinitely — never expire."""
    sid = "abc"
    subs_repo.insert_pending(app.state.db, sid=sid, display_name="J",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9, lon=-77.0, radius_m=1000)
    subs_repo.set_status(app.state.db, sid, "APPROVED")
    # Token signed with no TTL
    token = sign("secret", purpose="unsubscribe", subscriber_id=sid)
    client = TestClient(app)
    r = client.get(f"/u/{sid}?token={token}")
    assert r.status_code == 200
