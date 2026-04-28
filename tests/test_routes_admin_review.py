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
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    return create_app()


def _seed(app):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="Jane",
        email="jane@x", phone=None, preferred_channel="email",
        address_text="1 St", lat=38.9, lon=-77.0, radius_m=1000,
    )
    return sign("secret", purpose="approve", subscriber_id="abc", ttl_seconds=86400)


def test_get_review_renders_subscriber_summary(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/a/{token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"approve" in r.content.lower()


def test_post_approve_changes_status_and_sends_welcome(app):
    from wswdy.notifiers.fake import FakeNotifier
    fake = FakeNotifier()
    app.state.email_notifier = fake

    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/a/{token}/approve", follow_redirects=False)
    assert r.status_code in (200, 303)
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "APPROVED"
    # Welcome message went out
    assert any("welcome" in e["subject"].lower() or "confirmed" in e["text"].lower()
               for e in fake.sent)


def test_post_reject_changes_status(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/a/{token}/reject")
    assert r.status_code in (200, 303)
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "REJECTED"


def test_invalid_token_rejected(app):
    client = TestClient(app)
    r = client.get("/a/not.a.real.token")
    assert r.status_code == 400


def test_expired_token_rejected(app):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="J", email="j@x", phone=None,
        preferred_channel="email", address_text="x", lat=38.9, lon=-77.0, radius_m=1000,
    )
    token = sign("secret", purpose="approve", subscriber_id="abc", ttl_seconds=-1)
    client = TestClient(app)
    r = client.get(f"/a/{token}")
    assert r.status_code == 400
