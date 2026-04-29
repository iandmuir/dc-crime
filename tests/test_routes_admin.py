import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.repos.fetch_log import record_success
from wswdy.repos.send_log import record
from wswdy.repos.subscribers import insert_pending, set_status


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "ADMINTOKEN123")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def test_admin_no_token_rejected(app):
    client = TestClient(app)
    r = client.get("/admin")
    assert r.status_code in (401, 403)


def test_admin_wrong_token_rejected(app):
    client = TestClient(app)
    r = client.get("/admin?token=wrong")
    assert r.status_code in (401, 403)


def test_admin_valid_token_renders(app):
    insert_pending(app.state.db, sid="a", display_name="A", email="a@x", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    set_status(app.state.db, "a", "APPROVED")
    record_success(app.state.db, added=42, updated=3)
    record(app.state.db, "a", "2026-04-28", "email", "sent")

    client = TestClient(app)
    r = client.get("/admin?token=ADMINTOKEN123")
    assert r.status_code == 200
    assert b"42" in r.content
    assert b"approved" in r.content.lower() or b"APPROVED" in r.content
