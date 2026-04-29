from fastapi.testclient import TestClient

from wswdy.main import create_app


def test_healthz_ok(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "x")
    monkeypatch.setenv("ADMIN_TOKEN", "y")
    monkeypatch.setenv("MAPTILER_API_KEY", "z")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    app = create_app()
    client = TestClient(app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_static_css_served(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "x")
    monkeypatch.setenv("ADMIN_TOKEN", "y")
    monkeypatch.setenv("MAPTILER_API_KEY", "z")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    app = create_app()
    client = TestClient(app)
    r = client.get("/static/shared.css")
    assert r.status_code == 200
    assert b"--paper" in r.content or b"--bg" in r.content
