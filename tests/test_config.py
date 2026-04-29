from wswdy.config import Settings


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("HMAC_SECRET", "abc")
    monkeypatch.setenv("ADMIN_TOKEN", "xyz")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", "/tmp/x.db")
    s = Settings()
    assert s.hmac_secret == "abc"
    assert s.admin_token == "xyz"
    assert s.maptiler_api_key == "k"
    assert s.db_path == "/tmp/x.db"
    assert s.smtp_port == 587  # default
    assert str(s.mpd_feed_url).startswith("https://")


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("HMAC_SECRET", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MAPTILER_API_KEY", raising=False)
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Settings()
