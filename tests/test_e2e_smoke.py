"""End-to-end happy path with no real network.

Stubs MapTiler geocoding and replaces real notifiers with FakeNotifier so the
test runs offline. Exercises every public surface in sequence.
"""
import re
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import upsert_many


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "ADMINTOK")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "smoke.db"))
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    monkeypatch.setenv("ADMIN_EMAIL", "ian@test")
    return create_app()


def _seed_recent_crime(app, hours_ago=2):
    when = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat(timespec="seconds")
    upsert_many(app.state.db, [{
        "ccn": "SMOKE1", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
        "block_address": "1400 block of P St NW", "lat": 38.9100, "lon": -77.0319,
        "report_dt": when, "start_dt": None, "end_dt": None,
        "ward": "2", "district": "3", "raw_json": "{}",
    }])


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
async def test_full_happy_path(mock_geo, app, tmp_path):
    fake_email = FakeNotifier()
    fake_wa = FakeNotifier()
    app.state.email_notifier = fake_email
    app.state.whatsapp_notifier = fake_wa
    # Refresh alerter to use the fake email
    from wswdy.alerts import AdminAlerter
    app.state.alerter = AdminAlerter(
        db=app.state.db, email=fake_email,
        admin_email="ian@test", ha_webhook_url="",
    )

    mock_geo.return_value = {"lat": 38.9097, "lon": -77.0319,
                              "display": "1500 14th St NW, Washington, DC"}

    client = TestClient(app)

    # 1. Get the signup form
    r = client.get("/")
    assert r.status_code == 200

    # 2. Submit signup
    r = client.post("/signup", data={
        "display_name": "Ian", "address_text": "1500 14th St NW",
        "preferred_channel": "email", "email": "ian@test",
        "radius_m": "1000",
    }, follow_redirects=False)
    assert r.status_code == 303

    # 3. Admin gets the review email — subject is "[wswdy] new signup: Ian"
    review_emails = [e for e in fake_email.sent if "new signup" in e["subject"].lower()]
    assert review_emails
    review_text = review_emails[-1]["text"]
    # Extract the approve URL — format is https://x.test/a/{token}
    m = re.search(r"https://x\.test/a/([^\s]+)", review_text)
    assert m
    token = m.group(1)

    # 4. Approve
    r = client.post(f"/a/{token}/approve", follow_redirects=False)
    assert r.status_code == 303
    approved = subs_repo.list_by_status(app.state.db, "APPROVED")
    assert len(approved) == 1
    sid = approved[0]["id"]

    # 5. Welcome email landed — subject is "Welcome to wswdy, Ian"
    welcome = [e for e in fake_email.sent if "welcome" in e["subject"].lower()
               or "confirmed" in e["text"].lower()]
    assert welcome

    # 6. Seed a crime in the radius
    _seed_recent_crime(app)

    # 7. Manually run the daily send (no scheduler dependency in test)
    from wswdy.jobs.send import run_daily_sends
    out = await run_daily_sends(
        db=app.state.db, email=fake_email, whatsapp=fake_wa,
        alerter=app.state.alerter,
        base_url="https://x.test", hmac_secret="secret",
        send_date=str(date.today()),
        now_iso=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        stagger=False, render_static_map=None,
    )
    assert out["sent"] == 1
    # Digest subject: "DC briefing for Ian — {date}"
    digests = [e for e in fake_email.sent
               if "DC briefing" in e["subject"] or "Good morning" in e["text"]]
    assert digests
    digest = digests[-1]
    assert "Ian" in digest["text"]
    # ROBBERY + GUN → "Armed robbery" in digest; "armed robbery" in lowercased text
    assert "robbery" in digest["text"].lower()

    # 8. Visit the map
    from wswdy.tokens import sign
    map_token = sign("secret", purpose="map", subscriber_id=sid)
    r = client.get(f"/map/{sid}?token={map_token}")
    assert r.status_code == 200
    r = client.get(f"/api/crimes?subscriber={sid}&token={map_token}&window=24h")
    assert r.status_code == 200
    assert len(r.json()["features"]) == 1

    # 9. Hit /api/preview
    r = client.post("/api/preview",
                    json={"lat": 38.9097, "lon": -77.0319, "radius_m": 1000})
    assert r.status_code == 200

    # 10. Visit admin
    r = client.get("/admin?token=ADMINTOK")
    assert r.status_code == 200

    # 11. Unsubscribe
    unsub_token = sign("secret", purpose="unsubscribe", subscriber_id=sid)
    r = client.post(f"/u/{sid}?token={unsub_token}")
    assert r.status_code == 200
    s = subs_repo.get(app.state.db, sid)
    assert s["status"] == "UNSUBSCRIBED"
