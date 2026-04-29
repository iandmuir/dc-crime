from pathlib import Path

import httpx
import respx

from wswdy.alerts import AdminAlerter
from wswdy.jobs.fetch import run_fetch
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.fetch_log import last_attempt

FIXTURE = Path(__file__).parent / "fixtures" / "mpd_sample.geojson"


@respx.mock
async def test_fetch_success_upserts_and_logs(db):
    respx.get("https://feed.test/q").mock(
        return_value=httpx.Response(200, content=FIXTURE.read_bytes())
    )
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter)
    assert out["status"] == "ok"
    assert last_attempt(db)["status"] == "ok"
    # At least one crime upserted
    assert db.execute("SELECT COUNT(*) FROM crimes").fetchone()[0] > 0


@respx.mock
async def test_fetch_retries_on_failure_then_succeeds(db):
    route = respx.get("https://feed.test/q")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, content=FIXTURE.read_bytes()),
    ]
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter,
                          retry_delays_s=[0, 0])  # no real sleeping
    assert out["status"] == "ok"
    assert route.call_count == 3


@respx.mock
async def test_fetch_all_attempts_fail_alerts_admin(db):
    respx.get("https://feed.test/q").mock(return_value=httpx.Response(503))
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter,
                          retry_delays_s=[0, 0])
    assert out["status"] == "failed"
    assert last_attempt(db)["status"] == "failed"
    assert email.sent  # admin emailed
    assert email.sent[0]["subject"].startswith("[wswdy] mpd_down")


async def test_fetch_uses_fixture_when_path_provided(db, tmp_path):
    fixture = tmp_path / "mpd.json"
    fixture.write_bytes(FIXTURE.read_bytes())
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://unused.test", alerter=alerter,
                          fixture_path=str(fixture))
    assert out["status"] == "ok"
    assert db.execute("SELECT COUNT(*) FROM crimes").fetchone()[0] > 0
