import httpx
import respx

from wswdy.alerts import AdminAlerter
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.admin_alerts import is_suppressed, list_recent


@respx.mock
async def test_alert_sends_email_and_webhook_and_records(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook",
                     suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="MPD 503 for 30min")
    assert email.sent and email.sent[0]["recipient"] == "admin@x"
    assert "MPD 503" in email.sent[0]["text"]
    assert respx.calls.call_count == 1
    assert is_suppressed(db, "mpd_down")
    assert list_recent(db)[0]["alert_type"] == "mpd_down"


@respx.mock
async def test_alert_suppressed_within_window(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook", suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="first")
    await a.alert(alert_type="mpd_down", message="second")  # suppressed
    assert len(email.sent) == 1
    assert respx.calls.call_count == 1


@respx.mock
async def test_alert_distinct_types_are_independent(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook", suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="m1")
    await a.alert(alert_type="whatsapp_session_expired", message="m2")
    assert len(email.sent) == 2


async def test_alert_no_webhook_url_skips_webhook(db):
    email = FakeNotifier()
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="", suppression_hours=6)
    await a.alert(alert_type="x", message="y")
    assert email.sent  # email still sent


@respx.mock
async def test_alert_webhook_failure_does_not_block_email(db):
    """HA webhook failures are swallowed — email must still be delivered."""
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(side_effect=httpx.ConnectError("refused"))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook", suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="MPD is down")
    assert email.sent  # email was sent despite webhook failure
