from wswdy.notifiers.base import dispatch, SendResult
from wswdy.notifiers.fake import FakeNotifier


SUB = {"id": "s1", "preferred_channel": "whatsapp",
       "phone": "+12025551234", "email": "fall@back.com"}


async def test_dispatch_routes_to_preferred_channel():
    email = FakeNotifier()
    wa = FakeNotifier()
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert wa.sent and not email.sent
    assert wa.sent[0]["recipient"] == "+12025551234"


async def test_dispatch_falls_back_to_email_on_whatsapp_unreachable():
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="unreachable")
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert wa.sent and email.sent
    assert email.sent[0]["recipient"] == "fall@back.com"


async def test_dispatch_does_not_fall_back_on_session_expired():
    # Session-expired is operator-actionable; we don't double-send.
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="session_expired")
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "session_expired"
    assert email.sent == []


async def test_dispatch_email_subscriber_uses_email_directly():
    email = FakeNotifier()
    wa = FakeNotifier()
    sub = {**SUB, "preferred_channel": "email"}
    r = await dispatch(sub, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert email.sent and not wa.sent
    assert email.sent[0]["recipient"] == "fall@back.com"


async def test_dispatch_whatsapp_no_email_fallback_returns_failure():
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="unreachable")
    sub = {**SUB, "email": None}
    r = await dispatch(sub, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "unreachable"
