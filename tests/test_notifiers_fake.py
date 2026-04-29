from wswdy.notifiers.base import SendResult
from wswdy.notifiers.fake import FakeNotifier


async def test_fake_notifier_records_sends():
    n = FakeNotifier()
    r = await n.send(recipient="x@y.com", subject="hi", text="body", image_path=None)
    assert isinstance(r, SendResult)
    assert r.ok is True
    assert n.sent == [{"recipient": "x@y.com", "subject": "hi",
                       "text": "body", "image_path": None}]


async def test_fake_notifier_can_be_set_to_fail():
    n = FakeNotifier(fail_with="boom")
    r = await n.send(recipient="x@y.com", subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "boom"
