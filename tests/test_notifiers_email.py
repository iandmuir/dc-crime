from unittest.mock import AsyncMock, patch

from wswdy.notifiers.email import EmailNotifier


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_ok(mock_send):
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p",
                     sender="from@x")
    r = await n.send(recipient="to@y", subject="s", text="t", image_path=None)
    assert r.ok is True
    args, kwargs = mock_send.call_args
    assert kwargs["hostname"] == "smtp.test"
    msg = args[0]
    assert msg["To"] == "to@y"
    assert msg["From"] == "from@x"
    assert msg["Subject"] == "s"


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_with_inline_image(mock_send, tmp_path):
    img = tmp_path / "preview.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    await n.send(recipient="to@y", subject="s", text="hello", image_path=img)
    msg = mock_send.call_args.args[0]
    # Walk the multipart and ensure an image part exists
    parts = list(msg.walk())
    assert any(p.get_content_type() == "image/png" for p in parts)


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_failure(mock_send):
    mock_send.side_effect = Exception("connection refused")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    r = await n.send(recipient="to@y", subject="s", text="t", image_path=None)
    assert r.ok is False
    assert "connection refused" in r.error
