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


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_strips_reply_stop_and_adds_unsubscribe_link(mock_send):
    """Email body should NOT contain "Reply STOP" (it's WhatsApp-only — we
    don't parse inbound mail). The unsubscribe URL replaces it in plain
    text and lives in a styled footer link in HTML."""
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    text = (
        "Good morning, Jane ☀️\n\n"
        "Quiet night — 0 crimes reported.\n\n"
        "🗺️ Map: https://x/map/abc?token=t\n\n"
        "Reply STOP to unsubscribe."
    )
    await n.send(
        recipient="to@y", subject="s", text=text, image_path=None,
        unsubscribe_url="https://x/u/abc?token=t",
    )
    msg = mock_send.call_args.args[0]
    parts = {p.get_content_type(): p.get_payload(decode=True).decode("utf-8")
             for p in msg.walk() if p.get_content_type() in
             ("text/plain", "text/html")}

    plain = parts["text/plain"]
    assert "Reply STOP" not in plain
    assert "Unsubscribe: https://x/u/abc?token=t" in plain

    html = parts["text/html"]
    assert "Reply STOP" not in html
    assert "https://x/u/abc?token=t" in html
    # And the standard List-Unsubscribe header is also set
    assert msg["List-Unsubscribe"] == "<https://x/u/abc?token=t>"


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_without_unsubscribe_url_still_strips_reply_stop(mock_send):
    """If no unsubscribe_url is supplied, just strip the Reply STOP line
    cleanly (don't leave a dangling 'Unsubscribe: None')."""
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    text = "Hi.\n\nReply STOP to unsubscribe."
    await n.send(recipient="to@y", subject="s", text=text, image_path=None)
    msg = mock_send.call_args.args[0]
    plain = next(p.get_payload(decode=True).decode("utf-8")
                 for p in msg.walk() if p.get_content_type() == "text/plain")
    assert "Reply STOP" not in plain
    assert "Unsubscribe:" not in plain
    assert plain.strip() == "Hi."
