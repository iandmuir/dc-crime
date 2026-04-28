from unittest.mock import AsyncMock, patch

from wswdy.clients.whatsapp_mcp import McpSessionExpired, McpUnreachable
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_ok(mock_send):
    mock_send.return_value = {"status": "ok", "id": "msg_123"}
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="ignored",
                     text="hi", image_path=None)
    assert r.ok is True
    mock_send.assert_called_once()


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_session_expired_returns_special_error(mock_send):
    mock_send.side_effect = McpSessionExpired("session_expired")
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="x", text="y", image_path=None)
    assert r.ok is False
    assert r.error == "session_expired"


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_unreachable_returns_unreachable(mock_send):
    mock_send.side_effect = McpUnreachable("connect refused")
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="x", text="y", image_path=None)
    assert r.ok is False
    assert r.error == "unreachable"
    assert "connect refused" in r.detail
