import json

import httpx
import pytest
import respx

from wswdy.clients.whatsapp_mcp import (
    McpSessionExpired,
    McpUnreachable,
    send_message,
)


@respx.mock
async def test_send_message_ok():
    respx.post("https://mcp.test/api/send").mock(
        return_value=httpx.Response(200, json={"success": True, "message": "sent"})
    )
    out = await send_message(base_url="https://mcp.test", token="t",
                             to="+12025551234", text="hi", image_path=None)
    assert out["status"] == "ok"


@respx.mock
async def test_send_message_session_expired():
    respx.post("https://mcp.test/api/send").mock(
        return_value=httpx.Response(
            200, json={"success": False, "message": "Not connected to WhatsApp"}
        )
    )
    with pytest.raises(McpSessionExpired):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_unreachable():
    respx.post("https://mcp.test/api/send").mock(side_effect=httpx.ConnectError("nope"))
    with pytest.raises(McpUnreachable):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_passes_media_path(tmp_path):
    img = tmp_path / "x.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "message": "ok"})

    respx.post("https://mcp.test/api/send").mock(side_effect=handler)
    await send_message(base_url="https://mcp.test", token="t",
                       to="+12025551234", text="hi", image_path=img)
    assert captured["body"]["recipient"] == "+12025551234"
    assert captured["body"]["message"] == "hi"
    assert captured["body"]["media_path"].endswith("x.png")


@respx.mock
async def test_send_message_rejected_returns_status():
    respx.post("https://mcp.test/api/send").mock(
        return_value=httpx.Response(
            200, json={"success": False, "message": "Recipient is required"}
        )
    )
    out = await send_message(base_url="https://mcp.test", token="t",
                             to="+12025551234", text="hi")
    assert out["status"] == "rejected"
    assert "Recipient" in out["detail"]
