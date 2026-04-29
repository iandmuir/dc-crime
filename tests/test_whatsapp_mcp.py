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
    respx.post("https://mcp.test/send").mock(
        return_value=httpx.Response(200, json={"status": "ok"})
    )
    out = await send_message(base_url="https://mcp.test", token="t",
                             to="+12025551234", text="hi", image_path=None)
    assert out["status"] == "ok"


@respx.mock
async def test_send_message_session_expired():
    respx.post("https://mcp.test/send").mock(
        return_value=httpx.Response(401, json={"error": "session_expired"})
    )
    with pytest.raises(McpSessionExpired):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_unreachable():
    respx.post("https://mcp.test/send").mock(side_effect=httpx.ConnectError("nope"))
    with pytest.raises(McpUnreachable):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_attaches_image(tmp_path):
    img = tmp_path / "x.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content
        return httpx.Response(200, json={"status": "ok"})

    respx.post("https://mcp.test/send").mock(side_effect=handler)
    await send_message(base_url="https://mcp.test", token="t",
                       to="+12025551234", text="hi", image_path=img)
    assert b"x.png" in captured["body"] or b"image" in captured["body"]
