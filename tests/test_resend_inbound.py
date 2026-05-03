"""Tests for clients.resend_inbound — list + download attachments."""
from unittest.mock import AsyncMock, patch

import pytest

from wswdy.clients.resend_inbound import (
    ResendInboundError,
    download_attachment,
    list_attachments,
)


def _mock_client(responses):
    """Build a mock httpx.AsyncClient context manager whose .get() returns
    the supplied responses in order."""
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get = AsyncMock(side_effect=responses)
    return client


def _resp(status: int, *, json=None, content: bytes = b"", text: str = ""):
    r = AsyncMock()
    r.status_code = status
    r.json = lambda: json
    r.content = content
    r.text = text
    return r


@pytest.mark.asyncio
async def test_list_attachments_unwraps_data_envelope():
    payload = {"data": [
        {"id": "a1", "filename": "x.pdf", "download_url": "u1"},
        {"id": "a2", "filename": "y.pdf", "download_url": "u2"},
    ]}
    with patch("wswdy.clients.resend_inbound.httpx.AsyncClient",
               return_value=_mock_client([_resp(200, json=payload)])):
        out = await list_attachments("eid", api_key="k")
    assert [a["id"] for a in out] == ["a1", "a2"]


@pytest.mark.asyncio
async def test_list_attachments_handles_bare_list():
    payload = [{"id": "a1", "filename": "x.pdf"}]
    with patch("wswdy.clients.resend_inbound.httpx.AsyncClient",
               return_value=_mock_client([_resp(200, json=payload)])):
        out = await list_attachments("eid", api_key="k")
    assert out[0]["id"] == "a1"


@pytest.mark.asyncio
async def test_list_attachments_raises_on_http_error():
    with patch("wswdy.clients.resend_inbound.httpx.AsyncClient",
               return_value=_mock_client([_resp(403, text="forbidden")])):
        with pytest.raises(ResendInboundError, match="403"):
            await list_attachments("eid", api_key="k")


@pytest.mark.asyncio
async def test_list_attachments_requires_api_key():
    with pytest.raises(ResendInboundError, match="not configured"):
        await list_attachments("eid", api_key="")


@pytest.mark.asyncio
async def test_download_attachment_returns_bytes():
    with patch("wswdy.clients.resend_inbound.httpx.AsyncClient",
               return_value=_mock_client([_resp(200, content=b"PDF...")])):
        out = await download_attachment("https://x/y")
    assert out == b"PDF..."


@pytest.mark.asyncio
async def test_download_attachment_raises_on_http_error():
    with patch("wswdy.clients.resend_inbound.httpx.AsyncClient",
               return_value=_mock_client([_resp(404, text="missing")])):
        with pytest.raises(ResendInboundError, match="404"):
            await download_attachment("https://x/y")
