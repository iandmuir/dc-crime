"""HTTP client for the WhatsApp MCP bridge in the adjacent LXC.

Contract:
  POST {base_url}/send
  Headers: Authorization: Bearer <token>
  Body (multipart if image_path is given, else json):
    {to, text}                                     (json)
    {to, text, image (file)}                       (multipart)
  Returns 200 {status: "ok"}            on success
          401 {error: "session_expired"} when WhatsApp Web session is gone
          5xx / connection error         otherwise
"""
from pathlib import Path

import httpx


class McpUnreachable(Exception):
    """Raised when the MCP service is unreachable (network or 5xx)."""


class McpSessionExpired(Exception):
    """Raised when the MCP returns 401/session_expired — needs QR re-scan."""


async def send_message(
    *,
    base_url: str,
    token: str,
    to: str,
    text: str,
    image_path: str | Path | None = None,
    timeout_s: float = 30.0,
) -> dict:
    """Send a WhatsApp message via the MCP bridge.

    Raises:
        McpSessionExpired: when the bridge returns 401.
        McpUnreachable: on network errors or 5xx responses.
    """
    url = base_url.rstrip("/") + "/send"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            if image_path is not None:
                image_path = Path(image_path)
                with image_path.open("rb") as f:
                    files = {"image": (image_path.name, f, "image/png")}
                    data = {"to": to, "text": text}
                    r = await client.post(url, headers=headers, data=data, files=files)
            else:
                r = await client.post(url, headers=headers, json={"to": to, "text": text})
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        raise McpUnreachable(str(e)) from e

    if r.status_code == 401:
        raise McpSessionExpired(r.text)
    if r.status_code >= 500:
        raise McpUnreachable(f"{r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()
