"""HTTP client for the lharries/whatsapp-mcp Go bridge.

The bridge runs on the same host as wswdy, on a localhost port.

Bridge contract (from whatsapp-bridge/main.go):
  POST {base_url}/api/send
  Body (JSON):
    {
      "recipient":   "+1XXXXXXXXXX" or "<jid>@s.whatsapp.net",
      "message":     "text body",
      "media_path":  "/abs/path/to/image.png"   # optional
    }
  Returns 200 {success: true,  message: "..."} on success
          200 {success: false, message: "..."} on logical failure
          400/4xx                              on bad request
          5xx / connection error               on transport problems

Because it's localhost-only, no auth header is required (the bridge is
bound to 127.0.0.1, so external traffic can't reach it). The `token`
parameter is accepted for API compatibility but not sent.

Media is passed as a filesystem path the bridge can read directly,
NOT as a multipart upload. wswdy renders maps to `log_dir/static_maps/`
which the bridge user (whatsapp) must be able to read.
"""
from pathlib import Path

import httpx


class McpUnreachable(Exception):
    """Raised when the bridge is unreachable (network or 5xx)."""


class McpSessionExpired(Exception):
    """Raised when the WhatsApp session has been invalidated — needs QR re-scan."""


async def send_message(
    *,
    base_url: str,
    token: str,  # noqa: ARG001 — kept for API compat; bridge has no auth
    to: str,
    text: str,
    image_path: str | Path | None = None,
    timeout_s: float = 30.0,
) -> dict:
    """Send a WhatsApp message via the local Go bridge.

    Returns a dict with at least `{"status": "ok" | ..., "id": ...}`-shaped
    keys (normalized from the bridge's `{success, message}` response).

    Raises:
        McpSessionExpired: when the bridge reports a session/disconnection error.
        McpUnreachable: on network errors or 5xx responses.
    """
    url = base_url.rstrip("/") + "/api/send"
    payload: dict[str, str] = {"recipient": to, "message": text}
    if image_path is not None:
        payload["media_path"] = str(Path(image_path).resolve())

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.post(url, json=payload)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        raise McpUnreachable(str(e)) from e

    if r.status_code >= 500:
        raise McpUnreachable(f"{r.status_code}: {r.text}")
    r.raise_for_status()

    body = r.json()
    # Bridge returns {success: bool, message: str}. Normalize.
    if body.get("success"):
        return {"status": "ok", "id": body.get("message", "")}

    msg = (body.get("message") or "").lower()
    if "not connected" in msg or "session" in msg or "logged out" in msg:
        raise McpSessionExpired(body.get("message", "session expired"))
    return {"status": "rejected", "detail": body.get("message", "")}
