"""WhatsApp notifier — wraps the MCP HTTP client into the Notifier protocol."""
from pathlib import Path

from wswdy.clients.whatsapp_mcp import McpSessionExpired, McpUnreachable, send_message
from wswdy.notifiers.base import SendResult


class WhatsAppMcpNotifier:
    """Structurally satisfies the Notifier protocol via the WhatsApp MCP bridge."""

    def __init__(self, *, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        """Send a WhatsApp message. `subject` is ignored (WhatsApp has no subject line)."""
        try:
            res = await send_message(
                base_url=self.base_url, token=self.token,
                to=recipient, text=text, image_path=image_path,
            )
        except McpSessionExpired as e:
            return SendResult(ok=False, error="session_expired", detail=str(e))
        except McpUnreachable as e:
            return SendResult(ok=False, error="unreachable", detail=str(e))
        if res.get("status") != "ok":
            return SendResult(ok=False, error="rejected", detail=str(res))
        return SendResult(ok=True, detail=res.get("id"))
