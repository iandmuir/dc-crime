"""SMTP-backed notifier."""
from email.message import EmailMessage
from pathlib import Path

import aiosmtplib

from wswdy.notifiers.base import SendResult


class EmailNotifier:
    """Sends email via aiosmtplib (STARTTLS). Implements the Notifier protocol."""

    def __init__(self, *, host: str, port: int, user: str, password: str,
                 sender: str, use_starttls: bool = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sender = sender
        self.use_starttls = use_starttls

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = recipient
        msg["Subject"] = subject

        if image_path is not None:
            # Multipart: plain-text fallback + HTML body with inline image via cid:preview
            html = (
                f"<html><body style='font-family: -apple-system, system-ui, sans-serif;"
                f" background:#FAFAF6; padding:24px;'>"
                f"<pre style='font: 14px/1.5 ui-monospace, monospace; white-space:pre-wrap;"
                f" background:#fff; padding:18px; border:1px solid #E5E3DC; border-radius:10px;'>"
                f"{_escape(text)}</pre>"
                f"<img src='cid:preview' style='display:block;margin-top:12px;"
                f"max-width:100%;border:1px solid #E5E3DC;border-radius:10px;' />"
                f"</body></html>"
            )
            msg.set_content(text)  # plain-text fallback
            msg.add_alternative(html, subtype="html")
            # Attach inline image referenced as cid:preview
            data = image_path.read_bytes()
            msg.get_payload()[1].add_related(
                data, maintype="image", subtype="png", cid="<preview>",
            )
        else:
            msg.set_content(text)

        try:
            await aiosmtplib.send(
                msg,
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                start_tls=self.use_starttls,
            )
        except Exception as e:
            return SendResult(ok=False, error=str(e))
        return SendResult(ok=True)


def _escape(s: str) -> str:
    """HTML-escape the text content for safe embedding in an HTML email."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
