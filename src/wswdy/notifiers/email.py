"""SMTP-backed notifier."""
from email.message import EmailMessage
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
        if image_path is not None:
            # Build multipart/related > multipart/alternative + inline image
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
            related = MIMEMultipart("related")
            alternative = MIMEMultipart("alternative")
            alternative.attach(MIMEText(text, "plain"))
            alternative.attach(MIMEText(html, "html"))
            related.attach(alternative)
            # Attach inline image referenced as cid:preview
            img_part = MIMEImage(image_path.read_bytes(), "png")
            img_part.add_header("Content-ID", "<preview>")
            img_part.add_header("Content-Disposition", "inline")
            related.attach(img_part)
            msg: EmailMessage | MIMEMultipart = related
        else:
            msg = EmailMessage()
            msg.set_content(text)

        msg["From"] = self.sender
        msg["To"] = recipient
        msg["Subject"] = subject

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
