"""SMTP-backed notifier."""
import logging
from email.message import EmailMessage
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import aiosmtplib

from wswdy.notifiers.base import SendResult

logger = logging.getLogger(__name__)


class EmailNotifier:
    """Sends email via aiosmtplib. Implements the Notifier protocol.

    TLS mode is auto-selected from the port:
      - 465: implicit TLS (TLS established before SMTP). Resend's TLS port.
      - 587: STARTTLS (upgrade after EHLO). Resend's submission port.
    Override via the use_tls / use_starttls kwargs if you need to force one.
    """

    def __init__(self, *, host: str, port: int, user: str, password: str,
                 sender: str, use_tls: bool | None = None,
                 use_starttls: bool | None = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sender = sender
        # Auto-select TLS strategy from port if caller didn't say.
        if use_tls is None and use_starttls is None:
            self.use_tls = port == 465
            self.use_starttls = port != 465
        else:
            self.use_tls = bool(use_tls)
            self.use_starttls = bool(use_starttls)

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
                use_tls=self.use_tls,
                start_tls=self.use_starttls,
            )
        except Exception as e:
            logger.warning(
                "SMTP send failed (host=%s port=%d to=%s): %s: %s",
                self.host, self.port, recipient, type(e).__name__, e,
            )
            return SendResult(ok=False, error=f"{type(e).__name__}: {e}", detail=str(e))
        return SendResult(ok=True)


def _escape(s: str) -> str:
    """HTML-escape the text content for safe embedding in an HTML email."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
