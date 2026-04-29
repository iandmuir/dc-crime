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
                   image_path: Path | None,
                   unsubscribe_url: str | None = None) -> SendResult:
        # The shared digest text ends with "Reply STOP to unsubscribe.",
        # which makes sense for WhatsApp but is meaningless for email
        # (we don't parse inbound mail). Strip it for the email channel
        # and replace with an unsubscribe URL line in plain text + the
        # styled footer link in HTML.
        plain_text = _email_plain_text(text, unsubscribe_url)
        html_body = _render_html(
            _strip_reply_stop(text),
            has_image=image_path is not None,
            unsubscribe_url=unsubscribe_url,
        )

        if image_path is not None:
            # multipart/related > multipart/alternative > [text + html] + image
            related = MIMEMultipart("related")
            alternative = MIMEMultipart("alternative")
            alternative.attach(MIMEText(plain_text, "plain"))
            alternative.attach(MIMEText(html_body, "html"))
            related.attach(alternative)
            img_part = MIMEImage(image_path.read_bytes(), "png")
            img_part.add_header("Content-ID", "<preview>")
            img_part.add_header("Content-Disposition", "inline")
            related.attach(img_part)
            msg: EmailMessage | MIMEMultipart = related
        else:
            # multipart/alternative with text + html
            alternative = MIMEMultipart("alternative")
            alternative.attach(MIMEText(plain_text, "plain"))
            alternative.attach(MIMEText(html_body, "html"))
            msg = alternative

        msg["From"] = self.sender
        msg["To"] = recipient
        msg["Subject"] = subject
        # RFC 2369 / 8058: most clients (Gmail, Apple Mail, etc.) render a
        # native one-click unsubscribe header — invisible spam-fighter goodness.
        if unsubscribe_url:
            msg["List-Unsubscribe"] = f"<{unsubscribe_url}>"
            msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"

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


_REPLY_STOP_LINE = "Reply STOP to unsubscribe."


def _strip_reply_stop(text: str) -> str:
    """Remove the WhatsApp-style "Reply STOP" line from the digest body. Email
    clients can't act on it, so we replace with a real link instead."""
    # Strip line + any trailing/leading whitespace it left behind
    out = text.replace("\n\n" + _REPLY_STOP_LINE, "")
    out = out.replace("\n" + _REPLY_STOP_LINE, "")
    out = out.replace(_REPLY_STOP_LINE, "")
    return out.rstrip()


def _email_plain_text(text: str, unsubscribe_url: str | None) -> str:
    """Build the plain-text email body. Drops the WhatsApp-style 'Reply STOP'
    line and (when present) appends a real unsubscribe URL the recipient
    can click in any reasonable mail client."""
    body = _strip_reply_stop(text)
    if unsubscribe_url:
        body += f"\n\nUnsubscribe: {unsubscribe_url}"
    return body


def _escape(s: str) -> str:
    """HTML-escape the text content for safe embedding in an HTML email."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _render_html(text: str, *, has_image: bool, unsubscribe_url: str | None) -> str:
    """Render the digest text as a styled HTML email with optional inline image
    placeholder (cid:preview) and an unsubscribe footer link."""
    body_inner = (
        f"<pre style='font: 14px/1.5 ui-monospace, monospace; white-space:pre-wrap;"
        f" background:#fff; padding:18px; border:1px solid #E5E3DC; border-radius:10px;"
        f" margin:0;'>{_escape(text)}</pre>"
    )
    if has_image:
        body_inner += (
            "<img src='cid:preview' style='display:block;margin-top:12px;"
            "max-width:100%;border:1px solid #E5E3DC;border-radius:10px;' />"
        )
    if unsubscribe_url:
        body_inner += (
            f"<div style='margin-top:18px;padding-top:14px;border-top:1px solid #E5E3DC;"
            f"font: 12px/1.5 -apple-system, system-ui, sans-serif;color:#737373;"
            f"text-align:center;'>"
            f"You're getting this because you signed up at wtfdc.iandmuir.com. "
            f"<a href='{_escape(unsubscribe_url)}' style='color:#737373;'>"
            f"Unsubscribe</a>."
            f"</div>"
        )
    return (
        f"<html><body style='font-family: -apple-system, system-ui, sans-serif;"
        f" background:#FAFAF6; padding:24px; margin:0;'>"
        f"{body_inner}"
        f"</body></html>"
    )
