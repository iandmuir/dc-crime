"""Notifier protocol and supporting types."""
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class SendResult:
    ok: bool
    error: str | None = None
    detail: str | None = None  # provider-specific detail (message id, etc.)


@runtime_checkable
class Notifier(Protocol):
    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None,
                   unsubscribe_url: str | None = None) -> SendResult: ...


async def dispatch(
    subscriber: dict,
    *,
    email_notifier: "Notifier",
    whatsapp_notifier: "Notifier",
    subject: str,
    text: str,
    image_path: "Path | None",
    unsubscribe_url: "str | None" = None,
) -> SendResult:
    """Send to the subscriber's preferred channel; falls back to email if WhatsApp
    is unreachable AND email is on file. Does NOT fall back on session_expired
    (operator-actionable — don't double-send).

    `unsubscribe_url` is optional metadata the email notifier renders into a
    footer link in the HTML body. The plain-text body and WhatsApp messages
    don't show it (WhatsApp users use "STOP" replies instead)."""
    channel = subscriber["preferred_channel"]
    if channel == "email":
        return await email_notifier.send(
            recipient=subscriber["email"], subject=subject,
            text=text, image_path=image_path,
            unsubscribe_url=unsubscribe_url,
        )

    # WhatsApp path
    res = await whatsapp_notifier.send(
        recipient=subscriber["phone"], subject=subject,
        text=text, image_path=image_path,
    )
    if res.ok or res.error == "session_expired" or not subscriber.get("email"):
        return res
    # Fall back to email on transient WhatsApp failures only
    return await email_notifier.send(
        recipient=subscriber["email"], subject=subject,
        text=text, image_path=image_path,
        unsubscribe_url=unsubscribe_url,
    )
