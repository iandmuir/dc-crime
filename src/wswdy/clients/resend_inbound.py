"""Resend Inbound — list + download attachments for a received email.

Resend's `email.received` webhook payload contains metadata only — no
body, no headers, no attachment bytes. To get the actual file content
we call the Attachments API:

    GET https://api.resend.com/emails/receiving/{email_id}/attachments

which returns each attachment with a short-lived `download_url` (signed
URL with an `expires_at`). We GET that URL to pull the bytes.

The two functions here keep a clean split:

  - ``list_attachments`` — fetches the metadata + download URLs
  - ``download_attachment`` — pulls the bytes for one attachment

So the webhook handler can iterate over attachments, filter to PDFs,
and persist only those.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)

API_BASE = "https://api.resend.com"


class ResendInboundError(Exception):
    """Raised when Resend's API returns an unexpected response."""


async def get_received_email(
    email_id: str, *, api_key: str, timeout_s: float = 15.0,
) -> dict[str, Any]:
    """Fetch a received email's full payload (subject, html, text, headers).

    The webhook payload only carries metadata, so to read the email body
    we have to call this endpoint:

        GET https://api.resend.com/emails/receiving/{email_id}

    Returns the email's full JSON record. Important keys for our use:
    ``subject``, ``text`` (plain-text body), ``html``, ``from``, ``to``.
    """
    if not api_key:
        raise ResendInboundError("Resend API key is not configured")
    url = f"{API_BASE}/emails/receiving/{email_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(url, headers=headers)
        if r.status_code >= 400:
            raise ResendInboundError(
                f"get_received_email {r.status_code}: {r.text[:200]}"
            )
        return r.json()


async def list_attachments(
    email_id: str, *, api_key: str, timeout_s: float = 15.0,
) -> list[dict[str, Any]]:
    """Return the list of attachment records for a received email.

    Each record is a dict with at least ``id``, ``filename``,
    ``content_type``, ``size``, and ``download_url``. Other fields
    (``content_id``, ``inline``, ``content_disposition``) may be
    present depending on the message.
    """
    if not api_key:
        raise ResendInboundError("Resend API key is not configured")
    url = f"{API_BASE}/emails/receiving/{email_id}/attachments"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(url, headers=headers)
        if r.status_code >= 400:
            raise ResendInboundError(
                f"list_attachments {r.status_code}: {r.text[:200]}"
            )
        body = r.json()
    # The API returns either {"data": [...]} or a bare list, depending on
    # endpoint versioning — accept both for forward compatibility.
    if isinstance(body, dict) and "data" in body:
        return list(body["data"])
    if isinstance(body, list):
        return body
    raise ResendInboundError(f"unexpected attachments response shape: {type(body)}")


async def download_attachment(
    download_url: str, *, timeout_s: float = 30.0,
) -> bytes:
    """Fetch the raw bytes of a single attachment via its download URL."""
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(download_url)
        if r.status_code >= 400:
            raise ResendInboundError(
                f"download_attachment {r.status_code}: {r.text[:200]}"
            )
        return r.content
