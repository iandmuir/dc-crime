"""Resend Inbound webhook — receive emails and ingest their PDFs.

Flow:

  1. Resend POSTs ``email.received`` events here, signed Svix-style.
  2. We verify the signature (timestamp tolerance + HMAC-SHA256 over
     ``svix_id.svix_timestamp.body``).
  3. We list the email's attachments via the Resend Attachments API,
     download each PDF, save it to a per-message temp directory, and
     hand it off to ``jobs.pdf_ingest.ingest_pdf`` which auto-routes
     crime vs. arrest by header text.
  4. We always return 200 to Resend after a successful signature check —
     ingest failures are logged but don't trigger retries (the payload
     would just keep arriving with the same bad PDF).

Why verify Svix-style: Resend uses Svix as their webhook delivery
infrastructure, so the headers and signing scheme exactly match
https://docs.svix.com/receiving/verifying-payloads/how-manual.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import tempfile
import time
from pathlib import Path

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse, Response

from wswdy.clients.resend_inbound import (
    ResendInboundError,
    download_attachment,
    list_attachments,
)
from wswdy.jobs.pdf_ingest import ingest_pdf

log = logging.getLogger(__name__)

router = APIRouter()


# ---------- Svix signature verification ----------------------------------

class WebhookAuthError(Exception):
    """Raised when a webhook request fails signature verification."""


def _decode_secret(secret: str) -> bytes:
    """Strip the ``whsec_`` prefix Resend includes and base64-decode."""
    if secret.startswith("whsec_"):
        secret = secret[len("whsec_"):]
    # Svix secrets are base64-encoded; support raw secrets too as a
    # fallback in case Resend ever changes the format.
    try:
        return base64.b64decode(secret)
    except Exception:
        return secret.encode("utf-8")


def verify_svix_signature(
    *, body: bytes, svix_id: str, svix_timestamp: str, svix_signature: str,
    secret: str, tolerance_s: int = 300, now: float | None = None,
) -> None:
    """Verify a Svix-signed webhook. Raises ``WebhookAuthError`` on failure.

    The ``svix-signature`` header may contain multiple space-separated
    signatures (e.g. ``v1,abc... v1,def...``) when keys are rotated; we
    accept the request if ANY of them matches.
    """
    if not (svix_id and svix_timestamp and svix_signature):
        raise WebhookAuthError("missing svix-* headers")
    try:
        ts = int(svix_timestamp)
    except ValueError as e:
        raise WebhookAuthError(f"invalid svix-timestamp: {e}") from e
    current = now if now is not None else time.time()
    if abs(current - ts) > tolerance_s:
        raise WebhookAuthError(
            f"timestamp outside tolerance ({abs(current - ts):.0f}s > "
            f"{tolerance_s}s)"
        )
    if not secret:
        raise WebhookAuthError("webhook secret is not configured")

    key = _decode_secret(secret)
    signed_payload = f"{svix_id}.{svix_timestamp}.".encode() + body
    expected = base64.b64encode(
        hmac.new(key, signed_payload, hashlib.sha256).digest()
    ).decode()

    # Header looks like "v1,<sig> v1,<sig>" — pull each <sig> out.
    candidates = [s.split(",", 1)[1] for s in svix_signature.split()
                  if s.startswith("v1,") and "," in s]
    for candidate in candidates:
        if hmac.compare_digest(candidate, expected):
            return
    raise WebhookAuthError("no signature matched")


# ---------- ingest helpers -----------------------------------------------

def _is_pdf(att: dict) -> bool:
    """Filter for PDF attachments. We accept either content_type or a .pdf
    filename, since some senders set the content type to octet-stream."""
    ct = (att.get("content_type") or "").lower()
    name = (att.get("filename") or "").lower()
    return ct == "application/pdf" or name.endswith(".pdf")


async def _ingest_email_attachments(
    *, db, email_id: str, api_key: str, maptiler_api_key: str,
) -> dict:
    """List → download → ingest every PDF attachment on the email."""
    attachments = await list_attachments(email_id, api_key=api_key)
    pdfs = [a for a in attachments if _is_pdf(a)]
    if not pdfs:
        log.info("inbound %s: no PDF attachments", email_id)
        return {"status": "no_pdfs", "email_id": email_id}

    results = []
    with tempfile.TemporaryDirectory(prefix=f"resend-{email_id}-") as tmpd:
        tmpdir = Path(tmpd)
        for att in pdfs:
            url = att.get("download_url")
            if not url:
                log.warning(
                    "inbound %s: attachment %r has no download_url",
                    email_id, att.get("filename"),
                )
                continue
            content = await download_attachment(url)
            path = tmpdir / (att.get("filename") or f"{att.get('id', 'att')}.pdf")
            path.write_bytes(content)
            try:
                res = await ingest_pdf(
                    db=db, path=path, maptiler_api_key=maptiler_api_key,
                )
            except Exception as e:  # noqa: BLE001 — log + continue on bad PDFs
                log.exception("ingest failed for %s: %s", path.name, e)
                res = {"status": "error", "error": str(e), "filename": path.name}
            results.append({"filename": path.name, **res})
    return {"status": "ok", "email_id": email_id, "results": results}


# ---------- the webhook endpoint -----------------------------------------

@router.post("/inbound/email-received")
async def inbound_email_received(
    request: Request,
    svix_id: str = Header(default="", alias="svix-id"),
    svix_timestamp: str = Header(default="", alias="svix-timestamp"),
    svix_signature: str = Header(default="", alias="svix-signature"),
):
    """Receive an `email.received` webhook from Resend Inbound.

    Returns 401 on signature failure, 200 with a JSON summary otherwise.
    Always returns 200 once auth passes — even if individual PDFs fail
    to parse — so Resend doesn't retry-storm us with a poison message.
    """
    settings = request.app.state.settings
    body = await request.body()

    try:
        verify_svix_signature(
            body=body,
            svix_id=svix_id,
            svix_timestamp=svix_timestamp,
            svix_signature=svix_signature,
            secret=settings.resend_inbound_webhook_secret,
            tolerance_s=settings.resend_webhook_tolerance_s,
        )
    except WebhookAuthError as e:
        log.warning("inbound webhook auth failed: %s", e)
        return Response(status_code=401, content=f"unauthorized: {e}")

    try:
        payload = await request.json()
    except Exception as e:  # noqa: BLE001
        log.warning("inbound webhook: bad JSON: %s", e)
        return Response(status_code=400, content="invalid JSON")

    if payload.get("type") != "email.received":
        # Not the event we care about — ack quietly so Resend doesn't retry.
        return JSONResponse({"status": "ignored", "type": payload.get("type")})

    data = payload.get("data") or {}
    email_id = data.get("email_id")
    if not email_id:
        log.warning("inbound webhook: missing email_id in payload")
        return Response(status_code=400, content="missing email_id")

    try:
        result = await _ingest_email_attachments(
            db=request.app.state.db,
            email_id=email_id,
            api_key=settings.resend_api_key,
            maptiler_api_key=settings.maptiler_api_key,
        )
    except ResendInboundError as e:
        log.exception("inbound webhook: Resend API error: %s", e)
        # Surface as 502 so Resend retries — likely transient.
        return Response(status_code=502, content=f"resend api error: {e}")

    return JSONResponse(result)
