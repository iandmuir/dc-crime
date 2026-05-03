"""Resend Inbound webhook — receive emails and ingest their bodies.

Flow:

  1. Resend POSTs ``email.received`` events here, signed Svix-style.
  2. We verify the signature (timestamp tolerance + HMAC-SHA256 over
     ``svix_id.svix_timestamp.body``).
  3. We call Resend's "Received emails" API to fetch the full email
     (subject, plain-text body), then hand subject + text body off to
     ``jobs.email_ingest.ingest_email`` which auto-routes crime vs.
     arrest by subject and persists the parsed records.
  4. We deliberately **ignore the PDF attachment**. MPD's daily PDF is
     for a (semi-)random district that doesn't reliably match the
     email's subject; the email body is the canonical, district-correct
     source.
  5. We always return 200 to Resend after a successful signature check —
     ingest failures are logged but don't trigger retries (the payload
     would just keep arriving with the same bad email).

Why verify Svix-style: Resend uses Svix as their webhook delivery
infrastructure, so the headers and signing scheme exactly match
https://docs.svix.com/receiving/verifying-payloads/how-manual.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse, Response

from wswdy.clients.resend_inbound import (
    ResendInboundError,
    get_received_email,
)
from wswdy.jobs.email_ingest import ingest_email

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


# ---------- ingest helper -------------------------------------------------

async def _ingest_email_body(
    *, db, email_id: str, api_key: str, maptiler_api_key: str,
) -> dict:
    """Fetch the email's subject + text body via Resend's Received Emails
    API and hand it off to the email-driven ingest job."""
    email = await get_received_email(email_id, api_key=api_key)
    subject = (email.get("subject") or "").strip()
    text_body = email.get("text") or ""

    if not text_body:
        # Some clients only send HTML — record the gap and surface as a
        # warning so we can extend the parser if it becomes a recurring
        # issue. (MPD always sends both, so this should be rare.)
        log.warning("inbound %s: email has no plain-text body", email_id)
        return {"status": "no_text_body", "email_id": email_id,
                "subject": subject}

    try:
        res = await ingest_email(
            db=db, subject=subject, text_body=text_body,
            source_file=email_id, maptiler_api_key=maptiler_api_key,
        )
    except Exception as e:  # noqa: BLE001 — log + return a 200 summary
        log.exception("email ingest failed for %s: %s", email_id, e)
        return {"status": "error", "email_id": email_id,
                "subject": subject, "error": str(e)}

    return {"email_id": email_id, "subject": subject, **res}


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
        result = await _ingest_email_body(
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
