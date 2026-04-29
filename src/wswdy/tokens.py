"""HMAC-signed token utility.

Format: base64url(json_payload).base64url(hmac_sha256)
Payload: {"p": purpose, "s": subscriber_id, "e": expires_at_unix or null}
"""
import base64
import hashlib
import hmac
import json
import time
from typing import Any


class TokenError(Exception):
    """Raised when a token is malformed, tampered, expired, or wrong-purpose."""


def _b64encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _b64decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign(secret: str, *, purpose: str, subscriber_id: str,
         ttl_seconds: int | None = None) -> str:
    expires = int(time.time()) + ttl_seconds if ttl_seconds is not None else None
    payload = {"p": purpose, "s": subscriber_id, "e": expires}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    head = _b64encode(raw)
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    return f"{head}.{_b64encode(sig)}"


def verify(secret: str, token: str, *, purpose: str) -> dict[str, Any]:
    try:
        head, sig_b64 = token.split(".", 1)
        raw = _b64decode(head)
        expected_sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
        actual_sig = _b64decode(sig_b64)
    except (ValueError, base64.binascii.Error) as e:
        raise TokenError(f"malformed token: {e}") from e

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise TokenError("invalid signature")

    try:
        payload = json.loads(raw)
    except ValueError as e:
        raise TokenError(f"malformed token payload: {e}") from e
    if payload.get("p") != purpose:
        raise TokenError(f"wrong purpose: expected {purpose}, got {payload.get('p')}")

    expires = payload.get("e")
    if expires is not None and int(time.time()) > expires:
        raise TokenError("expired")

    return {"subscriber_id": payload["s"], "purpose": payload["p"]}
