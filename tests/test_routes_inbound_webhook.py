"""Tests for the Resend inbound webhook — signature verification + ingest.

We mock the Resend client (network) and the PDF ingest job (filesystem +
parser). The signature verifier itself runs against the real HMAC code
since that's the security-critical bit."""
import base64
import hashlib
import hmac
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from wswdy.main import create_app
from wswdy.routes.inbound_webhook import (
    WebhookAuthError,
    verify_svix_signature,
)


# A randomly generated secret — base64-encoded 24 bytes. Used by every
# test that signs a request.
_RAW_SECRET = b"\x00" * 24
_SECRET = "whsec_" + base64.b64encode(_RAW_SECRET).decode()


def _sign(body: bytes, *, svix_id: str, svix_timestamp: str,
          secret: bytes = _RAW_SECRET) -> str:
    payload = f"{svix_id}.{svix_timestamp}.".encode() + body
    sig = base64.b64encode(
        hmac.new(secret, payload, hashlib.sha256).digest()
    ).decode()
    return f"v1,{sig}"


# ---------------- pure verifier tests --------------------------------

def test_verify_svix_accepts_valid_signature():
    body = b'{"hello":"world"}'
    ts = str(int(time.time()))
    sig = _sign(body, svix_id="msg_1", svix_timestamp=ts)
    verify_svix_signature(
        body=body, svix_id="msg_1", svix_timestamp=ts, svix_signature=sig,
        secret=_SECRET,
    )


def test_verify_svix_rejects_bad_signature():
    body = b'{"hello":"world"}'
    ts = str(int(time.time()))
    with pytest.raises(WebhookAuthError, match="no signature matched"):
        verify_svix_signature(
            body=body, svix_id="msg_1", svix_timestamp=ts,
            svix_signature="v1,wrong",
            secret=_SECRET,
        )


def test_verify_svix_rejects_old_timestamp():
    body = b'{"hello":"world"}'
    old = str(int(time.time()) - 1000)
    sig = _sign(body, svix_id="msg_1", svix_timestamp=old)
    with pytest.raises(WebhookAuthError, match="tolerance"):
        verify_svix_signature(
            body=body, svix_id="msg_1", svix_timestamp=old, svix_signature=sig,
            secret=_SECRET, tolerance_s=300,
        )


def test_verify_svix_accepts_one_of_multiple_signatures():
    """Resend may send space-separated v1 signatures during key rotation."""
    body = b'{}'
    ts = str(int(time.time()))
    good = _sign(body, svix_id="msg_1", svix_timestamp=ts)
    header = f"v1,wrongsig {good}"
    verify_svix_signature(
        body=body, svix_id="msg_1", svix_timestamp=ts, svix_signature=header,
        secret=_SECRET,
    )


# ---------------- end-to-end webhook tests ---------------------------

@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("RESEND_API_KEY", "r-key")
    monkeypatch.setenv("RESEND_INBOUND_WEBHOOK_SECRET", _SECRET)
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _signed_post(client, body: bytes):
    """Build a request with valid Svix headers and POST to the webhook."""
    ts = str(int(time.time()))
    return client.post(
        "/inbound/email-received",
        content=body,
        headers={
            "content-type": "application/json",
            "svix-id": "msg_test",
            "svix-timestamp": ts,
            "svix-signature": _sign(body, svix_id="msg_test", svix_timestamp=ts),
        },
    )


def test_inbound_rejects_missing_signature(app):
    client = TestClient(app)
    r = client.post(
        "/inbound/email-received",
        content=b'{"type":"email.received","data":{"email_id":"x"}}',
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 401


def test_inbound_rejects_tampered_body(app):
    client = TestClient(app)
    body = b'{"type":"email.received","data":{"email_id":"x"}}'
    ts = str(int(time.time()))
    sig = _sign(body, svix_id="msg_test", svix_timestamp=ts)
    # Tamper with the body but reuse the signature for the original body
    r = client.post(
        "/inbound/email-received",
        content=b'{"type":"email.received","data":{"email_id":"y"}}',
        headers={
            "content-type": "application/json",
            "svix-id": "msg_test",
            "svix-timestamp": ts,
            "svix-signature": sig,
        },
    )
    assert r.status_code == 401


def test_inbound_ignores_non_email_received_event(app):
    client = TestClient(app)
    body = json.dumps({"type": "email.bounced", "data": {}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    assert r.json()["status"] == "ignored"


def test_inbound_400_when_missing_email_id(app):
    client = TestClient(app)
    body = json.dumps({"type": "email.received", "data": {}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 400


@patch("wswdy.routes.inbound_webhook.ingest_email", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.get_received_email", new_callable=AsyncMock)
def test_inbound_happy_path_fetches_body_and_ingests(
    mock_get, mock_ingest, app,
):
    """Webhook fetches the email body via Resend's API, then hands the
    subject + plain-text body to the email-driven ingest job."""
    mock_get.return_value = {
        "id": "em_42",
        "subject": "MPD: Preliminary Crime Report for 2D",
        "text": "This report contains information about recent crimes "
                "reported in the 2D District.\nPSA 202\nCCN 999\n...",
        "html": "<div>...</div>",
    }
    mock_ingest.return_value = {
        "status": "ok", "kind": "crime", "district": "2D",
        "added": 4, "updated": 0, "total": 4,
    }

    client = TestClient(app)
    body = json.dumps({
        "type": "email.received",
        "data": {"email_id": "em_42", "from": "noreply@mpd",
                 "to": ["mpd@hoosoluafe.resend.app"]},
    }).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    out = r.json()
    assert out["status"] == "ok"
    assert out["email_id"] == "em_42"
    assert out["district"] == "2D"
    assert out["added"] == 4

    mock_get.assert_awaited_once_with("em_42", api_key="r-key")
    mock_ingest.assert_awaited_once()
    # ingest got the subject + text body, not the attachments
    call_kwargs = mock_ingest.await_args.kwargs
    assert call_kwargs["subject"] == "MPD: Preliminary Crime Report for 2D"
    assert "2D District" in call_kwargs["text_body"]
    assert call_kwargs["source_file"] == "em_42"


@patch("wswdy.routes.inbound_webhook.get_received_email", new_callable=AsyncMock)
def test_inbound_no_text_body_returns_no_text_body(mock_get, app):
    """If Resend sends an email with no plain-text part, we surface that
    as an explicit status rather than crashing — and we don't retry."""
    mock_get.return_value = {
        "id": "em_42", "subject": "Crime Report 2D", "text": "", "html": "<p>x</p>",
    }
    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    assert r.json()["status"] == "no_text_body"


@patch("wswdy.routes.inbound_webhook.get_received_email", new_callable=AsyncMock)
def test_inbound_502_when_resend_api_fails(mock_get, app):
    from wswdy.clients.resend_inbound import ResendInboundError
    mock_get.side_effect = ResendInboundError("upstream 500")
    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 502


@patch("wswdy.routes.inbound_webhook.ingest_email", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.get_received_email", new_callable=AsyncMock)
def test_inbound_ingest_failure_does_not_500(
    mock_get, mock_ingest, app,
):
    """A parsing failure should not blow up the whole request — log +
    return a 200 with an error summary so Resend doesn't retry."""
    mock_get.return_value = {
        "id": "em_42", "subject": "Crime Report 2D",
        "text": "garbage that doesn't parse",
    }
    mock_ingest.side_effect = RuntimeError("parser boom")

    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    out = r.json()
    assert out["status"] == "error"
    assert "parser boom" in out["error"]
