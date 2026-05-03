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


@patch("wswdy.routes.inbound_webhook.ingest_pdf", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.download_attachment", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.list_attachments", new_callable=AsyncMock)
def test_inbound_happy_path_downloads_pdfs_and_runs_ingest(
    mock_list, mock_download, mock_ingest, app,
):
    mock_list.return_value = [
        {"id": "att1", "filename": "crime.pdf",
         "content_type": "application/pdf",
         "download_url": "https://r/att1"},
        {"id": "att2", "filename": "logo.png",   # non-PDF — should be skipped
         "content_type": "image/png",
         "download_url": "https://r/att2"},
    ]
    mock_download.return_value = b"%PDF-1.7 fake"
    mock_ingest.return_value = {"status": "ok", "kind": "crime",
                                 "added": 4, "updated": 0}

    client = TestClient(app)
    body = json.dumps({
        "type": "email.received",
        "data": {"email_id": "em_42", "from": "noreply@mpd",
                 "to": ["mpd@inbound.iandmuir.com"]},
    }).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    out = r.json()
    assert out["status"] == "ok"
    assert out["email_id"] == "em_42"
    assert len(out["results"]) == 1                # only the PDF ingested
    assert out["results"][0]["filename"] == "crime.pdf"

    # Resend client called with our email_id + API key
    mock_list.assert_awaited_once_with("em_42", api_key="r-key")
    # Only the PDF was downloaded
    mock_download.assert_awaited_once_with("https://r/att1")
    # ingest_pdf got the temp-saved PDF
    mock_ingest.assert_awaited_once()


@patch("wswdy.routes.inbound_webhook.list_attachments", new_callable=AsyncMock)
def test_inbound_no_pdfs_returns_no_pdfs(mock_list, app):
    mock_list.return_value = [
        {"id": "att1", "filename": "logo.png", "content_type": "image/png",
         "download_url": "https://r/att1"},
    ]
    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    assert r.json()["status"] == "no_pdfs"


@patch("wswdy.routes.inbound_webhook.list_attachments", new_callable=AsyncMock)
def test_inbound_502_when_resend_api_fails(mock_list, app):
    from wswdy.clients.resend_inbound import ResendInboundError
    mock_list.side_effect = ResendInboundError("upstream 500")
    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 502


@patch("wswdy.routes.inbound_webhook.ingest_pdf", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.download_attachment", new_callable=AsyncMock)
@patch("wswdy.routes.inbound_webhook.list_attachments", new_callable=AsyncMock)
def test_inbound_ingest_failure_does_not_500(
    mock_list, mock_download, mock_ingest, app,
):
    """A bad PDF should not blow up the whole request — log + continue, 200."""
    mock_list.return_value = [
        {"id": "att1", "filename": "crime.pdf",
         "content_type": "application/pdf",
         "download_url": "https://r/att1"},
    ]
    mock_download.return_value = b"not a real pdf"
    mock_ingest.side_effect = RuntimeError("pdfplumber boom")

    client = TestClient(app)
    body = json.dumps({"type": "email.received",
                        "data": {"email_id": "em_42"}}).encode()
    r = _signed_post(client, body)
    assert r.status_code == 200
    out = r.json()
    assert out["results"][0]["status"] == "error"
    assert "pdfplumber" in out["results"][0]["error"]
